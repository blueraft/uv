use std::path::PathBuf;

use anyhow::{Context, Result};

use pep508_rs::ExtraName;
use uv_cache::Cache;
use uv_client::{BaseClientBuilder, Connectivity, FlatIndexClient, RegistryClientBuilder};
use uv_configuration::{Concurrency, ExtrasSpecification, PreviewMode, SetupPyStrategy};
use uv_dispatch::BuildDispatch;
use uv_distribution::pyproject_mut::PyProjectTomlMut;
use uv_distribution::{
    pyproject::{DependencyType, Source, SourceError},
    pyproject_mut::TomlVariant,
};
use uv_distribution::{DistributionDatabase, ProjectWorkspace, VirtualProject, Workspace};
use uv_git::GitResolver;
use uv_normalize::PackageName;
use uv_requirements::{NamedRequirementsResolver, RequirementsSource, RequirementsSpecification};
use uv_resolver::{FlatIndex, InMemoryIndex};
use uv_scripts::Pep723Metadata;
use uv_toolchain::{
    request_from_version_file, EnvironmentPreference, Toolchain, ToolchainFetch,
    ToolchainPreference, ToolchainRequest, VersionRequest,
};
use uv_types::{BuildIsolation, HashStrategy, InFlight};
use uv_warnings::warn_user_once;

use crate::commands::pip::operations::Modifications;
use crate::commands::pip::resolution_environment;
use crate::commands::project::SharedState;
use crate::commands::reporters::ResolverReporter;
use crate::commands::{project, ExitStatus};
use crate::printer::Printer;
use crate::settings::ResolverInstallerSettings;

/// Add one or more packages to the project requirements.
#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
pub(crate) async fn add(
    requirements: Vec<RequirementsSource>,
    editable: Option<bool>,
    script: Option<PathBuf>,
    dependency_type: DependencyType,
    raw_sources: bool,
    rev: Option<String>,
    tag: Option<String>,
    branch: Option<String>,
    extras: Vec<ExtraName>,
    package: Option<PackageName>,
    python: Option<String>,
    settings: ResolverInstallerSettings,
    toolchain_preference: ToolchainPreference,
    toolchain_fetch: ToolchainFetch,
    preview: PreviewMode,
    connectivity: Connectivity,
    concurrency: Concurrency,
    native_tls: bool,
    cache: &Cache,
    printer: Printer,
) -> Result<ExitStatus> {
    if preview.is_disabled() {
        warn_user_once!("`uv add` is experimental and may change without warning.");
    }

    let client_builder = BaseClientBuilder::new()
        .connectivity(connectivity)
        .native_tls(native_tls)
        .keyring(settings.keyring_provider);

    let (toml, venv) = if let Some(script_path) = &script {
        let Some(contents) = uv_scripts::read_pep723_content(script_path).await? else {
            anyhow::bail!("Failed to read metadata from script");
        };
        // Parse the metadata.
        let metadata: Pep723Metadata = toml::from_str(&contents)?;

        // (1) Explicit request from user
        let python_request = if let Some(request) = python.as_deref() {
            Some(ToolchainRequest::parse(request))
            // (2) Request from `.python-version`
        } else if let Some(request) = request_from_version_file().await? {
            Some(request)
            // (3) `Requires-Python` in `pyproject.toml`
        } else {
            metadata.requires_python.map(|requires_python| {
                ToolchainRequest::Version(VersionRequest::Range(requires_python))
            })
        };

        let interpreter = Toolchain::find_or_fetch(
            python_request,
            EnvironmentPreference::Any,
            toolchain_preference,
            toolchain_fetch,
            &client_builder,
            cache,
        )
        .await?
        .into_interpreter();

        // Create a virtual environment
        let temp_dir = cache.environment()?;
        let venv = uv_virtualenv::create_venv(
            temp_dir.path(),
            interpreter,
            uv_virtualenv::Prompt::None,
            false,
            false,
        )?;
        (TomlVariant::Script(contents), venv)
    } else {
        // Find the project in the workspace.
        let project = if let Some(package) = package {
            Workspace::discover(&std::env::current_dir()?, None)
                .await?
                .with_current_project(package.clone())
                .with_context(|| format!("Package `{package}` not found in workspace"))?
        } else {
            ProjectWorkspace::discover(&std::env::current_dir()?, None).await?
        };
        // Discover or create the virtual environment.
        let venv = project::get_or_init_environment(
            project.workspace(),
            python.as_deref().map(ToolchainRequest::parse),
            toolchain_preference,
            toolchain_fetch,
            connectivity,
            native_tls,
            cache,
            printer,
        )
        .await?;

        (TomlVariant::Project(project), venv)
    };

    // Read the requirements.
    let RequirementsSpecification { requirements, .. } =
        RequirementsSpecification::from_sources(&requirements, &[], &[], &client_builder).await?;

    // TODO(charlie): These are all default values. We should consider whether we want to make them
    // optional on the downstream APIs.
    let python_version = None;
    let python_platform = None;
    let hasher = HashStrategy::default();
    let setup_py = SetupPyStrategy::default();
    let build_isolation = BuildIsolation::default();

    // Determine the environment for the resolution.
    let (tags, markers) =
        resolution_environment(python_version, python_platform, venv.interpreter())?;

    // Initialize the registry client.
    let client = RegistryClientBuilder::new(cache.clone())
        .native_tls(native_tls)
        .connectivity(connectivity)
        .index_urls(settings.index_locations.index_urls())
        .index_strategy(settings.index_strategy)
        .keyring(settings.keyring_provider)
        .markers(&markers)
        .platform(venv.interpreter().platform())
        .build();

    // Initialize any shared state.
    let git = GitResolver::default();
    let in_flight = InFlight::default();
    let index = InMemoryIndex::default();

    // Resolve the flat indexes from `--find-links`.
    let flat_index = {
        let client = FlatIndexClient::new(&client, cache);
        let entries = client.fetch(settings.index_locations.flat_index()).await?;
        FlatIndex::from_entries(entries, Some(&tags), &hasher, &settings.build_options)
    };

    // Create a build dispatch.
    let build_dispatch = BuildDispatch::new(
        &client,
        cache,
        venv.interpreter(),
        &settings.index_locations,
        &flat_index,
        &index,
        &git,
        &in_flight,
        settings.index_strategy,
        setup_py,
        &settings.config_setting,
        build_isolation,
        settings.link_mode,
        &settings.build_options,
        settings.exclude_newer,
        concurrency,
        preview,
    );

    // Resolve any unnamed requirements.
    let requirements = NamedRequirementsResolver::new(
        requirements,
        &hasher,
        &index,
        DistributionDatabase::new(&client, &build_dispatch, concurrency.downloads, preview),
    )
    .with_reporter(ResolverReporter::from(printer))
    .resolve()
    .await?;

    // Add the requirements to the `pyproject.toml`.
    let mut pyproject = PyProjectTomlMut::from_toml(&toml)?;
    for mut req in requirements {
        // Add the specified extras.
        req.extras.extend(extras.iter().cloned());
        req.extras.sort_unstable();
        req.extras.dedup();

        let (req, source) = match toml {
            TomlVariant::Script(_) => (pep508_rs::Requirement::from(req), None),
            TomlVariant::Project(_) if raw_sources => (pep508_rs::Requirement::from(req), None),
            TomlVariant::Project(ref project) => {
                // Otherwise, try to construct the source.
                let workspace = project.workspace().packages().contains_key(&req.name);
                let result = Source::from_requirement(
                    &req.name,
                    req.source.clone(),
                    workspace,
                    editable,
                    rev.clone(),
                    tag.clone(),
                    branch.clone(),
                );

                let source = match result {
                    Ok(source) => source,
                    Err(SourceError::UnresolvedReference(rev)) => {
                        anyhow::bail!("Cannot resolve Git reference `{rev}` for requirement `{}`. Specify the reference with one of `--tag`, `--branch`, or `--rev`, or use the `--raw-sources` flag.", req.name)
                    }
                    Err(err) => return Err(err.into()),
                };

                // Ignore the PEP 508 source.
                let mut req = pep508_rs::Requirement::from(req);
                req.clear_url();

                (req, source)
            }
        };

        match dependency_type {
            DependencyType::Production => {
                pyproject.add_dependency(req, source)?;
            }
            DependencyType::Dev => {
                pyproject.add_dev_dependency(req, source)?;
            }
            DependencyType::Optional(ref group) => {
                pyproject.add_optional_dependency(req, group, source)?;
            }
        }
    }

    match toml {
        TomlVariant::Script(contents) => {
            dbg!(&pyproject.to_string());
        }
        TomlVariant::Project(project) => {
            // Save the modified `pyproject.toml`.
            fs_err::write(
                project.current_project().root().join("pyproject.toml"),
                pyproject.to_string(),
            )?;

            // Initialize any shared state.
            let state = SharedState::default();

            // Lock and sync the environment.
            let lock = project::lock::do_lock(
                project.workspace(),
                venv.interpreter(),
                settings.as_ref().into(),
                &state,
                preview,
                connectivity,
                concurrency,
                native_tls,
                cache,
                printer,
            )
            .await?;

            // Perform a full sync, because we don't know what exactly is affected by the removal.
            // TODO(ibraheem): Should we accept CLI overrides for this? Should we even sync here?
            let extras = ExtrasSpecification::All;
            let dev = true;

            project::sync::do_sync(
                &VirtualProject::Project(project.to_owned()),
                &venv,
                &lock,
                extras,
                dev,
                Modifications::Sufficient,
                settings.as_ref().into(),
                &state,
                preview,
                connectivity,
                concurrency,
                native_tls,
                cache,
                printer,
            )
            .await?;
        }
    }

    Ok(ExitStatus::Success)
}
