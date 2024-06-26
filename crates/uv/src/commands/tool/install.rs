use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt::Write;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use distribution_types::Name;
use itertools::Itertools;

use pypi_types::VerbatimParsedUrl;
use tracing::debug;
use uv_cache::Cache;
use uv_client::Connectivity;
use uv_configuration::{Concurrency, PreviewMode, Reinstall};
#[cfg(unix)]
use uv_fs::replace_symlink;
use uv_fs::Simplified;
use uv_installer::SitePackages;
use uv_requirements::RequirementsSpecification;
use uv_tool::{entrypoint_paths, find_executable_directory, InstalledTools, Tool, ToolEntrypoint};
use uv_toolchain::{EnvironmentPreference, Toolchain, ToolchainPreference, ToolchainRequest};
use uv_warnings::warn_user_once;

use crate::commands::project::update_environment;
use crate::commands::ExitStatus;
use crate::printer::Printer;
use crate::settings::ResolverInstallerSettings;

/// Install a tool.
pub(crate) async fn install(
    package: String,
    from: Option<String>,
    python: Option<String>,
    with: Vec<String>,
    force: bool,
    settings: ResolverInstallerSettings,
    preview: PreviewMode,
    toolchain_preference: ToolchainPreference,
    connectivity: Connectivity,
    concurrency: Concurrency,
    native_tls: bool,
    cache: &Cache,
    printer: Printer,
) -> Result<ExitStatus> {
    if preview.is_disabled() {
        warn_user_once!("`uv tool install` is experimental and may change without warning.");
    }

    let from = if let Some(from) = from {
        let from_requirement = pep508_rs::Requirement::<VerbatimParsedUrl>::from_str(&from)?;
        // Check if the user provided more than just a name positionally or if that name conflicts with `--from`
        if from_requirement.name.to_string() != package {
            // Determine if its an entirely different package or a conflicting specification
            let package_requirement =
                pep508_rs::Requirement::<VerbatimParsedUrl>::from_str(&package)?;
            if from_requirement.name == package_requirement.name {
                bail!(
                    "Package requirement `{}` provided with `--from` conflicts with install request `{}`",
                    from,
                    package
                );
            }
            bail!(
                "Package name `{}` provided with `--from` does not match install request `{}`",
                from_requirement.name,
                package
            );
        }
        from_requirement
    } else {
        pep508_rs::Requirement::<VerbatimParsedUrl>::from_str(&package)?
    };

    let name = from.name.to_string();

    let installed_tools = InstalledTools::from_settings()?;

    let existing_tool_receipt = installed_tools.get_tool_receipt(&name)?;
    // TODO(zanieb): Automatically replace an existing tool if the request differs
    let reinstall_entry_points = if existing_tool_receipt.is_some() {
        if force {
            debug!("Replacing existing tool due to `--force` flag.");
            true
        } else {
            match settings.reinstall {
                Reinstall::All => {
                    debug!("Replacing existing tool due to `--reinstall` flag.");
                    true
                }
                // Do not replace the entry points unless the tool is explicitly requested
                Reinstall::Packages(ref packages) => packages.contains(&from.name),
                // If not reinstalling... then we're done
                Reinstall::None => {
                    writeln!(printer.stderr(), "Tool `{name}` is already installed")?;
                    return Ok(ExitStatus::Failure);
                }
            }
        }
    } else {
        false
    };

    let requirements = [Ok(from.clone())]
        .into_iter()
        .chain(
            with.iter()
                .map(|name| pep508_rs::Requirement::from_str(name)),
        )
        .collect::<Result<Vec<pep508_rs::Requirement<VerbatimParsedUrl>>, _>>()?;

    let spec = RequirementsSpecification::from_requirements(
        requirements
            .iter()
            .cloned()
            .map(pypi_types::Requirement::from)
            .collect(),
    );

    let Some(from) = requirements.first().cloned() else {
        bail!("Expected at least one requirement")
    };

    let interpreter = Toolchain::find(
        &python
            .as_deref()
            .map(ToolchainRequest::parse)
            .unwrap_or_default(),
        EnvironmentPreference::OnlySystem,
        toolchain_preference,
        cache,
    )?
    .into_interpreter();

    // TODO(zanieb): Build the environment in the cache directory then copy into the tool directory
    // This lets us confirm the environment is valid before removing an existing install
    let environment = installed_tools.environment(
        &name,
        // Do not remove the existing environment if we're reinstalling a subset of packages
        !matches!(settings.reinstall, Reinstall::Packages(_)),
        interpreter,
        cache,
    )?;

    // Install the ephemeral requirements.
    let environment = update_environment(
        environment,
        spec,
        &settings,
        preview,
        connectivity,
        concurrency,
        native_tls,
        cache,
        printer,
    )
    .await?;

    let site_packages = SitePackages::from_environment(&environment)?;
    let installed = site_packages.get_packages(&from.name);
    let Some(installed_dist) = installed.first().copied() else {
        bail!("Expected at least one requirement")
    };

    // Exit early if we're not supposed to be reinstalling entry points
    // e.g. `--reinstall-package` was used for some dependency
    if existing_tool_receipt.is_some() && !reinstall_entry_points {
        writeln!(printer.stderr(), "Updated environment for tool `{name}`")?;
        return Ok(ExitStatus::Success);
    }

    // Find a suitable path to install into
    // TODO(zanieb): Warn if this directory is not on the PATH
    let executable_directory = find_executable_directory()?;
    fs_err::create_dir_all(&executable_directory)
        .context("Failed to create executable directory")?;

    debug!(
        "Installing tool entry points into {}",
        executable_directory.user_display()
    );

    let entry_points = entrypoint_paths(
        &environment,
        installed_dist.name(),
        installed_dist.version(),
    )?;

    // Determine the entry points targets
    // Use a sorted collection for deterministic output
    let target_entry_points = entry_points
        .into_iter()
        .map(|(name, source_path)| {
            let target_path = executable_directory.join(
                source_path
                    .file_name()
                    .map(std::borrow::ToOwned::to_owned)
                    .unwrap_or_else(|| OsString::from(name.clone())),
            );
            (name, source_path, target_path)
        })
        .collect::<BTreeSet<_>>();

    if target_entry_points.is_empty() {
        // Clean up the environment we just created
        installed_tools.remove_environment(&name)?;

        bail!("No entry points found for tool `{name}`");
    }

    // Check if they exist, before installing
    let mut existing_entry_points = target_entry_points
        .iter()
        .filter(|(_, _, target_path)| target_path.exists())
        .peekable();

    // Note we use `reinstall_entry_points` here instead of `reinstall`; requesting reinstall
    // will _not_ remove existing entry points when they are not managed by uv.
    if force || reinstall_entry_points {
        for (name, _, target) in existing_entry_points {
            debug!("Removing existing entry point `{name}`");
            fs_err::remove_file(target)?;
        }
    } else if existing_entry_points.peek().is_some() {
        // Clean up the environment we just created
        installed_tools.remove_environment(&name)?;

        let existing_entry_points = existing_entry_points
            // SAFETY: We know the target has a filename because we just constructed it above
            .map(|(_, _, target)| target.file_name().unwrap().to_string_lossy())
            .collect::<Vec<_>>();
        let (s, exists) = if existing_entry_points.len() == 1 {
            ("", "exists")
        } else {
            ("s", "exist")
        };
        bail!(
            "Entry point{s} for tool already {exists}: {} (use `--force` to overwrite)",
            existing_entry_points.iter().join(", ")
        )
    }

    for (name, source_path, target_path) in &target_entry_points {
        debug!("Installing `{name}`");
        #[cfg(unix)]
        replace_symlink(source_path, target_path).context("Failed to install entrypoint")?;
        #[cfg(windows)]
        fs_err::copy(source_path, target_path).context("Failed to install entrypoint")?;
    }

    writeln!(
        printer.stderr(),
        "Installed: {}",
        target_entry_points
            .iter()
            .map(|(name, _, _)| name)
            .join(", ")
    )?;

    debug!("Adding receipt for tool `{name}`");
    let installed_tools = installed_tools.init()?;
    let tool = Tool::new(
        requirements,
        python,
        target_entry_points
            .into_iter()
            .map(|(name, _, target_path)| ToolEntrypoint::new(name, target_path)),
    );
    installed_tools.add_tool_receipt(&name, tool)?;

    Ok(ExitStatus::Success)
}
