//! Build script for zqlz-app
//!
//! This handles platform-specific build configuration:
//! - Windows: Embeds icon and version info into the executable
//! - macOS: Sets deployment target and framework linking

fn main() {
    if let Err(error) = generate_bundled_theme_manifest() {
        panic!("failed to generate bundled theme manifest: {error}");
    }

    // Set macOS deployment target
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-env=MACOSX_DEPLOYMENT_TARGET=10.15");
    }

    // Set commit SHA for version info
    #[allow(clippy::disallowed_methods)]
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        && output.status.success()
    {
        let sha = String::from_utf8_lossy(&output.stdout);
        println!("cargo:rustc-env=ZQLZ_COMMIT_SHA={}", sha.trim());
    }

    // Windows-specific: embed icon and version info
    #[cfg(target_os = "windows")]
    {
        windows_build();
    }
}

fn generate_bundled_theme_manifest() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;

    let manifest_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let themes_directory = manifest_directory.join("assets/themes");

    println!("cargo:rerun-if-changed={}", themes_directory.display());

    let mut theme_to_file: BTreeMap<String, (String, bool)> = BTreeMap::new();

    let mut theme_paths = fs::read_dir(&themes_directory)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("json")
        })
        .collect::<Vec<_>>();
    theme_paths.sort();

    for path in theme_paths {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("invalid UTF-8 theme file name for '{}'", path.display()))?
            .to_owned();
        let file_content = fs::read_to_string(&path)?;
        let parsed: serde_json::Value = serde_json::from_str(&file_content)?;
        let themes = parsed
            .get("themes")
            .and_then(|value| value.as_array())
            .ok_or_else(|| format!("theme file '{}' has no themes array", path.display()))?;

        for theme in themes {
            let Some(theme_name) = theme.get("name").and_then(|value| value.as_str()) else {
                continue;
            };

            if theme_name.trim().is_empty() {
                continue;
            }

            let is_default = theme
                .get("is_default")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);

            if let Some((existing_file, _)) = theme_to_file.get(theme_name)
                && existing_file != &file_name
            {
                println!(
                    "cargo:warning=Duplicate bundled theme name '{}' in '{}' and '{}' (keeping first)",
                    theme_name, existing_file, file_name
                );
                continue;
            }

            theme_to_file
                .entry(theme_name.to_owned())
                .or_insert((file_name.clone(), is_default));
        }
    }

    let output_directory = PathBuf::from(std::env::var("OUT_DIR")?);
    let output_path = output_directory.join("bundled_theme_manifest.rs");
    let mut output_file = fs::File::create(output_path)?;

    writeln!(
        output_file,
        "const BUNDLED_THEME_MANIFEST: &[BundledThemeManifestEntry] = &["
    )?;
    for (theme_name, (file_name, is_default)) in theme_to_file {
        let theme_name = serde_json::to_string(&theme_name)?;
        let file_name = serde_json::to_string(&file_name)?;
        writeln!(
            output_file,
            "    BundledThemeManifestEntry {{ theme_name: {theme_name}, file_name: {file_name}, is_default: {is_default} }},"
        )?;
    }
    writeln!(output_file, "];")?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_build() {
    use std::path::PathBuf;

    let resources_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources");
    let windows_resources = resources_dir.join("windows");

    // Determine which icon to use based on release channel
    let release_channel = std::env::var("RELEASE_CHANNEL").unwrap_or_else(|_| "dev".to_string());

    let icon_name = match release_channel.as_str() {
        "stable" => "app-icon.ico",
        "nightly" => "app-icon-nightly.ico",
        _ => "app-icon-dev.ico",
    };

    let icon_path = windows_resources.join(icon_name);

    // Fall back to default icon if variant doesn't exist
    let icon_path = if icon_path.exists() {
        icon_path
    } else {
        windows_resources.join("app-icon.ico")
    };

    if icon_path.exists() {
        let mut res = winresource::WindowsResource::new();

        // Set icon
        res.set_icon(icon_path.to_str().unwrap());

        // Set version info
        let version = env!("CARGO_PKG_VERSION");
        let version_parts: Vec<&str> = version.split('.').collect();
        let major = version_parts
            .first()
            .unwrap_or(&"0")
            .parse::<u64>()
            .unwrap_or(0);
        let minor = version_parts
            .get(1)
            .unwrap_or(&"0")
            .parse::<u64>()
            .unwrap_or(0);
        let patch = version_parts
            .get(2)
            .unwrap_or(&"0")
            .parse::<u64>()
            .unwrap_or(0);

        res.set("FileVersion", &format!("{}.{}.{}.0", major, minor, patch));
        res.set(
            "ProductVersion",
            &format!("{}.{}.{}.0", major, minor, patch),
        );
        res.set("FileDescription", "ZQLZ - Database Client & SQL Editor");
        res.set("ProductName", "ZQLZ");
        res.set("OriginalFilename", "zqlz.exe");
        res.set("LegalCopyright", "Copyright (c) ZQLZ Team");
        res.set("CompanyName", "ZQLZ");

        // Compile resources
        if let Err(e) = res.compile() {
            eprintln!("cargo:warning=Failed to compile Windows resources: {}", e);
        }
    } else {
        println!(
            "cargo:warning=Windows icon not found at {:?}, executable will use default icon",
            icon_path
        );
    }
}
