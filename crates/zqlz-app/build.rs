//! Build script for zqlz-app
//!
//! This handles platform-specific build configuration:
//! - Windows: Embeds icon and version info into the executable
//! - macOS: Sets deployment target and framework linking

fn main() {
    // Set macOS deployment target
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-env=MACOSX_DEPLOYMENT_TARGET=10.15");
    }

    // Set commit SHA for version info
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    {
        if output.status.success() {
            let sha = String::from_utf8_lossy(&output.stdout);
            println!("cargo:rustc-env=ZQLZ_COMMIT_SHA={}", sha.trim());
        }
    }

    // Windows-specific: embed icon and version info
    #[cfg(target_os = "windows")]
    {
        windows_build();
    }
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
