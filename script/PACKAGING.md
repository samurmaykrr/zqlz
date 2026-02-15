# ZQLZ Packaging System

This directory contains scripts and resources for building distributable packages of ZQLZ.

## Quick Start

### macOS

```bash
# Build a dev .app bundle and .dmg (unsigned)
./script/bundle-mac

# Build for Intel Macs
./script/bundle-mac x86_64-apple-darwin

# Build for Apple Silicon (default)
./script/bundle-mac aarch64-apple-darwin
```

### Linux

```bash
# Build a Linux tarball
./script/bundle-linux

# Build for specific architecture
./script/bundle-linux x86_64-unknown-linux-gnu
./script/bundle-linux aarch64-unknown-linux-gnu
```

### Windows (PowerShell)

```powershell
# Build installer
.\script\bundle-windows.ps1

# Build for specific architecture
.\script\bundle-windows.ps1 -Architecture x86_64
.\script\bundle-windows.ps1 -Architecture aarch64
```

## Release Channels

Set the `RELEASE_CHANNEL` environment variable to control the build variant:

- `dev` (default) - Development builds with debug symbols
- `nightly` - Nightly builds with release optimizations
- `stable` - Production release builds

```bash
RELEASE_CHANNEL=stable ./script/bundle-mac
```

## Code Signing

### macOS

Set these environment variables for signed/notarized builds:

```bash
export MACOS_CERTIFICATE="<base64-encoded .p12 certificate>"
export MACOS_CERTIFICATE_PASSWORD="<certificate password>"
export APPLE_NOTARIZATION_KEY="<base64-encoded AuthKey .p8 file>"
export APPLE_NOTARIZATION_KEY_ID="<key ID>"
export APPLE_NOTARIZATION_ISSUER_ID="<issuer ID>"
```

### Windows

Set these environment variables for Azure Trusted Signing:

```powershell
$env:AZURE_TENANT_ID = "<tenant ID>"
$env:AZURE_CLIENT_ID = "<client ID>"
$env:AZURE_CLIENT_SECRET = "<client secret>"
$env:AZURE_SIGNING_ENDPOINT = "<signing endpoint>"
$env:AZURE_SIGNING_ACCOUNT = "<account name>"
$env:AZURE_SIGNING_CERT_PROFILE = "<certificate profile>"
```

## Directory Structure

```
script/
├── bundle-mac          # macOS bundling script
├── bundle-linux        # Linux bundling script
├── bundle-windows.ps1  # Windows bundling script (PowerShell)
└── lib/               # Shared utilities

crates/zqlz-app/
├── resources/
│   ├── app-icon.png           # Main app icon (512x512)
│   ├── app-icon@2x.png        # Retina app icon (1024x1024)
│   ├── app-icon-dev*.png      # Dev channel icons
│   ├── app-icon-nightly*.png  # Nightly channel icons
│   ├── zqlz.entitlements      # macOS entitlements
│   ├── zqlz.desktop.in        # Linux .desktop template
│   ├── info/                  # macOS Info.plist extensions
│   │   ├── DocumentTypes.plist
│   │   ├── Permissions.plist
│   │   └── SupportedPlatforms.plist
│   └── windows/               # Windows resources
│       ├── app-icon*.ico      # Windows icons
│       ├── zqlz.iss           # Inno Setup script
│       ├── sign.ps1           # Windows signing script
│       └── messages/          # Installer translations
│           └── en.isl
└── build.rs                   # Build script (Windows resource embedding)
```

## Output Files

After running the bundle scripts, you'll find:

### macOS
- `target/<triple>/<profile>/bundle/ZQLZ.app/` - Application bundle
- `target/<triple>/<profile>/ZQLZ-<arch>.dmg` - DMG installer

### Linux
- `target/<triple>/<profile>/zqlz.app/` - Application bundle directory
- `target/<triple>/<profile>/zqlz-linux-<arch>.tar.gz` - Distributable tarball

### Windows
- `target/<triple>/<profile>/installer-staging/` - Staging directory
- `target/<triple>/<profile>/ZQLZ-<arch>.exe` - Inno Setup installer
- `target/<triple>/<profile>/ZQLZ-<arch>.zip` - Portable ZIP (if Inno Setup not available)

## Customizing Icons

Replace the placeholder icons in `crates/zqlz-app/resources/`:

1. Create your icons at these sizes:
   - `app-icon.png` - 512x512 pixels
   - `app-icon@2x.png` - 1024x1024 pixels

2. For Windows, create ICO files:
   ```bash
   magick app-icon@2x.png \
     \( -clone 0 -resize 16x16 \) \
     \( -clone 0 -resize 32x32 \) \
     \( -clone 0 -resize 48x48 \) \
     \( -clone 0 -resize 64x64 \) \
     \( -clone 0 -resize 128x128 \) \
     \( -clone 0 -resize 256x256 \) \
     -delete 0 \
     windows/app-icon.ico
   ```

3. Create variants for dev and nightly channels with appropriate suffixes.

## Requirements

### macOS
- Xcode Command Line Tools
- ImageMagick (optional, for icon generation)

### Linux
- `envsubst` (from gettext)
- `tar`
- `objcopy` or `strip` (optional, for stripping debug symbols)

### Windows
- Visual Studio Build Tools
- Inno Setup 6 (for installer creation)
- PowerShell 5.1+
- Azure.CodeSigning module (optional, for signing)
