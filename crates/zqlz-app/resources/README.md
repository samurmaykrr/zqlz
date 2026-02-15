# App Icons

This directory contains the application icons for ZQLZ in different variants.

## Icon Variants

### Stable Release
- `app-icon.png` (512x512) - Standard resolution
- `app-icon@2x.png` (1024x1024) - Retina/HiDPI resolution
- **No badge** - Clean icon for production releases

### Dev Build
- `app-icon-dev.png` (512x512) - Standard resolution
- `app-icon-dev@2x.png` (1024x1024) - Retina/HiDPI resolution
- **Red "DEV" badge** in bottom-right corner

### Nightly Build
- `app-icon-nightly.png` (512x512) - Standard resolution
- `app-icon-nightly@2x.png` (1024x1024) - Retina/HiDPI resolution
- **Red "NIGHTLY" badge** in bottom-right corner

## Windows Icons

The `windows/` directory contains `.ico` files for Windows:
- `app-icon.ico` - Stable release (7 embedded sizes: 16, 32, 48, 64, 96, 128, 256)
- `app-icon-dev.ico` - Dev build with badge
- `app-icon-nightly.ico` - Nightly build with badge

## Source Icon

- `icon.png` (2592x3552) - Original high-resolution source artwork

## Regenerating Icons

To regenerate all icons from the source:

```bash
python3 create_app_icons.py
```

This will:
1. Resize the source icon to required dimensions
2. Add appropriate badges for dev and nightly variants
3. Create PNG files for macOS/Linux
4. Create .ico files for Windows

### Requirements

- Python 3.x
- Pillow (PIL) library: `pip install Pillow`
- ImageMagick (for .ico generation): `brew install imagemagick`

## Usage in Build Configuration

The icons are referenced in `Cargo.toml`:

### macOS (via cargo-bundle)
```toml
[package.metadata.bundle-stable]
icon = ["resources/app-icon@2x.png", "resources/app-icon.png"]

[package.metadata.bundle-dev]
icon = ["resources/app-icon-dev@2x.png", "resources/app-icon-dev.png"]

[package.metadata.bundle-nightly]
icon = ["resources/app-icon-nightly@2x.png", "resources/app-icon-nightly.png"]
```

### Windows (via winresource)
The `.ico` files are embedded during the build process via the `build.rs` script.

## Badge Design

- **Color**: Red (#FF0000) background for high visibility
- **Text**: White color for maximum contrast
- **Position**: Bottom-right corner with padding
- **Size**: Proportional to icon size (10% height)
- **Font**: System font (Helvetica/SF Compact on macOS)
