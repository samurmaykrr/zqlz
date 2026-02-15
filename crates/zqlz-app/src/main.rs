//! ZQLZ - A modern database IDE built with GPUI
//!
//! This is the main entry point for the ZQLZ application.

mod actions;
mod app;
mod app_init;
mod app_menus;
mod assets;
mod bundled_themes;
mod components;
mod icons;
mod keymaps;
mod logging;
mod main_view;
mod panic_handler;
mod sql_lsp;
mod storage;
mod workspace_state;

use gpui::*;
use panic_handler::{PanicData, PanicHandler};
use std::sync::{Arc, Mutex};
use zqlz_settings::ZqlzSettings;
use zqlz_ui::widgets::menu::AppMenuBar;
use zqlz_zed_adapter::{SettingsBridge, ThemeBridge};

use crate::app::AppState;
use crate::main_view::MainView;

fn main() {
    // Initialize comprehensive logging system first
    // This sets up both console and file logging with JSON output for bug reports
    if let Err(e) = logging::init_default() {
        // Can't use tracing here since logging isn't initialized yet
        // This is the one acceptable use of eprintln - before logging is ready
        eprintln!("FATAL: Failed to initialize logging: {}", e);
        std::process::exit(1);
    }

    // Set up comprehensive panic hook with file logging
    let log_dir = PanicHandler::log_directory();
    let panic_handler = PanicHandler::new(&log_dir);
    let last_panic = panic_handler.install();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        build_mode = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        "Starting ZQLZ"
    );
    tracing::info!(crash_log_dir = %log_dir.display(), "Panic handler initialized");
    tracing::info!(app_log_dir = %logging::log_directory().display(), "Application logs location");

    // Create the GPUI application with combined assets
    // This provides gpui-component icons plus ZQLZ-specific icons
    let app = Application::new().with_assets(assets::CombinedAssets);

    app.run(move |cx| {
        tracing::info!("App run callback started");

        // When running unbundled (cargo run), there's no .app bundle so macOS
        // shows the default terminal icon. Set our dev icon on the dock instead.
        // Must happen inside app.run() because NSApplication ivars aren't
        // initialized until GPUI's platform setup completes.
        #[cfg(target_os = "macos")]
        set_dock_icon_if_unbundled();

        // Initialize the UI system (widgets, actions, keybindings, etc.)
        tracing::info!("Initializing UI system...");
        zqlz_ui::init(cx);

        // Initialize Zed editor subsystem (must be before creating any editors)
        // Note: This initializes Zed's theme system but doesn't sync to ZQLZ yet
        // because ZqlzSettings isn't available
        tracing::info!("Initializing Zed editor adapter...");
        zqlz_zed_adapter::init(cx);

        // Initialize settings system with bundled themes
        // The bundled themes loader is passed to settings init so it can reload
        // themes after the theme directory watcher triggers a reload
        zqlz_settings::init_with_bundled_themes(cx, bundled_themes::load_bundled_themes);

        // Apply ZQLZ settings to Zed's SettingsStore first.
        // This updates Zed's GlobalTheme to match the user's chosen theme
        // (e.g. "Catppuccin Mocha") before we sync colors to ZQLZ.
        tracing::info!("Applying ZQLZ settings to Zed SettingsStore...");
        SettingsBridge::apply_zqlz_settings_to_zed(cx);

        // Now sync Zed's theme colors to ZQLZ's Theme global.
        // Zed's GlobalTheme is now correct (set above), so ZQLZ gets the right colors.
        tracing::info!("Syncing Zed theme to ZQLZ...");
        ThemeBridge::sync_zed_theme_to_zqlz(cx);

        // Register panels and load keybindings from JSON files
        app_init::register_panels(cx);
        keymaps::load_keymaps(cx);

        // Initialize application menus
        // The returned AppMenuBar entity is stored in global state for Windows/Linux
        let app_menu_bar = app_menus::init(cx);
        cx.set_global(AppMenuBarGlobal(app_menu_bar));

        tracing::info!("UI system initialized");

        // Set global application state
        tracing::info!("Setting global AppState...");
        cx.set_global(AppState::new());
        tracing::info!("AppState set");

        // Store panic handler reference for UI access
        cx.set_global(PanicHandlerGlobal {
            last_panic: last_panic.clone(),
        });

        // Quit action is already bound in zqlz_ui::init()

        // Open the main window
        tracing::info!("Opening main window...");
        let result = open_main_window(cx);
        if let Err(e) = result {
            tracing::error!("Failed to open main window: {}", e);
        }
        tracing::info!("Main window open complete");
    });
}

/// Global wrapper for panic handler
pub struct PanicHandlerGlobal {
    pub last_panic: Arc<Mutex<Option<PanicData>>>,
}

impl Global for PanicHandlerGlobal {}

/// Global wrapper for the AppMenuBar (used on Windows/Linux)
pub struct AppMenuBarGlobal(pub Entity<AppMenuBar>);

impl Global for AppMenuBarGlobal {}

/// Open the main application window
fn open_main_window(cx: &mut App) -> anyhow::Result<()> {
    use zqlz_ui::widgets::{Root, TitleBar};

    let window_options = WindowOptions {
        titlebar: Some(TitleBar::title_bar_options()),
        window_bounds: Some(WindowBounds::centered(size(px(1280.0), px(800.0)), cx)),
        window_min_size: Some(size(px(800.0), px(600.0))),
        ..Default::default()
    };

    cx.spawn(async move |cx| {
        cx.open_window(window_options, |window, cx| {
            window.activate_window();
            window.set_window_title("ZQLZ - Database IDE [DEBUG BUILD 2026-01-02]");

            // Apply saved settings (fonts, scrollbar, mode) and sync Zed theme colors.
            // ZqlzSettings::apply sets mode/fonts from ZQLZ settings, then
            // ThemeBridge re-syncs the correct colors from Zed's GlobalTheme.
            ZqlzSettings::global(cx).clone().apply(cx);
            ThemeBridge::sync_zed_theme_to_zqlz(cx);

            // Create the main view
            let main_view = cx.new(|cx| MainView::new(window, cx));

            // Wrap in Root for theme support, dialogs, notifications, etc.
            cx.new(|cx| Root::new(main_view, window, cx))
        })?;

        Ok::<_, anyhow::Error>(())
    })
    .detach();

    tracing::info!("Main window opened successfully");

    Ok(())
}

/// Set the macOS dock icon from the bundled dev icon PNG when running outside
/// of a .app bundle (i.e. via `cargo run`). Inside a real bundle the icon
/// comes from AppIcon.icns so this is a no-op.
#[cfg(target_os = "macos")]
fn set_dock_icon_if_unbundled() {
    use cocoa::base::{id, nil};
    use cocoa::foundation::NSData;
    use objc::{class, msg_send, sel, sel_impl};

    // Detect whether we're inside a .app bundle by checking for the
    // standard Contents/Info.plist layout.
    let is_bundled = std::env::current_exe()
        .ok()
        .and_then(|exe| {
            // .app/Contents/MacOS/binary → check for .app/Contents/Info.plist
            exe.parent()? // MacOS/
                .parent()? // Contents/
                .join("Info.plist")
                .exists()
                .then_some(true)
        })
        .unwrap_or(false);

    if is_bundled {
        return;
    }

    static ICON_BYTES: &[u8] = include_bytes!("../resources/app-icon-dev@2x.png");

    unsafe {
        let data = NSData::dataWithBytes_length_(
            nil,
            ICON_BYTES.as_ptr() as *const std::ffi::c_void,
            ICON_BYTES.len() as u64,
        );
        let icon: id = msg_send![class!(NSImage), alloc];
        let icon: id = msg_send![icon, initWithData: data];
        if icon != nil {
            let app: id = msg_send![class!(NSApplication), sharedApplication];
            let () = msg_send![app, setApplicationIconImage: icon];
        }
    }
}
