//! ZQLZ - A modern database IDE built with GPUI
//!
//! This is the main entry point for the ZQLZ application.

#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
#![allow(unexpected_cfgs)]

mod actions;
mod agent_tools;
mod app;
mod app_init;
mod app_menus;
mod assets;
mod bundled_themes;
mod components;
mod icons;
mod ipc_server;
mod keymaps;
mod logging;
mod main_view;
mod panic_handler;
mod sql_lsp;
mod storage;
mod window_manager;
mod workspace;
mod workspace_state;

use futures::StreamExt as _;
use gpui::*;
use panic_handler::{PanicData, PanicHandler};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use url::Url;
use zqlz_settings::ZqlzSettings;
use zqlz_ui::widgets::{Root, menu::AppMenuBar};

use crate::app::AppState;
use crate::main_view::MainView;

fn main() {
    // Must run before logging init: this binary is linked as a Windows-subsystem
    // application, so it starts with no console and the `eprintln!` below would
    // otherwise go nowhere.
    logging::attach_parent_console(foreground_requested());

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

    // Create the GPUI application with ZQLZ-specific assets.
    let app = gpui_platform::application().with_assets(assets::CombinedAssets);
    let launch_targets = collect_launch_targets();

    app.on_open_urls({
        let launch_targets = launch_targets.clone();
        move |urls| {
            if urls.is_empty() {
                return;
            }

            if let Ok(mut pending_targets) = launch_targets.lock() {
                for url in urls {
                    pending_targets.push(url.to_string());
                }
            }
        }
    });

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
        window_manager::init(cx);
        zqlz_ui::init(cx);
        zqlz_text_editor::actions::init(cx);
        zqlz_text_editor::find_replace_panel::init(cx);
        zqlz_table_designer::init(cx);

        // Initialize settings system with bundled themes
        // The bundled themes loader is passed to settings init so it can reload
        // themes after the theme directory watcher triggers a reload
        zqlz_settings::init_with_bundled_themes(cx, bundled_themes::load_bundled_themes);

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

        // Keep active database connections alive across idle periods so they
        // don't get dropped out from under the user by a server or NAT timeout.
        app::spawn_connection_heartbeat(cx.global::<AppState>().connections.clone(), cx);

        let startup_targets = snapshot_launch_targets(&launch_targets);

        agent_tools::load_agent_access_policy(cx);

        // Start the IPC server so the `zqlz` CLI can communicate with this
        // running instance. We clone only the Arc fields we need so the handle
        // is Send + Sync without pulling AppState out of GPUI's context.
        let state = cx.global::<AppState>();
        let (ui_jobs, ui_job_receiver) = futures::channel::mpsc::unbounded();
        let ipc_handle = ipc_server::IpcServerHandle {
            connection_service: state.connection_service.clone(),
            query_service: state.query_service.clone(),
            query_history: state.query_history.clone(),
            storage: state.storage.clone(),
            ui_jobs,
        };

        match ipc_server::default_socket_path() {
            Ok(socket_path) => {
                if !startup_targets.is_empty() {
                    match ipc_server::try_forward_open_targets(&socket_path, startup_targets) {
                        Ok(true) => {
                            tracing::info!(
                                "Forwarded startup targets to existing instance; exiting new process"
                            );
                            cx.quit();
                            return;
                        }
                        Ok(false) => {
                            tracing::debug!("No existing instance found for startup target handoff");
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "Failed to hand off startup targets; continuing as first instance"
                            );
                        }
                    }
                }

                ipc_server::start(ipc_handle, socket_path);
                tracing::info!("IPC server started");
            }
            Err(error) => {
                tracing::error!("Failed to determine IPC socket path: {}", error);
            }
        }

        // Store panic handler reference for UI access
        cx.set_global(PanicHandlerGlobal {
            last_panic: last_panic.clone(),
        });

        // Quit action is already bound in zqlz_ui::init()

        // Open the main window
        tracing::info!("Opening main window...");
        let result = open_main_window(cx, launch_targets.clone(), ui_job_receiver);
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
fn open_main_window(
    cx: &mut App,
    launch_targets: Arc<Mutex<Vec<String>>>,
    ui_job_receiver: ipc_server::UiJobReceiver,
) -> anyhow::Result<()> {
    window_manager::open_main_window(cx, move |window, cx| {
        // Apply saved settings (fonts, scrollbar, mode)
        // ZqlzSettings::apply sets mode/fonts from ZQLZ settings
        ZqlzSettings::global(cx).clone().apply(cx);

        // Create the main view
        let main_view = cx.new(|cx| MainView::new(window, cx));

        let startup_paths = drain_launch_target_paths(&launch_targets);
        if !startup_paths.is_empty() {
            main_view.update(cx, |main_view, cx| {
                for path in &startup_paths {
                    main_view.open_external_path(path, window, cx);
                }
            });
        }

        start_ui_job_runtime(&main_view, ui_job_receiver, window, cx);

        // Wrap in Root for theme support, dialogs, notifications, etc.
        cx.new(|cx| Root::new(main_view, window, cx))
    })
}

/// The hidden `--foreground` flag passed by `zqlz --foreground`, which asks this
/// process to attach to the launching terminal instead of detaching from it.
const FOREGROUND_FLAG: &str = "--foreground";

fn foreground_requested() -> bool {
    std::env::args_os()
        .skip(1)
        .any(|argument| argument == FOREGROUND_FLAG)
}

fn collect_launch_targets() -> Arc<Mutex<Vec<String>>> {
    let targets = std::env::args_os()
        .skip(1)
        .map(|argument| argument.to_string_lossy().into_owned())
        .filter(|argument| argument != FOREGROUND_FLAG)
        .collect::<Vec<_>>();
    Arc::new(Mutex::new(targets))
}

fn snapshot_launch_targets(launch_targets: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    match launch_targets.lock() {
        Ok(targets) => targets.clone(),
        Err(_) => {
            tracing::warn!("failed to acquire launch target queue");
            Vec::new()
        }
    }
}

pub(crate) fn path_from_open_target(target: &str) -> Option<PathBuf> {
    if let Ok(url) = Url::parse(target) {
        match url.scheme() {
            "file" => return url.to_file_path().ok(),
            "zqlz" => return path_from_zqlz_url(&url),
            _ => return None,
        }
    }

    Some(PathBuf::from(target))
}

fn drain_launch_target_paths(launch_targets: &Arc<Mutex<Vec<String>>>) -> Vec<PathBuf> {
    let Ok(mut pending_targets) = launch_targets.lock() else {
        tracing::warn!("failed to acquire pending launch target queue");
        return Vec::new();
    };

    if pending_targets.is_empty() {
        return Vec::new();
    }

    std::mem::take(&mut *pending_targets)
        .into_iter()
        .filter_map(|target| path_from_open_target(&target))
        .collect()
}

/// Runs work posted by the IPC server (forwarded open targets, MCP agent
/// tools) against the main view as it arrives.
fn start_ui_job_runtime(
    main_view: &Entity<MainView>,
    mut ui_job_receiver: ipc_server::UiJobReceiver,
    window: &mut Window,
    cx: &mut App,
) {
    main_view.update(cx, |_, cx| {
        cx.spawn_in(window, async move |main_view, cx| {
            while let Some(job) = ui_job_receiver.next().await {
                if let Err(error) =
                    main_view.update_in(cx, |main_view, window, cx| job(main_view, window, cx))
                {
                    tracing::debug!(%error, "stopping IPC UI job runtime");
                    break;
                }
            }
        })
        .detach();
    });
}

fn path_from_zqlz_url(url: &Url) -> Option<PathBuf> {
    let command = url
        .host_str()
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    if !command.is_empty() && command != "open" && command != "open-file" {
        tracing::debug!(url = %url, command = %command, "unsupported zqlz URL command");
        return None;
    }

    let query_target = url.query_pairs().find_map(|(key, value)| {
        let key = key.to_ascii_lowercase();
        match key.as_str() {
            "path" | "file" | "target" => Some(value.into_owned()),
            _ => None,
        }
    });

    if let Some(query_target) = query_target {
        if query_target.is_empty() {
            return None;
        }

        return path_from_open_target(&query_target);
    }

    let path = url.path();
    if path.is_empty() || path == "/" {
        return None;
    }

    #[cfg(windows)]
    {
        return Some(PathBuf::from(path.trim_start_matches('/')));
    }

    #[cfg(not(windows))]
    {
        Some(PathBuf::from(path))
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{drain_launch_target_paths, path_from_open_target};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    #[test]
    fn parses_plain_path_argument() {
        let expected = PathBuf::from("query.sql");
        assert_eq!(path_from_open_target("query.sql"), Some(expected));
    }

    #[test]
    fn parses_file_url() {
        #[cfg(windows)]
        {
            let expected = PathBuf::from(r"C:\temp\query.sql");
            assert_eq!(
                path_from_open_target("file:///C:/temp/query.sql"),
                Some(expected)
            );
        }

        #[cfg(not(windows))]
        {
            let expected = PathBuf::from("/tmp/query.sql");
            assert_eq!(
                path_from_open_target("file:///tmp/query.sql"),
                Some(expected)
            );
        }
    }

    #[test]
    fn parses_zqlz_open_query_target() {
        assert_eq!(
            path_from_open_target("zqlz://open?path=query.sql"),
            Some(PathBuf::from("query.sql"))
        );
    }

    #[test]
    fn parses_zqlz_open_file_url_target() {
        #[cfg(windows)]
        {
            let expected = PathBuf::from(r"C:\temp\query.sql");
            assert_eq!(
                path_from_open_target("zqlz://open?path=file:///C:/temp/query.sql"),
                Some(expected)
            );
        }

        #[cfg(not(windows))]
        {
            let expected = PathBuf::from("/tmp/query.sql");
            assert_eq!(
                path_from_open_target("zqlz://open?path=file:///tmp/query.sql"),
                Some(expected)
            );
        }
    }

    #[test]
    fn rejects_unknown_zqlz_command() {
        assert_eq!(path_from_open_target("zqlz://connect?id=abc"), None);
    }

    #[test]
    fn parses_zqlz_direct_path() {
        #[cfg(windows)]
        {
            let expected = PathBuf::from(r"C:\temp\query.sql");
            assert_eq!(
                path_from_open_target("zqlz:///C:/temp/query.sql"),
                Some(expected)
            );
        }

        #[cfg(not(windows))]
        {
            let expected = PathBuf::from("/tmp/query.sql");
            assert_eq!(
                path_from_open_target("zqlz:///tmp/query.sql"),
                Some(expected)
            );
        }
    }

    #[test]
    fn drain_launch_target_paths_parses_and_clears_queue() {
        let launch_targets = Arc::new(Mutex::new(vec![
            "query.sql".to_string(),
            "zqlz://open?path=from-url.sql".to_string(),
            "zqlz://connect?id=abc".to_string(),
        ]));

        let paths = drain_launch_target_paths(&launch_targets);
        assert_eq!(
            paths,
            vec![PathBuf::from("query.sql"), PathBuf::from("from-url.sql")]
        );

        let pending_targets = launch_targets
            .lock()
            .expect("launch target queue lock poisoned");
        assert!(pending_targets.is_empty());
    }
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
