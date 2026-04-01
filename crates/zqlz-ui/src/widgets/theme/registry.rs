use crate::widgets::{
    Theme, ThemeColor, ThemeConfig, ThemeMode, ThemeSet, highlighter::HighlightTheme,
};
use anyhow::Result;
use gpui::{App, Global, SharedString};
use notify::Watcher as _;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, LazyLock},
};

const DEFAULT_THEME: &str = include_str!("./default-theme.json");
type DefaultThemeColorMap = HashMap<ThemeMode, (Arc<ThemeColor>, Arc<HighlightTheme>)>;

pub(crate) static DEFAULT_THEME_COLORS: LazyLock<DefaultThemeColorMap> = LazyLock::new(|| {
    let mut colors = HashMap::new();

    let themes: Vec<ThemeConfig> = serde_json::from_str::<ThemeSet>(DEFAULT_THEME)
        .expect("Failed to parse themes/default.json")
        .themes;

    for theme in themes {
        let mut theme_color = ThemeColor::default();
        theme_color.apply_config(&theme, &ThemeColor::default());

        let highlight_theme = HighlightTheme {
            name: theme.name.to_string(),
            appearance: theme.mode,
            style: theme.highlight.unwrap_or_default(),
        };

        colors.insert(
            theme.mode,
            (Arc::new(theme_color), Arc::new(highlight_theme)),
        );
    }

    colors
});

pub(super) fn init(cx: &mut App) {
    cx.set_global(ThemeRegistry::default());
    ThemeRegistry::global_mut(cx).init_default_themes();

    // Observe changes to the theme registry to apply changes to the active theme
    cx.observe_global::<ThemeRegistry>(|cx| {
        let mode = Theme::global(cx).mode;
        let light_theme = Theme::global(cx).light_theme.name.clone();
        let dark_theme = Theme::global(cx).dark_theme.name.clone();

        if let Some(theme) = ThemeRegistry::global(cx)
            .themes()
            .get(&light_theme)
            .cloned()
        {
            Theme::global_mut(cx).light_theme = theme;
        }
        if let Some(theme) = ThemeRegistry::global(cx).themes().get(&dark_theme).cloned() {
            Theme::global_mut(cx).dark_theme = theme;
        }

        let theme_name = if mode.is_dark() {
            dark_theme
        } else {
            light_theme
        };

        tracing::info!("Reload active theme: {:?}...", theme_name);
        Theme::change(mode, None, cx);
        cx.refresh_windows();
    })
    .detach();
}

/// Lightweight metadata for a discoverable theme that may not yet be loaded.
#[derive(Clone, Debug)]
pub struct ThemeCatalogEntry {
    pub name: SharedString,
    pub is_default: bool,
}

/// Loads one or more concrete theme configs when a specific theme name is requested.
///
/// Implementations are expected to return all themes from the backing source file
/// when available so related themes become available after a single parse.
pub type LazyThemeLoader = fn(&str) -> Result<Vec<ThemeConfig>>;

#[derive(Default, Debug)]
pub struct ThemeRegistry {
    themes_dir: PathBuf,
    default_themes: HashMap<ThemeMode, Rc<ThemeConfig>>,
    themes: HashMap<SharedString, Rc<ThemeConfig>>,
    bundled_theme_catalog: HashMap<SharedString, ThemeCatalogEntry>,
    lazy_theme_loaders: Vec<LazyThemeLoader>,
    has_custom_themes: bool,
}

impl Global for ThemeRegistry {}

impl ThemeRegistry {
    pub fn global(cx: &App) -> &Self {
        cx.global::<Self>()
    }

    pub fn global_mut(cx: &mut App) -> &mut Self {
        cx.global_mut::<Self>()
    }

    /// Watch themes directory.
    ///
    /// And reload themes to trigger the `on_load` callback.
    pub fn watch_dir<F>(themes_dir: PathBuf, cx: &mut App, on_load: F) -> Result<()>
    where
        F: Fn(&mut App) + 'static,
    {
        Self::global_mut(cx).themes_dir = themes_dir.clone();

        // Load theme in the background.
        cx.spawn(async move |cx| {
            cx.update(|cx| {
                if let Err(err) = Self::_watch_themes_dir(themes_dir, cx) {
                    tracing::error!("Failed to watch themes directory: {}", err);
                }

                Self::reload_themes(cx);
                on_load(cx);
            });
        })
        .detach();

        Ok(())
    }

    /// Returns a reference to the map of themes (including default themes).
    pub fn themes(&self) -> &HashMap<SharedString, Rc<ThemeConfig>> {
        &self.themes
    }

    /// Returns a mutable reference to the map of themes.
    pub fn themes_mut(&mut self) -> &mut HashMap<SharedString, Rc<ThemeConfig>> {
        &mut self.themes
    }

    /// Returns a sorted list of themes.
    pub fn sorted_themes(&self) -> Vec<&Rc<ThemeConfig>> {
        let mut themes = self.themes.values().collect::<Vec<_>>();
        // sort by is_default true first, then light first dark later, then by name case-insensitive
        themes.sort_by(|a, b| {
            b.is_default
                .cmp(&a.is_default)
                .then(a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        themes
    }

    /// Returns a sorted list of all discoverable theme names.
    ///
    /// This includes already loaded themes as well as catalog-only bundled themes
    /// that can be loaded on demand later.
    pub fn sorted_theme_names(&self) -> Vec<SharedString> {
        let mut names: HashSet<SharedString> = self.themes.keys().cloned().collect();
        names.extend(self.bundled_theme_catalog.keys().cloned());

        let mut names: Vec<SharedString> = names.into_iter().collect();
        names.sort_by(|left, right| {
            self.theme_is_default(right)
                .cmp(&self.theme_is_default(left))
                .then(left.to_lowercase().cmp(&right.to_lowercase()))
        });
        names
    }

    /// Registers catalog entries for bundled themes that may be lazily loaded.
    pub fn register_theme_catalog<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = ThemeCatalogEntry>,
    {
        for entry in entries {
            self.bundled_theme_catalog
                .entry(entry.name.clone())
                .or_insert(entry);
        }
    }

    /// Registers a lazy loader callback for theme configs.
    pub fn register_lazy_theme_loader(&mut self, loader: LazyThemeLoader) {
        if self
            .lazy_theme_loaders
            .iter()
            .any(|existing_loader| *existing_loader as usize == loader as usize)
        {
            return;
        }

        self.lazy_theme_loaders.push(loader);
    }

    /// Ensures a theme config exists in the in-memory registry.
    ///
    /// Returns true if the requested theme is available after attempting lazy
    /// loaders, false otherwise.
    pub fn ensure_theme_loaded_by_name(&mut self, theme_name: &SharedString) -> bool {
        if self.themes.contains_key(theme_name) {
            return true;
        }

        let loaders = self.lazy_theme_loaders.clone();
        for loader in loaders {
            match loader(theme_name.as_ref()) {
                Ok(loaded_themes) => {
                    for theme in loaded_themes {
                        self.themes
                            .entry(theme.name.clone())
                            .or_insert_with(|| Rc::new(theme));
                    }

                    if self.themes.contains_key(theme_name) {
                        return true;
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        theme = %theme_name,
                        "failed to lazily load theme"
                    );
                }
            }
        }

        false
    }

    /// Returns a reference to the map of default themes.
    pub fn default_themes(&self) -> &HashMap<ThemeMode, Rc<ThemeConfig>> {
        &self.default_themes
    }

    pub fn default_light_theme(&self) -> &Rc<ThemeConfig> {
        &self.default_themes[&ThemeMode::Light]
    }

    pub fn default_dark_theme(&self) -> &Rc<ThemeConfig> {
        &self.default_themes[&ThemeMode::Dark]
    }

    fn theme_is_default(&self, theme_name: &SharedString) -> bool {
        self.themes
            .get(theme_name)
            .map(|theme| theme.is_default)
            .or_else(|| {
                self.bundled_theme_catalog
                    .get(theme_name)
                    .map(|entry| entry.is_default)
            })
            .unwrap_or(false)
    }

    fn init_default_themes(&mut self) {
        let default_themes: Vec<ThemeConfig> = serde_json::from_str::<ThemeSet>(DEFAULT_THEME)
            .expect("failed to parse default theme.")
            .themes;
        for theme in default_themes.into_iter() {
            if theme.mode.is_dark() {
                self.default_themes.insert(ThemeMode::Dark, Rc::new(theme));
            } else {
                self.default_themes.insert(ThemeMode::Light, Rc::new(theme));
            }
        }
        self.themes = self
            .default_themes
            .values()
            .map(|theme| {
                let name = theme.name.clone();
                (name, Rc::clone(theme))
            })
            .collect();
    }

    fn _watch_themes_dir(themes_dir: PathBuf, cx: &mut App) -> anyhow::Result<()> {
        if !themes_dir.exists() {
            fs::create_dir_all(&themes_dir)?;
        }

        let (tx, rx) = smol::channel::bounded(100);
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if let Ok(event) = &res {
                    match event.kind {
                        notify::EventKind::Create(_)
                        | notify::EventKind::Modify(_)
                        | notify::EventKind::Remove(_) => {
                            if let Err(err) = tx.send_blocking(res) {
                                tracing::error!("Failed to send theme event: {:?}", err);
                            }
                        }
                        _ => {}
                    }
                }
            })?;

        cx.spawn(async move |cx| {
            if let Err(err) = watcher.watch(&themes_dir, notify::RecursiveMode::Recursive) {
                tracing::error!("Failed to watch themes directory: {:?}", err);
            }

            while (rx.recv().await).is_ok() {
                tracing::info!("Reloading themes...");
                cx.update(Self::reload_themes);
            }
        })
        .detach();

        Ok(())
    }

    fn reload_themes(cx: &mut App) {
        let registry = Self::global_mut(cx);
        match registry.reload() {
            Ok(_) => {
                tracing::info!("Themes reloaded successfully.");
            }
            Err(e) => tracing::error!("Failed to reload themes: {:?}", e),
        }
    }

    /// Reload themes from the `themes_dir`.
    fn reload(&mut self) -> Result<()> {
        let mut themes = vec![];

        if self.themes_dir.exists() {
            for entry in fs::read_dir(&self.themes_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    let file_content = fs::read_to_string(path.clone())?;

                    match serde_json::from_str::<ThemeSet>(&file_content) {
                        Ok(theme_set) => {
                            themes.extend(theme_set.themes);
                        }
                        Err(e) => {
                            tracing::error!(
                                "ignored invalid theme file: {}, {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }

        self.themes.clear();
        for theme in self.default_themes.values() {
            self.themes
                .insert(theme.name.clone(), Rc::new((**theme).clone()));
        }

        for theme in themes.iter() {
            if self.themes.contains_key(&theme.name) {
                continue;
            }

            if theme.is_default {
                self.default_themes
                    .insert(theme.mode, Rc::new(theme.clone()));
            }

            self.has_custom_themes = true;
            self.themes
                .insert(theme.name.clone(), Rc::new(theme.clone()));
        }

        Ok(())
    }
}
