//! Settings Panel
//!
//! A panel for viewing and editing application settings.

use crate::{
    AiProvider, CursorBlink, CursorShape, InlineSuggestionProvider, ScrollBeyondLastLine,
    ScrollbarVisibility, SearchWrap, SqlDialect, ThemeModePreference, ZqlzSettings,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    slider::{Slider, SliderEvent, SliderState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

/// Events emitted by the settings panel
#[derive(Clone, Debug)]
pub enum SettingsPanelEvent {
    SettingsChanged,
}

/// A custom select item for theme mode selection
#[derive(Clone, Debug)]
struct ThemeModeItem {
    value: ThemeModePreference,
    label: SharedString,
}

impl SelectItem for ThemeModeItem {
    type Value = ThemeModePreference;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// A custom select item for scrollbar visibility
#[derive(Clone, Debug)]
struct ScrollbarItem {
    value: ScrollbarVisibility,
    label: SharedString,
}

impl SelectItem for ScrollbarItem {
    type Value = ScrollbarVisibility;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// A custom select item for theme selection
#[derive(Clone, Debug)]
struct ThemeItem {
    name: SharedString,
}

impl SelectItem for ThemeItem {
    type Value = SharedString;

    fn title(&self) -> SharedString {
        self.name.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }
}

/// A custom select item for font selection
#[derive(Clone, Debug)]
struct FontItem {
    name: SharedString,
}

impl SelectItem for FontItem {
    type Value = SharedString;

    fn title(&self) -> SharedString {
        self.name.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }
}

/// A custom select item for SQL dialect selection
#[derive(Clone, Debug)]
struct SqlDialectItem {
    value: SqlDialect,
    label: SharedString,
}

impl SelectItem for SqlDialectItem {
    type Value = SqlDialect;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// A custom select item for inline suggestion provider selection
#[derive(Clone, Debug)]
struct InlineSuggestionProviderItem {
    value: InlineSuggestionProvider,
    label: SharedString,
}

impl SelectItem for InlineSuggestionProviderItem {
    type Value = InlineSuggestionProvider;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// A custom select item for AI provider selection
#[derive(Clone, Debug)]
struct AiProviderItem {
    value: AiProvider,
    label: SharedString,
}

impl SelectItem for AiProviderItem {
    type Value = AiProvider;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// A custom select item for cursor blink settings
#[derive(Clone, Debug)]
struct CursorBlinkItem {
    value: CursorBlink,
    label: SharedString,
}

impl SelectItem for CursorBlinkItem {
    type Value = CursorBlink;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl CursorBlink {
    fn display_name(&self) -> &'static str {
        match self {
            CursorBlink::On => "On",
            CursorBlink::Off => "Off",
            CursorBlink::System => "System",
        }
    }

    fn all() -> &'static [Self] {
        &[Self::On, Self::Off, Self::System]
    }
}

/// A custom select item for cursor shape settings
#[derive(Clone, Debug)]
struct CursorShapeItem {
    value: CursorShape,
    label: SharedString,
}

impl SelectItem for CursorShapeItem {
    type Value = CursorShape;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl CursorShape {
    fn display_name(&self) -> &'static str {
        match self {
            CursorShape::Block => "Block",
            CursorShape::Line => "Line",
            CursorShape::Underline => "Underline",
        }
    }

    fn all() -> &'static [Self] {
        &[Self::Block, Self::Line, Self::Underline]
    }
}

/// A custom select item for scroll beyond last line settings
#[derive(Clone, Debug)]
struct ScrollBeyondLastLineItem {
    value: ScrollBeyondLastLine,
    label: SharedString,
}

impl SelectItem for ScrollBeyondLastLineItem {
    type Value = ScrollBeyondLastLine;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl ScrollBeyondLastLine {
    fn display_name(&self) -> &'static str {
        match self {
            ScrollBeyondLastLine::Disabled => "Disabled",
            ScrollBeyondLastLine::Enabled => "Enabled",
            ScrollBeyondLastLine::HorizontalScrollbar => "Horizontal Scrollbar",
        }
    }

    fn all() -> &'static [Self] {
        &[Self::Disabled, Self::Enabled, Self::HorizontalScrollbar]
    }
}

/// A custom select item for search wrap settings
#[derive(Clone, Debug)]
struct SearchWrapItem {
    value: SearchWrap,
    label: SharedString,
}

impl SelectItem for SearchWrapItem {
    type Value = SearchWrap;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl SearchWrap {
    fn display_name(&self) -> &'static str {
        match self {
            SearchWrap::Disabled => "Disabled",
            SearchWrap::Enabled => "Wrap",
            SearchWrap::NoWrap => "No Wrap",
        }
    }

    fn all() -> &'static [Self] {
        &[Self::Disabled, Self::Enabled, Self::NoWrap]
    }
}

/// Settings panel for editing application settings
pub struct SettingsPanel {
    focus_handle: FocusHandle,

    // Appearance settings
    theme_mode_state: Entity<SelectState<SearchableVec<ThemeModeItem>>>,
    light_theme_state: Entity<SelectState<SearchableVec<ThemeItem>>>,
    dark_theme_state: Entity<SelectState<SearchableVec<ThemeItem>>>,
    scrollbar_state: Entity<SelectState<SearchableVec<ScrollbarItem>>>,

    // Font settings
    ui_font_size_state: Entity<SliderState>,
    ui_font_weight_state: Entity<SliderState>,
    editor_font_size_state: Entity<SliderState>,
    editor_font_weight_state: Entity<SliderState>,
    ui_font_state: Entity<SelectState<SearchableVec<FontItem>>>,
    editor_font_state: Entity<SelectState<SearchableVec<FontItem>>>,

    // Editor settings - tab size slider
    tab_size_state: Entity<SliderState>,

    // Cursor and selection settings
    cursor_blink_state: Entity<SelectState<SearchableVec<CursorBlinkItem>>>,
    cursor_shape_state: Entity<SelectState<SearchableVec<CursorShapeItem>>>,

    // Scroll behavior settings
    scroll_beyond_last_line_state: Entity<SelectState<SearchableVec<ScrollBeyondLastLineItem>>>,
    vertical_scroll_margin_state: Entity<SliderState>,
    horizontal_scroll_margin_state: Entity<SliderState>,
    scroll_sensitivity_state: Entity<SliderState>,

    // Search behavior settings
    search_wrap_state: Entity<SelectState<SearchableVec<SearchWrapItem>>>,

    // Syntax highlighting settings
    sql_dialect_state: Entity<SelectState<SearchableVec<SqlDialectItem>>>,

    // Inline suggestion settings
    inline_suggestions_provider_state:
        Entity<SelectState<SearchableVec<InlineSuggestionProviderItem>>>,
    inline_suggestions_delay_state: Entity<SliderState>,

    // AI settings
    _ai_provider_state: Entity<SelectState<SearchableVec<AiProviderItem>>>,
    ai_api_key_state: Entity<InputState>,

    _subscriptions: Vec<Subscription>,
}

impl SettingsPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // Get all system fonts
        let system_fonts: Vec<FontItem> = cx
            .text_system()
            .all_font_names()
            .into_iter()
            .map(|name| FontItem {
                name: SharedString::from(name),
            })
            .collect();

        // Clone settings values we need to avoid holding references
        let (
            theme_mode,
            light_theme,
            dark_theme,
            scrollbar_vis,
            ui_font_size,
            ui_font_weight,
            editor_font_size,
            editor_font_weight,
            tab_size,
            ui_font_family,
            editor_font_family,
            sql_dialect,
            inline_suggestions_provider,
            inline_suggestions_delay_ms,
            ai_provider,
            _ai_api_key,
            // Cursor and selection settings
            cursor_blink,
            cursor_shape,
            _selection_highlight,
            _rounded_selection,
            _relative_line_numbers,
            // Scroll behavior settings
            scroll_beyond_last_line,
            vertical_scroll_margin,
            horizontal_scroll_margin,
            scroll_sensitivity,
            _autoscroll_on_clicks,
            // Search behavior settings
            search_wrap,
            _use_smartcase_search,
        ) = {
            let settings = ZqlzSettings::global(cx);
            (
                settings.appearance.theme_mode,
                settings.appearance.light_theme.clone(),
                settings.appearance.dark_theme.clone(),
                settings.appearance.show_scrollbars,
                settings.fonts.ui_font_size,
                settings.fonts.ui_font_weight,
                settings.fonts.editor_font_size,
                settings.fonts.editor_font_weight,
                settings.editor.tab_size,
                settings.fonts.ui_font_family.clone(),
                settings.fonts.editor_font_family.clone(),
                settings.editor.sql_dialect,
                settings.editor.inline_suggestions_provider,
                settings.editor.inline_suggestions_delay_ms,
                settings.editor.ai_provider,
                settings.editor.ai_api_key.clone(),
                // Cursor and selection settings
                settings.editor.cursor_blink,
                settings.editor.cursor_shape,
                settings.editor.selection_highlight,
                settings.editor.rounded_selection,
                settings.editor.relative_line_numbers,
                // Scroll behavior settings
                settings.editor.scroll_beyond_last_line,
                settings.editor.vertical_scroll_margin,
                settings.editor.horizontal_scroll_margin,
                settings.editor.scroll_sensitivity,
                settings.editor.autoscroll_on_clicks,
                // Search behavior settings
                settings.editor.search_wrap,
                settings.editor.use_smartcase_search,
            )
        };

        // Build theme mode items
        let theme_mode_items: Vec<ThemeModeItem> = ThemeModePreference::all()
            .iter()
            .map(|m| ThemeModeItem {
                value: *m,
                label: m.display_name().into(),
            })
            .collect();
        let theme_mode_index = theme_mode_items
            .iter()
            .position(|item| item.value == theme_mode);

        // Build theme items from registry
        let themes: Vec<ThemeItem> = zqlz_ui::widgets::ThemeRegistry::global(cx)
            .sorted_themes()
            .iter()
            .map(|t| ThemeItem {
                name: t.name.clone(),
            })
            .collect();

        let light_theme_index = themes.iter().position(|t| t.name == light_theme);
        let dark_theme_index = themes.iter().position(|t| t.name == dark_theme);

        // Find font indices
        let ui_font_index = system_fonts.iter().position(|f| f.name == ui_font_family);
        let editor_font_index = system_fonts
            .iter()
            .position(|f| f.name == editor_font_family);

        // Build scrollbar items
        let scrollbar_items: Vec<ScrollbarItem> = ScrollbarVisibility::all()
            .iter()
            .map(|s| ScrollbarItem {
                value: *s,
                label: s.display_name().into(),
            })
            .collect();
        let scrollbar_index = scrollbar_items
            .iter()
            .position(|item| item.value == scrollbar_vis);

        // Create select states
        let theme_mode_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(theme_mode_items),
                theme_mode_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let light_theme_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(themes.clone()),
                light_theme_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
            .searchable(true)
        });

        let dark_theme_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(themes),
                dark_theme_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
            .searchable(true)
        });

        let scrollbar_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(scrollbar_items),
                scrollbar_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create font select states
        let ui_font_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(system_fonts.clone()),
                ui_font_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
            .searchable(true)
        });

        let editor_font_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(system_fonts),
                editor_font_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
            .searchable(true)
        });

        // Create slider states for font sizes
        let ui_font_size_state = cx.new(|_| {
            SliderState::new()
                .min(10.0)
                .max(24.0)
                .step(1.0)
                .default_value(ui_font_size)
        });

        let ui_font_weight_state = cx.new(|_| {
            SliderState::new()
                .min(100.0)
                .max(900.0)
                .step(100.0)
                .default_value(ui_font_weight as f32)
        });

        let editor_font_size_state = cx.new(|_| {
            SliderState::new()
                .min(10.0)
                .max(32.0)
                .step(1.0)
                .default_value(editor_font_size)
        });

        let editor_font_weight_state = cx.new(|_| {
            SliderState::new()
                .min(100.0)
                .max(900.0)
                .step(100.0)
                .default_value(editor_font_weight as f32)
        });

        // Create tab size slider
        let tab_size_state = cx.new(|_| {
            SliderState::new()
                .min(1.0)
                .max(8.0)
                .step(1.0)
                .default_value(tab_size as f32)
        });

        // Create cursor blink select
        let cursor_blink_items: Vec<CursorBlinkItem> = CursorBlink::all()
            .iter()
            .map(|c| CursorBlinkItem {
                value: *c,
                label: c.display_name().into(),
            })
            .collect();
        let cursor_blink_index = cursor_blink_items
            .iter()
            .position(|item| item.value == cursor_blink);
        let cursor_blink_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(cursor_blink_items),
                cursor_blink_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create cursor shape select
        let cursor_shape_items: Vec<CursorShapeItem> = CursorShape::all()
            .iter()
            .map(|c| CursorShapeItem {
                value: *c,
                label: c.display_name().into(),
            })
            .collect();
        let cursor_shape_index = cursor_shape_items
            .iter()
            .position(|item| item.value == cursor_shape);
        let cursor_shape_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(cursor_shape_items),
                cursor_shape_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create scroll beyond last line select
        let scroll_beyond_items: Vec<ScrollBeyondLastLineItem> = ScrollBeyondLastLine::all()
            .iter()
            .map(|s| ScrollBeyondLastLineItem {
                value: *s,
                label: s.display_name().into(),
            })
            .collect();
        let scroll_beyond_index = scroll_beyond_items
            .iter()
            .position(|item| item.value == scroll_beyond_last_line);
        let scroll_beyond_last_line_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(scroll_beyond_items),
                scroll_beyond_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create scroll margin sliders
        let vertical_scroll_margin_state = cx.new(|_| {
            SliderState::new()
                .min(0.0)
                .max(20.0)
                .step(1.0)
                .default_value(vertical_scroll_margin as f32)
        });

        let horizontal_scroll_margin_state = cx.new(|_| {
            SliderState::new()
                .min(0.0)
                .max(20.0)
                .step(1.0)
                .default_value(horizontal_scroll_margin as f32)
        });

        let scroll_sensitivity_state = cx.new(|_| {
            SliderState::new()
                .min(0.1)
                .max(5.0)
                .step(0.1)
                .default_value(scroll_sensitivity)
        });

        // Create search wrap select
        let search_wrap_items: Vec<SearchWrapItem> = SearchWrap::all()
            .iter()
            .map(|s| SearchWrapItem {
                value: *s,
                label: s.display_name().into(),
            })
            .collect();
        let search_wrap_index = search_wrap_items
            .iter()
            .position(|item| item.value == search_wrap);
        let search_wrap_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(search_wrap_items),
                search_wrap_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create SQL dialect select
        let sql_dialect_items: Vec<SqlDialectItem> = SqlDialect::all()
            .iter()
            .map(|d| SqlDialectItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let sql_dialect_index = sql_dialect_items
            .iter()
            .position(|item| item.value == sql_dialect);
        let sql_dialect_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(sql_dialect_items),
                sql_dialect_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create inline suggestions provider select
        let provider_items: Vec<InlineSuggestionProviderItem> = InlineSuggestionProvider::all()
            .iter()
            .map(|p| InlineSuggestionProviderItem {
                value: *p,
                label: p.display_name().into(),
            })
            .collect();
        let provider_index = provider_items
            .iter()
            .position(|item| item.value == inline_suggestions_provider);
        let inline_suggestions_provider_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(provider_items),
                provider_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create inline suggestions delay slider (0-1000ms)
        let inline_suggestions_delay_state = cx.new(|_| {
            SliderState::new()
                .min(0.0)
                .max(1000.0)
                .step(50.0)
                .default_value(inline_suggestions_delay_ms as f32)
        });

        // Create AI provider select
        let ai_provider_items: Vec<AiProviderItem> = AiProvider::all()
            .iter()
            .map(|p| AiProviderItem {
                value: *p,
                label: p.display_name().into(),
            })
            .collect();
        let ai_provider_index = ai_provider_items
            .iter()
            .position(|item| item.value == ai_provider);
        let ai_provider_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(ai_provider_items),
                ai_provider_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Create AI API key input (masked)
        let ai_api_key_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_masked(true, window, cx);
            state
        });

        // Subscribe to select changes
        let mut subscriptions = Vec::new();

        subscriptions.push(cx.subscribe(
            &theme_mode_state,
            |this, _, event: &SelectEvent<SearchableVec<ThemeModeItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let (appearance, fonts) = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.appearance.theme_mode = *value;
                        (settings.appearance.clone(), settings.fonts.clone())
                    };
                    appearance.apply_with_fonts(&fonts, cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &light_theme_state,
            |this, _, event: &SelectEvent<SearchableVec<ThemeItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let (appearance, fonts) = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.appearance.light_theme = value.clone();
                        (settings.appearance.clone(), settings.fonts.clone())
                    };
                    appearance.apply_with_fonts(&fonts, cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &dark_theme_state,
            |this, _, event: &SelectEvent<SearchableVec<ThemeItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let (appearance, fonts) = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.appearance.dark_theme = value.clone();
                        (settings.appearance.clone(), settings.fonts.clone())
                    };
                    appearance.apply_with_fonts(&fonts, cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &scrollbar_state,
            |this, _, event: &SelectEvent<SearchableVec<ScrollbarItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let (appearance, fonts) = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.appearance.show_scrollbars = *value;
                        (settings.appearance.clone(), settings.fonts.clone())
                    };
                    appearance.apply_with_fonts(&fonts, cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &ui_font_size_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let fonts = {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.fonts.ui_font_size = value.end();
                    settings.fonts.clone()
                };
                fonts.apply(cx);
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &ui_font_weight_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let fonts = {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.fonts.ui_font_weight = value.end() as u16;
                    settings.fonts.clone()
                };
                fonts.apply(cx);
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &editor_font_size_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let fonts = {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.fonts.editor_font_size = value.end();
                    settings.fonts.clone()
                };
                fonts.apply(cx);
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &editor_font_weight_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let fonts = {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.fonts.editor_font_weight = value.end() as u16;
                    settings.fonts.clone()
                };
                fonts.apply(cx);
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(
            cx.subscribe(&tab_size_state, |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let settings = ZqlzSettings::global_mut(cx);
                settings.editor.tab_size = value.end() as u32;
                cx.notify();
                this.emit_changed(cx);
            }),
        );

        subscriptions.push(cx.subscribe(
            &ui_font_state,
            |this, _, event: &SelectEvent<SearchableVec<FontItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let fonts = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.fonts.ui_font_family = value.clone();
                        settings.fonts.clone()
                    };
                    fonts.apply(cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &editor_font_state,
            |this, _, event: &SelectEvent<SearchableVec<FontItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let fonts = {
                        let settings = ZqlzSettings::global_mut(cx);
                        settings.fonts.editor_font_family = value.clone();
                        settings.fonts.clone()
                    };
                    fonts.apply(cx);
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &sql_dialect_state,
            |this, _, event: &SelectEvent<SearchableVec<SqlDialectItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.sql_dialect = *value;
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &inline_suggestions_provider_state,
            |this, _, event: &SelectEvent<SearchableVec<InlineSuggestionProviderItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.inline_suggestions_provider = *value;
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &inline_suggestions_delay_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let settings = ZqlzSettings::global_mut(cx);
                settings.editor.inline_suggestions_delay_ms = value.end() as u32;
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &ai_provider_state,
            |this, _, event: &SelectEvent<SearchableVec<AiProviderItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.ai_provider = *value;
                    // Update default model for the new provider
                    settings.editor.ai_model = value.default_model().into();
                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(
            cx.subscribe(&ai_api_key_state, |this, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    // Read the API key value first to avoid borrow conflict
                    let value = {
                        let api_key = this.ai_api_key_state.read(cx);
                        api_key.unmask_value()
                    };
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.ai_api_key = if value.is_empty() {
                        None
                    } else {
                        Some(value.into())
                    };
                    cx.notify();
                    this.emit_changed(cx);
                }
            }),
        );

        // Subscribe to cursor and selection settings changes
        subscriptions.push(cx.subscribe(
            &cursor_blink_state,
            |this, _, event: &SelectEvent<SearchableVec<CursorBlinkItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.cursor_blink = *value;

                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &cursor_shape_state,
            |this, _, event: &SelectEvent<SearchableVec<CursorShapeItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.cursor_shape = *value;

                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &scroll_beyond_last_line_state,
            |this, _, event: &SelectEvent<SearchableVec<ScrollBeyondLastLineItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.scroll_beyond_last_line = *value;

                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &vertical_scroll_margin_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let settings = ZqlzSettings::global_mut(cx);
                settings.editor.vertical_scroll_margin = value.end() as u32;
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &horizontal_scroll_margin_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let settings = ZqlzSettings::global_mut(cx);
                settings.editor.horizontal_scroll_margin = value.end() as u32;
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &scroll_sensitivity_state,
            |this, _, event: &SliderEvent, cx| {
                let SliderEvent::Change(value) = event;
                let settings = ZqlzSettings::global_mut(cx);
                settings.editor.scroll_sensitivity = value.end();
                cx.notify();
                this.emit_changed(cx);
            },
        ));

        subscriptions.push(cx.subscribe(
            &search_wrap_state,
            |this, _, event: &SelectEvent<SearchableVec<SearchWrapItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.search_wrap = *value;

                    cx.notify();
                    this.emit_changed(cx);
                }
            },
        ));

        Self {
            focus_handle: cx.focus_handle(),
            theme_mode_state,
            light_theme_state,
            dark_theme_state,
            scrollbar_state,
            ui_font_size_state,
            ui_font_weight_state,
            editor_font_size_state,
            editor_font_weight_state,
            ui_font_state,
            editor_font_state,
            tab_size_state,
            // Cursor and selection settings
            cursor_blink_state,
            cursor_shape_state,
            // Scroll behavior settings
            scroll_beyond_last_line_state,
            vertical_scroll_margin_state,
            horizontal_scroll_margin_state,
            scroll_sensitivity_state,
            // Search behavior settings
            search_wrap_state,
            sql_dialect_state,
            inline_suggestions_provider_state,
            inline_suggestions_delay_state,
            _ai_provider_state: ai_provider_state,
            ai_api_key_state,
            _subscriptions: subscriptions,
        }
    }

    fn emit_changed(&self, cx: &mut Context<Self>) {
        cx.emit(SettingsPanelEvent::SettingsChanged);
    }

    fn render_section_header(&self, title: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .py_2()
            .px_3()
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.title_bar)
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child(title.to_string()),
            )
    }

    fn render_subsection_header(&self, title: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        h_flex().w_full().py_1().px_3().child(
            div()
                .text_xs()
                .font_weight(FontWeight::MEDIUM)
                .text_color(theme.muted_foreground)
                .child(title.to_string()),
        )
    }

    fn render_setting_row(
        &self,
        label: &str,
        description: Option<&str>,
        control: impl IntoElement,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .py_2()
            .px_3()
            .gap_4()
            .items_center()
            .justify_between()
            .child(
                v_flex()
                    .flex_1()
                    .gap_0p5()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(label.to_string()),
                    )
                    .when_some(description, |this, desc| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child(desc.to_string()),
                        )
                    }),
            )
            .child(div().w(px(200.0)).flex_shrink_0().child(control))
    }

    fn render_toggle_row(
        &self,
        id: impl Into<ElementId>,
        label: &str,
        description: Option<&str>,
        checked: bool,
        on_change: impl Fn(bool, &mut Window, &mut App) + 'static,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .py_2()
            .px_3()
            .gap_4()
            .items_center()
            .justify_between()
            .child(
                v_flex()
                    .flex_1()
                    .gap_0p5()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(label.to_string()),
                    )
                    .when_some(description, |this, desc| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child(desc.to_string()),
                        )
                    }),
            )
            .child(
                Switch::new(id)
                    .checked(checked)
                    .on_click(move |checked, window, cx| {
                        on_change(*checked, window, cx);
                    }),
            )
    }

    fn render_appearance_section(&self, cx: &Context<Self>) -> impl IntoElement {
        v_flex()
            .w_full()
            .child(self.render_section_header("Appearance", cx))
            .child(self.render_setting_row(
                "Theme Mode",
                Some("Choose between light, dark, or system theme"),
                Select::new(&self.theme_mode_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Light Theme",
                Some("Theme used when in light mode"),
                Select::new(&self.light_theme_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Dark Theme",
                Some("Theme used when in dark mode"),
                Select::new(&self.dark_theme_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Scrollbar",
                Some("When to show scrollbars"),
                Select::new(&self.scrollbar_state).small(),
                cx,
            ))
    }

    fn render_fonts_section(&self, cx: &Context<Self>) -> impl IntoElement {
        let settings = ZqlzSettings::global(cx);

        // Helper to get font weight label
        let get_weight_label = |weight: u16| -> String {
            match weight {
                100 => "100 (Thin)".to_string(),
                200 => "200 (Extra Light)".to_string(),
                300 => "300 (Light)".to_string(),
                400 => "400 (Normal)".to_string(),
                500 => "500 (Medium)".to_string(),
                600 => "600 (Semi Bold)".to_string(),
                700 => "700 (Bold)".to_string(),
                800 => "800 (Extra Bold)".to_string(),
                900 => "900 (Black)".to_string(),
                _ => weight.to_string(),
            }
        };

        v_flex()
            .w_full()
            .child(self.render_section_header("Fonts", cx))
            .child(self.render_setting_row(
                "UI Font",
                Some("Font family for UI elements"),
                Select::new(&self.ui_font_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "UI Font Size",
                Some(&format!("{}px", settings.fonts.ui_font_size as i32)),
                Slider::new(&self.ui_font_size_state),
                cx,
            ))
            .child(self.render_setting_row(
                "UI Font Weight",
                Some(&get_weight_label(settings.fonts.ui_font_weight)),
                Slider::new(&self.ui_font_weight_state),
                cx,
            ))
            .child(self.render_setting_row(
                "Editor Font",
                Some("Font family for the query editor"),
                Select::new(&self.editor_font_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Editor Font Size",
                Some(&format!("{}px", settings.fonts.editor_font_size as i32)),
                Slider::new(&self.editor_font_size_state),
                cx,
            ))
            .child(self.render_setting_row(
                "Editor Font Weight",
                Some(&get_weight_label(settings.fonts.editor_font_weight)),
                Slider::new(&self.editor_font_weight_state),
                cx,
            ))
    }

    fn render_editor_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let settings = ZqlzSettings::global(cx);

        v_flex()
            .w_full()
            .child(self.render_section_header("Editor", cx))
            .child(self.render_setting_row(
                "Tab Size",
                Some(&format!("{} spaces", settings.editor.tab_size)),
                Slider::new(&self.tab_size_state),
                cx,
            ))
            .child(self.render_toggle_row(
                "insert-spaces",
                "Insert Spaces",
                Some("Use spaces instead of tabs"),
                settings.editor.insert_spaces,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.insert_spaces = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "show-line-numbers",
                "Show Line Numbers",
                Some("Display line numbers in the editor"),
                settings.editor.show_line_numbers,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.show_line_numbers = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "word-wrap",
                "Word Wrap",
                Some("Wrap long lines to fit the editor width"),
                settings.editor.word_wrap,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.word_wrap = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "highlight-current-line",
                "Highlight Current Line",
                Some("Highlight the line containing the cursor"),
                settings.editor.highlight_current_line,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.highlight_current_line = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "inline-diagnostics",
                "Inline Diagnostics",
                Some("Show inline diagnostics and hover details"),
                settings.editor.show_inline_diagnostics,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.show_inline_diagnostics = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "auto-indent",
                "Auto Indent",
                Some("Automatically indent new lines"),
                settings.editor.auto_indent,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.auto_indent = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "bracket-matching",
                "Bracket Matching",
                Some("Highlight matching brackets"),
                settings.editor.bracket_matching,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.bracket_matching = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "vim-mode",
                "Vim Mode",
                Some("Enable Vim keybindings and editing mode"),
                settings.editor.vim_mode_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.vim_mode_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "highlight-enabled",
                "Syntax Highlighting",
                Some("Enable SQL syntax highlighting"),
                settings.editor.highlight_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.highlight_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            // Cursor & Selection settings subsection
            .child(self.render_subsection_header("Cursor & Selection", cx))
            .child(self.render_setting_row(
                "Cursor Blink",
                Some("Cursor blink behavior"),
                Select::new(&self.cursor_blink_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Cursor Shape",
                Some("Cursor shape in the editor"),
                Select::new(&self.cursor_shape_state).small(),
                cx,
            ))
            .child(self.render_toggle_row(
                "selection-highlight",
                "Selection Highlight",
                Some("Highlight other selections containing the cursor"),
                settings.editor.selection_highlight,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.selection_highlight = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "rounded-selection",
                "Rounded Selection",
                Some("Use rounded corners for selections"),
                settings.editor.rounded_selection,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.rounded_selection = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "relative-line-numbers",
                "Relative Line Numbers",
                Some("Show line numbers relative to cursor position"),
                settings.editor.relative_line_numbers,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.relative_line_numbers = checked;
                    let _ = _window;
                },
                cx,
            ))
            // Scroll settings subsection
            .child(self.render_subsection_header("Scroll", cx))
            .child(self.render_setting_row(
                "Scroll Beyond Last Line",
                Some("Allow scrolling past the end of the document"),
                Select::new(&self.scroll_beyond_last_line_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Vertical Scroll Margin",
                Some(&format!("{} lines", settings.editor.vertical_scroll_margin)),
                Slider::new(&self.vertical_scroll_margin_state),
                cx,
            ))
            .child(self.render_setting_row(
                "Horizontal Scroll Margin",
                Some(&format!(
                    "{} lines",
                    settings.editor.horizontal_scroll_margin
                )),
                Slider::new(&self.horizontal_scroll_margin_state),
                cx,
            ))
            .child(self.render_setting_row(
                "Scroll Sensitivity",
                Some(&format!("{:.1}x", settings.editor.scroll_sensitivity)),
                Slider::new(&self.scroll_sensitivity_state),
                cx,
            ))
            .child(self.render_toggle_row(
                "autoscroll-on-clicks",
                "Autoscroll on Clicks",
                Some("Automatically scroll to keep cursor visible when clicking"),
                settings.editor.autoscroll_on_clicks,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.autoscroll_on_clicks = checked;
                    let _ = _window;
                },
                cx,
            ))
            // Search settings subsection
            .child(self.render_subsection_header("Search", cx))
            .child(self.render_setting_row(
                "Search Wrap",
                Some("Wrap around when searching"),
                Select::new(&self.search_wrap_state).small(),
                cx,
            ))
            .child(self.render_toggle_row(
                "use-smartcase-search",
                "Smart Case",
                Some("Ignore case unless search contains uppercase"),
                settings.editor.use_smartcase_search,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.use_smartcase_search = checked;
                    let _ = _window;
                },
                cx,
            ))
            // LSP (Language Server) settings subsection
            .child(self.render_subsection_header("Language Server (LSP)", cx))
            .child(self.render_toggle_row(
                "lsp-enabled",
                "Enable LSP",
                Some("Enable Language Server Protocol features"),
                settings.editor.lsp_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "lsp-completions",
                "Completions",
                Some("Enable auto-completion suggestions"),
                settings.editor.lsp_completions_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_completions_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "lsp-hover",
                "Hover",
                Some("Show hover information on hover"),
                settings.editor.lsp_hover_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_hover_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "lsp-diagnostics",
                "Diagnostics",
                Some("Show inline errors and warnings"),
                settings.editor.lsp_diagnostics_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_diagnostics_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "lsp-code-actions",
                "Code Actions",
                Some("Enable quick fixes and refactorings"),
                settings.editor.lsp_code_actions_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_code_actions_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "lsp-rename",
                "Rename",
                Some("Enable symbol rename refactoring"),
                settings.editor.lsp_rename_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.lsp_rename_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            // Inline suggestions subsection
            .child(self.render_subsection_header("Inline Suggestions", cx))
            .child(self.render_toggle_row(
                "inline-suggestions",
                "Enable Inline Suggestions",
                Some("Show inline code completions as you type"),
                settings.editor.inline_suggestions_enabled,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.inline_suggestions_enabled = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_setting_row(
                "Suggestion Provider",
                Some("Source for inline suggestions"),
                Select::new(&self.inline_suggestions_provider_state).small(),
                cx,
            ))
            .child(self.render_setting_row(
                "Suggestion Delay",
                Some(&format!(
                    "{}ms delay before showing suggestions",
                    settings.editor.inline_suggestions_delay_ms
                )),
                Slider::new(&self.inline_suggestions_delay_state),
                cx,
            ))
            // AI settings subsection
            .child(self.render_subsection_header("AI Completion", cx))
            .child(self.render_setting_row(
                "API Key",
                Some("API key for AI provider (leave empty to use default)"),
                Input::new(&self.ai_api_key_state).mask_toggle().small(),
                cx,
            ))
            .child(self.render_setting_row(
                "SQL Dialect",
                Some("SQL dialect for syntax highlighting"),
                Select::new(&self.sql_dialect_state).small(),
                cx,
            ))
            // Display settings subsection
            .child(self.render_subsection_header("Display", cx))
            .child(self.render_toggle_row(
                "show-gutter-diagnostics",
                "Show Gutter Diagnostics",
                Some("Display diagnostic indicators in the gutter"),
                settings.editor.show_gutter_diagnostics,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.show_gutter_diagnostics = checked;
                    let _ = _window;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "show-folding",
                "Show Folding",
                Some("Display code folding controls in the gutter"),
                settings.editor.show_folding,
                |checked, _window, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.editor.show_folding = checked;
                    let _ = _window;
                },
                cx,
            ))
    }

    fn render_connections_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let settings = ZqlzSettings::global(cx);

        v_flex()
            .w_full()
            .child(self.render_section_header("Connections", cx))
            .child(self.render_toggle_row(
                "auto-commit",
                "Auto Commit",
                Some("Automatically commit after each query"),
                settings.connections.auto_commit,
                |checked, _, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.connections.auto_commit = checked;
                },
                cx,
            ))
            .child(self.render_toggle_row(
                "fetch-schema-on-connect",
                "Fetch Schema on Connect",
                Some("Automatically load database schema when connecting"),
                settings.connections.fetch_schema_on_connect,
                |checked, _, cx| {
                    let settings = ZqlzSettings::global_mut(cx);
                    settings.connections.fetch_schema_on_connect = checked;
                },
                cx,
            ))
    }
}

impl Render for SettingsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("settings-panel")
            .key_context("SettingsPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .overflow_y_scroll()
            .child(self.render_appearance_section(cx))
            .child(self.render_fonts_section(cx))
            .child(self.render_editor_section(cx))
            .child(self.render_connections_section(cx))
    }
}

impl Focusable for SettingsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for SettingsPanel {}
impl EventEmitter<SettingsPanelEvent> for SettingsPanel {}

impl Panel for SettingsPanel {
    fn panel_name(&self) -> &'static str {
        "SettingsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Settings"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
