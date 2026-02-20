//! Template Library Panel
//!
//! Provides a UI for browsing, searching, and managing SQL templates.
//! Supports both Plain SQL templates and DBT-style templates with ref(), source(), etc.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_templates::{DbtContext, DbtTemplateEngine};
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement,
    select::{Select, SelectItem, SelectState},
    typography::{body_small, caption, label, muted_small},
    v_flex, ActiveTheme, Icon, IndexPath, Sizable, ZqlzIcon,
};

use crate::storage::{LocalStorage, SavedTemplate, TemplateType};
use crate::AppState;
use zqlz_query::QueryEditor;

/// Events emitted by the template library panel
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum TemplateLibraryEvent {
    /// User selected a template to use
    UseTemplate {
        template_sql: String,
        default_params: String,
        template_type: TemplateType,
    },
    /// User wants to edit a template
    EditTemplate(Uuid),
    /// Template was deleted
    TemplateDeleted(Uuid),
    /// Template was saved
    TemplateSaved(Uuid),
}

/// SelectItem implementation for TemplateType dropdown
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct TemplateTypeOption {
    template_type: TemplateType,
    label: SharedString,
    description: SharedString,
}

#[allow(dead_code)]
impl TemplateTypeOption {
    fn plain_sql() -> Self {
        Self {
            template_type: TemplateType::PlainSql,
            label: "Plain SQL".into(),
            description: "Simple variable substitution with {{ variable }}".into(),
        }
    }

    fn dbt_model() -> Self {
        Self {
            template_type: TemplateType::DbtModel,
            label: "DBT Model".into(),
            description: "Full DBT support: ref(), source(), var(), config()".into(),
        }
    }
}

impl SelectItem for TemplateTypeOption {
    type Value = TemplateType;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.template_type
    }

    fn render(&self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        v_flex()
            .gap_0p5()
            .child(label(self.label.clone()).text_color(theme.foreground))
            .child(caption(self.description.clone()).text_color(theme.muted_foreground))
    }
}

/// State for the save/edit template modal
#[allow(dead_code)]
struct TemplateEditState {
    template_id: Option<Uuid>,
    name_input: Entity<InputState>,
    description_input: Entity<InputState>,
    tags_input: Entity<InputState>,
    template_sql_input: Entity<QueryEditor>,
    params_input: Entity<InputState>,
    template_type_select: Entity<SelectState<Vec<TemplateTypeOption>>>,
    /// Compiled SQL preview for DBT templates
    compiled_preview: Option<Result<String, String>>,
}

/// Template Library Panel for browsing and managing SQL templates
#[allow(dead_code)]
pub struct TemplateLibraryPanel {
    focus_handle: FocusHandle,
    /// All loaded templates
    templates: Vec<SavedTemplate>,
    /// Filtered templates based on search
    filtered_templates: Vec<SavedTemplate>,
    /// Search query
    search_query: String,
    /// Search input state
    search_input: Entity<InputState>,
    /// Selected template index
    selected_index: Option<usize>,
    /// Local storage reference
    storage: LocalStorage,
    /// Available tags for filtering
    tags: Vec<String>,
    /// Currently selected tag filter
    selected_tag: Option<String>,
    /// Show edit modal
    show_edit_modal: bool,
    /// Edit state for the modal
    edit_state: Option<TemplateEditState>,
    /// Subscriptions
    _subscriptions: Vec<Subscription>,
}

#[allow(dead_code)]
impl TemplateLibraryPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let storage = LocalStorage::default();
        let templates = storage.load_templates().unwrap_or_default();
        let tags = storage.get_template_tags().unwrap_or_default();

        let search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search templates...")
                .clean_on_escape()
        });

        let mut subscriptions = Vec::new();
        let search_input_weak = search_input.downgrade();
        subscriptions.push(
            cx.subscribe(&search_input, move |this, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    if let Some(input) = search_input_weak.upgrade() {
                        this.search_query = input.read(cx).value().to_string();
                        this.apply_filters();
                        cx.notify();
                    }
                }
            }),
        );

        let filtered_templates = templates.clone();

        Self {
            focus_handle: cx.focus_handle(),
            templates,
            filtered_templates,
            search_query: String::new(),
            search_input,
            selected_index: None,
            storage,
            tags,
            selected_tag: None,
            show_edit_modal: false,
            edit_state: None,
            _subscriptions: subscriptions,
        }
    }

    /// Refresh templates from storage
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.templates = self.storage.load_templates().unwrap_or_default();
        self.tags = self.storage.get_template_tags().unwrap_or_default();
        self.apply_filters();
        cx.notify();
    }

    /// Apply search and tag filters
    fn apply_filters(&mut self) {
        self.filtered_templates = self
            .templates
            .iter()
            .filter(|t| {
                // Search filter
                let matches_search = self.search_query.is_empty()
                    || t.name
                        .to_lowercase()
                        .contains(&self.search_query.to_lowercase())
                    || t.description
                        .to_lowercase()
                        .contains(&self.search_query.to_lowercase())
                    || t.tags
                        .to_lowercase()
                        .contains(&self.search_query.to_lowercase());

                // Tag filter
                let matches_tag = self.selected_tag.is_none()
                    || t.tags
                        .split(',')
                        .map(|s| s.trim())
                        .any(|tag| Some(tag) == self.selected_tag.as_deref());

                matches_search && matches_tag
            })
            .cloned()
            .collect();

        // Reset selection if out of bounds
        if let Some(idx) = self.selected_index {
            if idx >= self.filtered_templates.len() {
                self.selected_index = if self.filtered_templates.is_empty() {
                    None
                } else {
                    Some(0)
                };
            }
        }
    }

    /// Open the new template modal
    pub fn open_new_template_modal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_edit_modal(None, window, cx);
    }

    /// Open the edit modal for a template
    fn open_edit_modal(
        &mut self,
        template: Option<&SavedTemplate>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Template name"));
        let description_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Description (optional)"));
        let tags_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Tags (comma-separated)"));

        // Create QueryEditor for SQL template input
        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .unwrap_or_else(|| Arc::new(zqlz_services::SchemaService::new()));

        let template_sql_input = cx.new(|cx| {
            QueryEditor::new(
                "Template SQL Editor".to_string(),
                None,
                schema_service.clone(),
                window,
                cx,
            )
        });

        let params_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Default parameters (JSON)")
                .rows(4)
        });

        // Create template type selector
        let template_type_options = vec![
            TemplateTypeOption::plain_sql(),
            TemplateTypeOption::dbt_model(),
        ];
        let initial_type = template
            .map(|t| &t.template_type)
            .unwrap_or(&TemplateType::PlainSql);
        let selected_index = match initial_type {
            TemplateType::PlainSql => Some(IndexPath::default().row(0)),
            TemplateType::DbtModel => Some(IndexPath::default().row(1)),
        };
        let template_type_select =
            cx.new(|cx| SelectState::new(template_type_options, selected_index, window, cx));

        // Set values if editing an existing template
        if let Some(t) = template {
            name_input.update(cx, |input, cx| {
                input.set_value(&t.name, window, cx);
            });
            description_input.update(cx, |input, cx| {
                input.set_value(&t.description, window, cx);
            });
            tags_input.update(cx, |input, cx| {
                input.set_value(&t.tags, window, cx);
            });
            template_sql_input.update(cx, |editor, cx| {
                editor.set_content(t.template_sql.clone(), window, cx);
            });
            params_input.update(cx, |input, cx| {
                input.set_value(&t.default_params, window, cx);
            });
        } else {
            // Set default value for params
            params_input.update(cx, |input, cx| {
                input.set_value("{}", window, cx);
            });
        }

        self.edit_state = Some(TemplateEditState {
            template_id: template.map(|t| t.id),
            name_input,
            description_input,
            tags_input,
            template_sql_input,
            params_input,
            template_type_select,
            compiled_preview: None,
        });

        self.show_edit_modal = true;
        cx.notify();
    }

    /// Save the template from the edit modal
    fn save_template_from_modal(&mut self, cx: &mut Context<Self>) {
        let Some(edit_state) = &self.edit_state else {
            return;
        };

        let name = edit_state.name_input.read(cx).value().to_string();
        let description = edit_state.description_input.read(cx).value().to_string();
        let tags = edit_state.tags_input.read(cx).value().to_string();
        let template_sql = edit_state
            .template_sql_input
            .read(cx)
            .content(cx)
            .to_string();
        let default_params = edit_state.params_input.read(cx).value().to_string();

        // Get selected template type
        let template_type = edit_state
            .template_type_select
            .read(cx)
            .selected_value()
            .cloned()
            .unwrap_or(TemplateType::PlainSql);

        if name.trim().is_empty() || template_sql.trim().is_empty() {
            return;
        }

        let template = if let Some(id) = edit_state.template_id {
            // Update existing template
            let mut template =
                SavedTemplate::new(name, description, template_sql, default_params, tags);
            template.id = id;
            template.template_type = template_type;
            if let Err(e) = self.storage.update_template(&template) {
                tracing::error!("Failed to update template: {}", e);
                return;
            }
            template
        } else {
            // Create new template based on type
            let template = match template_type {
                TemplateType::PlainSql => {
                    SavedTemplate::new(name, description, template_sql, default_params, tags)
                }
                TemplateType::DbtModel => {
                    SavedTemplate::new_dbt(name, description, template_sql, default_params, tags)
                }
            };
            if let Err(e) = self.storage.save_template(&template) {
                tracing::error!("Failed to save template: {}", e);
                return;
            }
            template
        };

        cx.emit(TemplateLibraryEvent::TemplateSaved(template.id));

        self.show_edit_modal = false;
        self.edit_state = None;
        self.refresh(cx);
    }

    /// Close the edit modal
    fn close_edit_modal(&mut self, cx: &mut Context<Self>) {
        self.show_edit_modal = false;
        self.edit_state = None;
        cx.notify();
    }

    /// Delete a template
    fn delete_template(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Err(e) = self.storage.delete_template(id) {
            tracing::error!("Failed to delete template: {}", e);
            return;
        }
        cx.emit(TemplateLibraryEvent::TemplateDeleted(id));
        self.refresh(cx);
    }

    /// Use a template (emit event to insert into editor)
    fn use_template(&mut self, template: &SavedTemplate, cx: &mut Context<Self>) {
        cx.emit(TemplateLibraryEvent::UseTemplate {
            template_sql: template.template_sql.clone(),
            default_params: template.default_params.clone(),
            template_type: template.template_type.clone(),
        });
    }

    /// Render a template item
    fn render_template_item(
        &self,
        template: &SavedTemplate,
        index: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_selected = self.selected_index == Some(index);
        let template_for_use = template.clone();
        let is_dbt = template.is_dbt();

        v_flex()
            .id(SharedString::from(format!("template-{}", template.id)))
            .w_full()
            .p_2()
            .gap_1()
            .rounded_md()
            .cursor_pointer()
            .when(is_selected, |this| this.bg(theme.list_active))
            .hover(|this| this.bg(theme.list_hover))
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.selected_index = Some(index);
                cx.notify();
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, event: &MouseDownEvent, _window, cx| {
                    // Double-click to use template
                    if event.click_count == 2 {
                        this.use_template(&template_for_use, cx);
                    }
                }),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(Icon::new(ZqlzIcon::Code).size_4().text_color(theme.accent))
                    .child(
                        label(template.name.clone())
                            .weight(FontWeight::MEDIUM)
                            .text_color(theme.foreground),
                    )
                    // Template type badge
                    .when(is_dbt, |this| {
                        this.child(
                            div()
                                .px_1p5()
                                .py_0p5()
                                .rounded(px(4.0))
                                .bg(theme.accent.opacity(0.2))
                                .child(
                                    caption("DBT")
                                        .text_color(theme.accent)
                                        .weight(FontWeight::MEDIUM),
                                ),
                        )
                    }),
            )
            .when(!template.description.is_empty(), |this| {
                this.child(
                    body_small(template.description.clone())
                        .text_color(theme.muted_foreground)
                        .max_w(px(280.0))
                        .text_ellipsis(),
                )
            })
            .when(!template.tags.is_empty(), |this| {
                this.child(
                    h_flex()
                        .gap_1()
                        .flex_wrap()
                        .children(template.tags.split(',').map(|tag| {
                            let tag = tag.trim();
                            div()
                                .px_1p5()
                                .py_0p5()
                                .rounded(px(4.0))
                                .bg(theme.muted)
                                .child(caption(tag.to_string()).text_color(theme.muted_foreground))
                        })),
                )
            })
    }

    /// Compile DBT template and show preview
    fn compile_dbt_preview(&mut self, cx: &mut Context<Self>) {
        let Some(edit_state) = &mut self.edit_state else {
            return;
        };

        let template_sql = edit_state
            .template_sql_input
            .read(cx)
            .content(cx)
            .to_string();
        let params_json = edit_state.params_input.read(cx).value().to_string();

        // Parse params JSON to extract vars
        let vars: serde_json::Value =
            serde_json::from_str(&params_json).unwrap_or(serde_json::json!({}));

        // Create a basic DBT context
        let mut dbt_ctx = DbtContext::new().with_schema("public");

        // Add any vars from params
        if let serde_json::Value::Object(map) = vars {
            for (key, value) in map {
                dbt_ctx.add_var(&key, value);
            }
        }

        let engine = DbtTemplateEngine::new(dbt_ctx);
        let result = engine.render(&template_sql);

        edit_state.compiled_preview = Some(match result {
            Ok(sql) => Ok(sql),
            Err(e) => Err(e.to_string()),
        });

        cx.notify();
    }

    /// Render the edit modal
    fn render_edit_modal(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();

        let Some(edit_state) = &self.edit_state else {
            return div().into_any_element();
        };

        let is_new = edit_state.template_id.is_none();
        let title = if is_new {
            "New Template"
        } else {
            "Edit Template"
        };

        // Check if DBT is selected for compile button visibility
        let is_dbt = edit_state
            .template_type_select
            .read(cx)
            .selected_value()
            .map(|t| matches!(t, TemplateType::DbtModel))
            .unwrap_or(false);

        // Get compiled preview if available
        let compiled_preview = edit_state.compiled_preview.clone();

        div()
            .id("template-edit-modal-backdrop")
            .absolute()
            .inset_0()
            .bg(rgba(0x00000080))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _window, cx| {
                    this.close_edit_modal(cx);
                }),
            )
            .child(
                v_flex()
                    .id("template-edit-modal")
                    .w(px(600.0))
                    .max_h(px(700.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded_lg()
                    .shadow_xl()
                    .p_4()
                    .gap_4()
                    .on_mouse_down(MouseButton::Left, |_event, _window, _cx| {
                        // Prevent backdrop click from closing
                    })
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(label(title).weight(FontWeight::SEMIBOLD).text_lg())
                            .child(
                                Button::new("close-modal")
                                    .icon(ZqlzIcon::X)
                                    .ghost()
                                    .small()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_edit_modal(cx);
                                    })),
                            ),
                    )
                    .child(
                        div()
                            .id("template-edit-modal-content")
                            .flex_1()
                            .overflow_y_scrollbar()
                            .child(
                                v_flex()
                                    .gap_3()
                                    // Name input
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                caption("Name").text_color(theme.muted_foreground),
                                            )
                                            .child(Input::new(&edit_state.name_input).w_full()),
                                    )
                                    // Template Type selector
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                caption("Template Type")
                                                    .text_color(theme.muted_foreground),
                                            )
                                            .child(
                                                Select::new(&edit_state.template_type_select)
                                                    .w_full()
                                                    .placeholder("Select template type"),
                                            ),
                                    )
                                    // Description input
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                caption("Description")
                                                    .text_color(theme.muted_foreground),
                                            )
                                            .child(
                                                Input::new(&edit_state.description_input).w_full(),
                                            ),
                                    )
                                    // Tags input
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                caption("Tags").text_color(theme.muted_foreground),
                                            )
                                            .child(Input::new(&edit_state.tags_input).w_full()),
                                    )
                                    // Template SQL input with compile button for DBT
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                h_flex()
                                                    .justify_between()
                                                    .items_center()
                                                    .child(
                                                        caption("Template SQL")
                                                            .text_color(theme.muted_foreground),
                                                    )
                                                    .when(is_dbt, |this| {
                                                        this.child(
                                                            Button::new("compile-preview")
                                                                .label("Compile Preview")
                                                                .ghost()
                                                                .xsmall()
                                                                .on_click(cx.listener(
                                                                    |this, _, _window, cx| {
                                                                        this.compile_dbt_preview(
                                                                            cx,
                                                                        );
                                                                    },
                                                                )),
                                                        )
                                                    }),
                                            )
                                            .child(edit_state.template_sql_input.clone()),
                                    )
                                    // Compiled preview (for DBT templates)
                                    .when_some(compiled_preview, |this, preview| {
                                        this.child(
                                            v_flex()
                                                .gap_1()
                                                .child(
                                                    caption("Compiled SQL Preview")
                                                        .text_color(theme.muted_foreground),
                                                )
                                                .child(
                                                    div()
                                                        .id("compiled-preview")
                                                        .w_full()
                                                        .max_h(px(150.0))
                                                        .overflow_y_scrollbar()
                                                        .p_2()
                                                        .rounded_md()
                                                        .bg(theme.muted)
                                                        .border_1()
                                                        .border_color(theme.border)
                                                        .child(match preview {
                                                            Ok(sql) => caption(sql)
                                                                .text_color(theme.foreground)
                                                                .into_any_element(),
                                                            Err(err) => {
                                                                caption(format!("Error: {}", err))
                                                                    .text_color(theme.danger)
                                                                    .into_any_element()
                                                            }
                                                        }),
                                                ),
                                        )
                                    })
                                    // Default Parameters input
                                    .child(
                                        v_flex()
                                            .gap_1()
                                            .child(
                                                h_flex()
                                                    .gap_1()
                                                    .child(
                                                        caption("Default Parameters (JSON)")
                                                            .text_color(theme.muted_foreground),
                                                    )
                                                    .when(is_dbt, |this| {
                                                        this.child(
                                                            caption("(used as var() values)")
                                                                .text_color(
                                                                    theme
                                                                        .muted_foreground
                                                                        .opacity(0.7),
                                                                ),
                                                        )
                                                    }),
                                            )
                                            .child(
                                                Input::new(&edit_state.params_input)
                                                    .w_full()
                                                    .appearance(false),
                                            ),
                                    ),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .justify_end()
                            .child(Button::new("cancel").label("Cancel").ghost().on_click(
                                cx.listener(|this, _, _window, cx| {
                                    this.close_edit_modal(cx);
                                }),
                            ))
                            .child(
                                Button::new("save")
                                    .label(if is_new { "Create" } else { "Save" })
                                    .primary()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.save_template_from_modal(cx);
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    /// Render tag filter chips
    fn render_tag_filters(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();

        h_flex()
            .gap_1()
            .flex_wrap()
            .child(
                div()
                    .id("tag-all")
                    .px_2()
                    .py_1()
                    .rounded(px(4.0))
                    .cursor_pointer()
                    .when(self.selected_tag.is_none(), |this| {
                        this.bg(theme.accent).text_color(gpui::white())
                    })
                    .when(self.selected_tag.is_some(), |this| {
                        this.bg(theme.muted).hover(|this| this.bg(theme.list_hover))
                    })
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.selected_tag = None;
                        this.apply_filters();
                        cx.notify();
                    }))
                    .child(caption("All")),
            )
            .children(self.tags.iter().map(|tag| {
                let tag_clone = tag.clone();
                let is_selected = self.selected_tag.as_ref() == Some(tag);

                div()
                    .id(SharedString::from(format!("tag-{}", tag)))
                    .px_2()
                    .py_1()
                    .rounded(px(4.0))
                    .cursor_pointer()
                    .when(is_selected, |this| {
                        this.bg(theme.accent).text_color(gpui::white())
                    })
                    .when(!is_selected, |this| {
                        this.bg(theme.muted).hover(|this| this.bg(theme.list_hover))
                    })
                    .on_click(cx.listener(move |this, _, _window, cx| {
                        this.selected_tag = Some(tag_clone.clone());
                        this.apply_filters();
                        cx.notify();
                    }))
                    .child(caption(tag.clone()))
            }))
    }
}

impl Render for TemplateLibraryPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Pre-render template items to avoid borrowing issues (before borrowing theme)
        let mut template_items = Vec::new();
        for (index, template) in self.filtered_templates.iter().enumerate() {
            template_items.push(self.render_template_item(template, index, cx));
        }

        // Pre-render tag filters
        let tag_filters = if !self.tags.is_empty() {
            Some(self.render_tag_filters(cx))
        } else {
            None
        };

        // Pre-render edit modal
        let edit_modal = if self.show_edit_modal {
            Some(self.render_edit_modal(cx))
        } else {
            None
        };

        // Get data needed for bottom action bar
        let selected_template = self
            .selected_index
            .and_then(|idx| self.filtered_templates.get(idx).cloned());

        // Now get theme (after all mutable borrows of cx)
        let theme = cx.theme();

        v_flex()
            .id("template-library-panel")
            .key_context("TemplateLibraryPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(
                // Header with search and new button
                h_flex()
                    .p_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Input::new(&self.search_input).w_full())
                    .child(
                        Button::new("new-template")
                            .icon(ZqlzIcon::Plus)
                            .primary()
                            .small()
                            .tooltip("New Template")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_new_template_modal(window, cx);
                            })),
                    ),
            )
            // Tag filters
            .when_some(tag_filters, |this, filters| {
                this.child(
                    div()
                        .p_2()
                        .border_b_1()
                        .border_color(theme.border)
                        .child(filters),
                )
            })
            // Template list
            .child(
                div().flex_1().overflow_y_scrollbar().p_2().child(
                    v_flex()
                        .gap_1()
                        .when(self.filtered_templates.is_empty(), |this| {
                            this.child(
                                v_flex()
                                    .items_center()
                                    .justify_center()
                                    .py_8()
                                    .gap_2()
                                    .child(
                                        Icon::new(ZqlzIcon::Code)
                                            .size(px(32.0))
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child(muted_small(
                                        if self.templates.is_empty() {
                                            "No templates yet"
                                        } else {
                                            "No matching templates"
                                        },
                                        cx,
                                    ))
                                    .when(self.templates.is_empty(), |this| {
                                        this.child(
                                            Button::new("create-first")
                                                .label("Create your first template")
                                                .ghost()
                                                .small()
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.open_new_template_modal(window, cx);
                                                })),
                                        )
                                    }),
                            )
                        })
                        .children(template_items),
                ),
            )
            // Selected template actions
            .when_some(selected_template, |this, template| {
                let template_for_use = template.clone();
                let template_for_edit = template.clone();
                let template_id = template.id;

                this.child(
                    h_flex()
                        .p_2()
                        .gap_2()
                        .border_t_1()
                        .border_color(theme.border)
                        .child(
                            Button::new("use-template")
                                .label("Use Template")
                                .primary()
                                .small()
                                .on_click(cx.listener(move |this, _, _window, cx| {
                                    this.use_template(&template_for_use, cx);
                                })),
                        )
                        .child(
                            Button::new("edit-template")
                                .icon(ZqlzIcon::Pencil)
                                .ghost()
                                .small()
                                .tooltip("Edit Template")
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.open_edit_modal(Some(&template_for_edit), window, cx);
                                })),
                        )
                        .child(
                            Button::new("delete-template")
                                .icon(ZqlzIcon::Trash)
                                .ghost()
                                .small()
                                .tooltip("Delete Template")
                                .on_click(cx.listener(move |this, _, _window, cx| {
                                    this.delete_template(template_id, cx);
                                })),
                        ),
                )
            })
            // Edit modal overlay
            .when_some(edit_modal, |this, modal| this.child(modal))
    }
}

impl Focusable for TemplateLibraryPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for TemplateLibraryPanel {}
impl EventEmitter<TemplateLibraryEvent> for TemplateLibraryPanel {}

impl Panel for TemplateLibraryPanel {
    fn panel_name(&self) -> &'static str {
        "TemplateLibraryPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Templates"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
