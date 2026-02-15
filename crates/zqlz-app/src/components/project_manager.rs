//! Project Manager Panel for DBT-style template projects
//!
//! This component provides a UI for managing DBT-style projects,
//! including creating, editing, and organizing models and sources.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

use zqlz_templates::dbt::QuotingConfig;
use zqlz_templates::project::{Model, Project, SourceDefinition, SourceTable};
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement,
    typography::{body_small, caption, label, muted_small},
    v_flex, ActiveTheme, Icon, Sizable, ZqlzIcon,
};

use crate::storage::LocalStorage;
use crate::AppState;
use zqlz_query::QueryEditor;

/// Events emitted by the Project Manager Panel
#[derive(Clone, Debug)]
pub enum ProjectManagerEvent {
    /// A project was selected for editing
    ProjectSelected(Uuid),
    /// A model was selected to open in the editor
    OpenModel { project_id: Uuid, model_id: Uuid },
    /// A new model should be created in the specified project
    CreateModel(Uuid),
    /// Request to compile a model
    CompileModel { project_id: Uuid, model_id: Uuid },
    /// Project list changed (created, deleted, updated)
    ProjectsChanged,
}

/// State for the project edit modal
struct ProjectEditState {
    project_id: Option<Uuid>,
    name_input: Entity<InputState>,
    description_input: Entity<InputState>,
    schema_input: Entity<InputState>,
}

/// State for the model edit modal
struct ModelEditState {
    project_id: Uuid,
    model_id: Option<Uuid>,
    name_input: Entity<InputState>,
    description_input: Entity<InputState>,
    sql_input: Entity<QueryEditor>,
}

/// State for the source edit modal
struct SourceEditState {
    project_id: Uuid,
    source_id: Option<Uuid>,
    name_input: Entity<InputState>,
    schema_input: Entity<InputState>,
    tables_input: Entity<InputState>,
}

/// The Project Manager Panel
pub struct ProjectManagerPanel {
    focus_handle: FocusHandle,
    projects: Vec<Project>,
    models_by_project: std::collections::HashMap<Uuid, Vec<Model>>,
    filtered_projects: Vec<Project>,
    search_input: Entity<InputState>,
    selected_project: Option<Uuid>,
    expanded_projects: std::collections::HashSet<Uuid>,
    show_project_modal: bool,
    show_model_modal: bool,
    show_source_modal: bool,
    project_edit_state: Option<ProjectEditState>,
    model_edit_state: Option<ModelEditState>,
    source_edit_state: Option<SourceEditState>,
    storage: LocalStorage,
    _subscriptions: Vec<Subscription>,
}

impl ProjectManagerPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search projects...")
                .clean_on_escape()
        });

        let mut subscriptions = Vec::new();
        subscriptions.push(
            cx.subscribe(&search_input, |this, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    this.apply_filters(cx);
                }
            }),
        );

        let mut panel = Self {
            focus_handle: cx.focus_handle(),
            projects: Vec::new(),
            models_by_project: std::collections::HashMap::new(),
            filtered_projects: Vec::new(),
            search_input,
            selected_project: None,
            expanded_projects: std::collections::HashSet::new(),
            show_project_modal: false,
            show_model_modal: false,
            show_source_modal: false,
            project_edit_state: None,
            model_edit_state: None,
            source_edit_state: None,
            storage: LocalStorage::default(),
            _subscriptions: subscriptions,
        };

        panel.refresh(cx);
        panel
    }

    /// Refresh projects from storage
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.projects = self.storage.load_projects().unwrap_or_default();

        // Load models for each project
        self.models_by_project.clear();
        for project in &self.projects {
            if let Ok(models) = self.storage.load_models_for_project(project.id) {
                self.models_by_project.insert(project.id, models);
            }
        }

        self.apply_filters(cx);
        cx.notify();
    }

    fn apply_filters(&mut self, cx: &mut Context<Self>) {
        let search_text = self
            .search_input
            .read(cx)
            .value()
            .to_string()
            .to_lowercase();

        if search_text.is_empty() {
            self.filtered_projects = self.projects.clone();
        } else {
            self.filtered_projects = self
                .projects
                .iter()
                .filter(|p| {
                    p.name.to_lowercase().contains(&search_text)
                        || p.description.to_lowercase().contains(&search_text)
                })
                .cloned()
                .collect();
        }
        cx.notify();
    }

    fn toggle_project_expanded(&mut self, project_id: Uuid, cx: &mut Context<Self>) {
        if self.expanded_projects.contains(&project_id) {
            self.expanded_projects.remove(&project_id);
        } else {
            self.expanded_projects.insert(project_id);
        }
        cx.notify();
    }

    fn open_new_project_modal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_project_modal(None, window, cx);
    }

    fn open_project_modal(
        &mut self,
        project: Option<&Project>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Project name"));
        let description_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Description (optional)"));
        let schema_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Default schema (e.g., public)"));

        if let Some(p) = project {
            name_input.update(cx, |input, cx| {
                input.set_value(&p.name, window, cx);
            });
            description_input.update(cx, |input, cx| {
                input.set_value(&p.description, window, cx);
            });
            schema_input.update(cx, |input, cx| {
                input.set_value(&p.default_schema, window, cx);
            });
        } else {
            schema_input.update(cx, |input, cx| {
                input.set_value("public", window, cx);
            });
        }

        self.project_edit_state = Some(ProjectEditState {
            project_id: project.map(|p| p.id),
            name_input,
            description_input,
            schema_input,
        });
        self.show_project_modal = true;
        cx.notify();
    }

    fn close_project_modal(&mut self, cx: &mut Context<Self>) {
        self.show_project_modal = false;
        self.project_edit_state = None;
        cx.notify();
    }

    fn save_project_from_modal(&mut self, cx: &mut Context<Self>) {
        let Some(edit_state) = &self.project_edit_state else {
            return;
        };

        let name = edit_state.name_input.read(cx).value().to_string();
        let description = edit_state.description_input.read(cx).value().to_string();
        let schema = edit_state.schema_input.read(cx).value().to_string();

        if name.trim().is_empty() {
            return;
        }

        let schema = if schema.trim().is_empty() {
            "public".to_string()
        } else {
            schema
        };

        if let Some(project_id) = edit_state.project_id {
            // Update existing project
            if let Ok(Some(mut project)) = self.storage.load_project(project_id) {
                project.name = name;
                project.description = description;
                project.default_schema = schema;
                if let Err(e) = self.storage.update_project(&project) {
                    tracing::error!("Failed to update project: {}", e);
                }
            }
        } else {
            // Create new project
            let project = Project {
                id: Uuid::new_v4(),
                name,
                description,
                connection_id: None,
                default_schema: schema,
                default_database: None,
                quoting: QuotingConfig::all_quoted(),
                vars: std::collections::HashMap::new(),
                sources: Vec::new(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            if let Err(e) = self.storage.save_project(&project) {
                tracing::error!("Failed to save project: {}", e);
            }
        }

        cx.emit(ProjectManagerEvent::ProjectsChanged);
        self.close_project_modal(cx);
        self.refresh(cx);
    }

    fn delete_project(&mut self, project_id: Uuid, cx: &mut Context<Self>) {
        if let Err(e) = self.storage.delete_project(project_id) {
            tracing::error!("Failed to delete project: {}", e);
            return;
        }
        cx.emit(ProjectManagerEvent::ProjectsChanged);
        self.refresh(cx);
    }

    fn open_new_model_modal(
        &mut self,
        project_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_model_modal(project_id, None, window, cx);
    }

    fn open_model_modal(
        &mut self,
        project_id: Uuid,
        model: Option<&Model>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Model name"));
        let description_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Description (optional)"));

        // Create QueryEditor for SQL model input
        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .unwrap_or_else(|| Arc::new(zqlz_services::SchemaService::new()));

        let sql_input = cx.new(|cx| {
            QueryEditor::new(
                "Model SQL Editor".to_string(),
                None,
                schema_service.clone(),
                window,
                cx,
            )
        });

        if let Some(m) = model {
            name_input.update(cx, |input, cx| {
                input.set_value(&m.name, window, cx);
            });
            description_input.update(cx, |input, cx| {
                input.set_value(&m.description, window, cx);
            });
            sql_input.update(cx, |editor, cx| {
                editor.set_content(m.sql.clone(), window, cx);
            });
        }

        self.model_edit_state = Some(ModelEditState {
            project_id,
            model_id: model.map(|m| m.id),
            name_input,
            description_input,
            sql_input,
        });
        self.show_model_modal = true;
        cx.notify();
    }

    fn close_model_modal(&mut self, cx: &mut Context<Self>) {
        self.show_model_modal = false;
        self.model_edit_state = None;
        cx.notify();
    }

    fn save_model_from_modal(&mut self, cx: &mut Context<Self>) {
        let Some(edit_state) = &self.model_edit_state else {
            return;
        };

        let name = edit_state.name_input.read(cx).value().to_string();
        let description = edit_state.description_input.read(cx).value().to_string();
        let sql = edit_state.sql_input.read(cx).content(cx).to_string();
        let project_id = edit_state.project_id;

        if name.trim().is_empty() {
            return;
        }

        if let Some(model_id) = edit_state.model_id {
            // Update existing model
            if let Ok(Some(mut model)) = self.storage.load_model(model_id) {
                model.name = name;
                model.description = description;
                model.sql = sql;
                model.updated_at = chrono::Utc::now();
                if let Err(e) = self.storage.update_model(&model) {
                    tracing::error!("Failed to update model: {}", e);
                }
            }
        } else {
            // Create new model
            let model = Model::new(project_id, name, sql).with_description(description);
            if let Err(e) = self.storage.save_model(&model) {
                tracing::error!("Failed to save model: {}", e);
            }
        }

        cx.emit(ProjectManagerEvent::ProjectsChanged);
        self.close_model_modal(cx);
        self.refresh(cx);
    }

    fn delete_model(&mut self, model_id: Uuid, cx: &mut Context<Self>) {
        if let Err(e) = self.storage.delete_model(model_id) {
            tracing::error!("Failed to delete model: {}", e);
            return;
        }
        cx.emit(ProjectManagerEvent::ProjectsChanged);
        self.refresh(cx);
    }

    fn open_new_source_modal(
        &mut self,
        project_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Source name (e.g., raw_data)"));
        let schema_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Schema name (e.g., raw)"));
        let tables_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Table names (comma-separated)"));

        self.source_edit_state = Some(SourceEditState {
            project_id,
            source_id: None,
            name_input,
            schema_input,
            tables_input,
        });
        self.show_source_modal = true;
        cx.notify();
    }

    fn close_source_modal(&mut self, cx: &mut Context<Self>) {
        self.show_source_modal = false;
        self.source_edit_state = None;
        cx.notify();
    }

    fn save_source_from_modal(&mut self, cx: &mut Context<Self>) {
        let Some(edit_state) = &self.source_edit_state else {
            return;
        };

        let name = edit_state.name_input.read(cx).value().to_string();
        let schema = edit_state.schema_input.read(cx).value().to_string();
        let tables_str = edit_state.tables_input.read(cx).value().to_string();
        let project_id = edit_state.project_id;

        if name.trim().is_empty() || schema.trim().is_empty() {
            return;
        }

        // Parse table names
        let tables: Vec<SourceTable> = tables_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| SourceTable::new(s))
            .collect();

        let source = SourceDefinition {
            id: Uuid::new_v4(),
            name,
            description: String::new(),
            database: None,
            schema,
            tables,
        };

        if let Err(e) = self.storage.add_source_to_project(project_id, &source) {
            tracing::error!("Failed to save source: {}", e);
            return;
        }

        cx.emit(ProjectManagerEvent::ProjectsChanged);
        self.close_source_modal(cx);
        self.refresh(cx);
    }

    fn render_project_item(
        &self,
        project: &Project,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_expanded = self.expanded_projects.contains(&project.id);
        let is_selected = self.selected_project == Some(project.id);
        let project_id = project.id;
        let project_name = project.name.clone();
        let project_desc = project.description.clone();
        let models = self
            .models_by_project
            .get(&project.id)
            .cloned()
            .unwrap_or_default();
        let sources = project.sources.clone();
        let model_count = models.len();
        let source_count = sources.len();

        let expand_icon = if is_expanded {
            ZqlzIcon::CaretDown
        } else {
            ZqlzIcon::CaretRight
        };

        v_flex()
            .w_full()
            .child(
                h_flex()
                    .id(SharedString::from(format!("project-{}", project.id)))
                    .w_full()
                    .px_2()
                    .py_1()
                    .gap_1()
                    .items_center()
                    .rounded_md()
                    .cursor_pointer()
                    .when(is_selected, |this| this.bg(theme.list_active))
                    .hover(|this| this.bg(theme.list_hover))
                    .on_click(cx.listener(move |this, _, _window, cx| {
                        this.selected_project = Some(project_id);
                        this.toggle_project_expanded(project_id, cx);
                    }))
                    .child(
                        Icon::new(expand_icon)
                            .size_3()
                            .text_color(theme.muted_foreground),
                    )
                    .child(
                        Icon::new(ZqlzIcon::Folder)
                            .size_4()
                            .text_color(theme.accent),
                    )
                    .child(
                        h_flex()
                            .flex_1()
                            .gap_2()
                            .items_center()
                            .child(label(project_name).text_color(theme.foreground))
                            .child(
                                muted_small(format!("{} models", model_count), cx)
                                    .text_color(theme.muted_foreground),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new(SharedString::from(format!(
                                    "add-model-{}",
                                    project_id
                                )))
                                .icon(ZqlzIcon::Plus)
                                .ghost()
                                .xsmall()
                                .tooltip("Add Model")
                                .on_click(cx.listener(
                                    move |this, _, window, cx| {
                                        this.open_new_model_modal(project_id, window, cx);
                                    },
                                )),
                            )
                            .child(
                                Button::new(SharedString::from(format!(
                                    "edit-project-{}",
                                    project_id
                                )))
                                .icon(ZqlzIcon::Pencil)
                                .ghost()
                                .xsmall()
                                .tooltip("Edit Project")
                                .on_click(cx.listener(
                                    move |this, _, window, cx| {
                                        if let Some(p) =
                                            this.projects.iter().find(|p| p.id == project_id)
                                        {
                                            let p_clone = p.clone();
                                            this.open_project_modal(Some(&p_clone), window, cx);
                                        }
                                    },
                                )),
                            )
                            .child(
                                Button::new(SharedString::from(format!(
                                    "delete-project-{}",
                                    project_id
                                )))
                                .icon(ZqlzIcon::Trash)
                                .ghost()
                                .xsmall()
                                .tooltip("Delete Project")
                                .on_click(cx.listener(
                                    move |this, _, _window, cx| {
                                        this.delete_project(project_id, cx);
                                    },
                                )),
                            ),
                    ),
            )
            .when(!project_desc.is_empty() && is_expanded, |this| {
                this.child(
                    div()
                        .pl(px(28.0))
                        .pr_2()
                        .child(body_small(project_desc).text_color(theme.muted_foreground)),
                )
            })
            .when(is_expanded, |this| {
                this.child(self.render_project_children(project_id, &models, &sources, cx))
            })
    }

    fn render_project_children(
        &self,
        project_id: Uuid,
        models: &[Model],
        sources: &[SourceDefinition],
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();

        v_flex()
            .pl(px(20.0))
            .gap_px()
            // Models section
            .child(
                v_flex()
                    .child(
                        h_flex()
                            .px_2()
                            .py_1()
                            .gap_2()
                            .items_center()
                            .child(
                                Icon::new(ZqlzIcon::Code)
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(
                                caption("Models")
                                    .text_color(theme.muted_foreground)
                                    .text_xs(),
                            ),
                    )
                    .children(models.iter().map(|model| {
                        let model_id = model.id;
                        let model_name = model.name.clone();

                        h_flex()
                            .id(SharedString::from(format!("model-{}", model.id)))
                            .w_full()
                            .px_2()
                            .py_1()
                            .pl(px(20.0))
                            .gap_2()
                            .items_center()
                            .rounded_md()
                            .cursor_pointer()
                            .hover(|this| this.bg(theme.list_hover))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, event: &MouseDownEvent, _window, cx| {
                                    if event.click_count == 2 {
                                        cx.emit(ProjectManagerEvent::OpenModel {
                                            project_id,
                                            model_id,
                                        });
                                    }
                                }),
                            )
                            .child(
                                Icon::new(ZqlzIcon::FileSql)
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(label(model_name).text_sm().text_color(theme.foreground))
                            .child(div().flex_1())
                            .child(
                                Button::new(SharedString::from(format!(
                                    "delete-model-{}",
                                    model_id
                                )))
                                .icon(ZqlzIcon::Trash)
                                .ghost()
                                .xsmall()
                                .on_click(cx.listener(
                                    move |this, _, _window, cx| {
                                        this.delete_model(model_id, cx);
                                    },
                                )),
                            )
                    }))
                    .when(models.is_empty(), |this| {
                        this.child(
                            div().px_2().py_1().pl(px(20.0)).child(
                                muted_small("No models yet", cx)
                                    .text_color(theme.muted_foreground)
                                    .text_xs(),
                            ),
                        )
                    }),
            )
            // Sources section
            .child(
                v_flex()
                    .mt_1()
                    .child(
                        h_flex()
                            .px_2()
                            .py_1()
                            .gap_2()
                            .items_center()
                            .child(
                                Icon::new(ZqlzIcon::Database)
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(
                                caption("Sources")
                                    .text_color(theme.muted_foreground)
                                    .text_xs(),
                            )
                            .child(div().flex_1())
                            .child(
                                Button::new(SharedString::from(format!(
                                    "add-source-{}",
                                    project_id
                                )))
                                .icon(ZqlzIcon::Plus)
                                .ghost()
                                .xsmall()
                                .tooltip("Add Source")
                                .on_click(cx.listener(
                                    move |this, _, window, cx| {
                                        this.open_new_source_modal(project_id, window, cx);
                                    },
                                )),
                            ),
                    )
                    .children(sources.iter().map(|source| {
                        let source_name = source.name.clone();
                        let table_count = source.tables.len();

                        h_flex()
                            .w_full()
                            .px_2()
                            .py_1()
                            .pl(px(20.0))
                            .gap_2()
                            .items_center()
                            .child(
                                Icon::new(ZqlzIcon::Table)
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(label(source_name).text_sm().text_color(theme.foreground))
                            .child(
                                muted_small(format!("{} tables", table_count), cx)
                                    .text_color(theme.muted_foreground)
                                    .text_xs(),
                            )
                    }))
                    .when(sources.is_empty(), |this| {
                        this.child(
                            div().px_2().py_1().pl(px(20.0)).child(
                                muted_small("No sources yet", cx)
                                    .text_color(theme.muted_foreground)
                                    .text_xs(),
                            ),
                        )
                    }),
            )
    }

    fn render_project_modal(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let Some(edit_state) = &self.project_edit_state else {
            return div().into_any_element();
        };

        let is_new = edit_state.project_id.is_none();
        let title = if is_new {
            "New Project"
        } else {
            "Edit Project"
        };

        div()
            .id("project-modal-backdrop")
            .absolute()
            .inset_0()
            .bg(rgba(0x00000080))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _window, cx| {
                    this.close_project_modal(cx);
                }),
            )
            .child(
                v_flex()
                    .id("project-modal")
                    .w(px(450.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded_lg()
                    .shadow_xl()
                    .p_4()
                    .gap_4()
                    .on_mouse_down(MouseButton::Left, |_, _, _| {})
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(label(title).text_base())
                            .child(
                                Button::new("close-project-modal")
                                    .icon(ZqlzIcon::X)
                                    .ghost()
                                    .small()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_project_modal(cx);
                                    })),
                            ),
                    )
                    .child(
                        v_flex()
                            .gap_3()
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(caption("Name").text_color(theme.muted_foreground))
                                    .child(Input::new(&edit_state.name_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(
                                        caption("Description").text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.description_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(
                                        caption("Default Schema")
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.schema_input).w_full()),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .justify_end()
                            .child(
                                Button::new("cancel-project")
                                    .label("Cancel")
                                    .ghost()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_project_modal(cx);
                                    })),
                            )
                            .child(
                                Button::new("save-project")
                                    .label(if is_new { "Create" } else { "Save" })
                                    .primary()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.save_project_from_modal(cx);
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_model_modal(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let Some(edit_state) = &self.model_edit_state else {
            return div().into_any_element();
        };

        let is_new = edit_state.model_id.is_none();
        let title = if is_new { "New Model" } else { "Edit Model" };

        div()
            .id("model-modal-backdrop")
            .absolute()
            .inset_0()
            .bg(rgba(0x00000080))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _window, cx| {
                    this.close_model_modal(cx);
                }),
            )
            .child(
                v_flex()
                    .id("model-modal")
                    .w(px(500.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded_lg()
                    .shadow_xl()
                    .p_4()
                    .gap_4()
                    .on_mouse_down(MouseButton::Left, |_, _, _| {})
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(label(title).text_base())
                            .child(
                                Button::new("close-model-modal")
                                    .icon(ZqlzIcon::X)
                                    .ghost()
                                    .small()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_model_modal(cx);
                                    })),
                            ),
                    )
                    .child(
                        v_flex()
                            .gap_3()
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(caption("Name").text_color(theme.muted_foreground))
                                    .child(Input::new(&edit_state.name_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(
                                        caption("Description").text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.description_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(
                                        caption("SQL Template").text_color(theme.muted_foreground),
                                    )
                                    .child(
                                        body_small(
                                            "Use {{ ref('model_name') }} to reference other models",
                                        )
                                        .text_color(theme.muted_foreground),
                                    )
                                    .child(edit_state.sql_input.clone()),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .justify_end()
                            .child(
                                Button::new("cancel-model")
                                    .label("Cancel")
                                    .ghost()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_model_modal(cx);
                                    })),
                            )
                            .child(
                                Button::new("save-model")
                                    .label(if is_new { "Create" } else { "Save" })
                                    .primary()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.save_model_from_modal(cx);
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_source_modal(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let Some(edit_state) = &self.source_edit_state else {
            return div().into_any_element();
        };

        div()
            .id("source-modal-backdrop")
            .absolute()
            .inset_0()
            .bg(rgba(0x00000080))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _window, cx| {
                    this.close_source_modal(cx);
                }),
            )
            .child(
                v_flex()
                    .id("source-modal")
                    .w(px(450.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded_lg()
                    .shadow_xl()
                    .p_4()
                    .gap_4()
                    .on_mouse_down(MouseButton::Left, |_, _, _| {})
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(label("New Source").text_base())
                            .child(
                                Button::new("close-source-modal")
                                    .icon(ZqlzIcon::X)
                                    .ghost()
                                    .small()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_source_modal(cx);
                                    })),
                            ),
                    )
                    .child(
                        v_flex()
                            .gap_3()
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(
                                        caption("Source Name").text_color(theme.muted_foreground),
                                    )
                                    .child(
                                        body_small("e.g., raw_data, external_api")
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.name_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(caption("Schema").text_color(theme.muted_foreground))
                                    .child(
                                        body_small(
                                            "The database schema containing the source tables",
                                        )
                                        .text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.schema_input).w_full()),
                            )
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(caption("Tables").text_color(theme.muted_foreground))
                                    .child(
                                        body_small("Comma-separated table names")
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child(Input::new(&edit_state.tables_input).w_full()),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .justify_end()
                            .child(
                                Button::new("cancel-source")
                                    .label("Cancel")
                                    .ghost()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.close_source_modal(cx);
                                    })),
                            )
                            .child(
                                Button::new("save-source")
                                    .label("Create")
                                    .primary()
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.save_source_from_modal(cx);
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_empty_state(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();

        v_flex()
            .items_center()
            .justify_center()
            .py_8()
            .gap_3()
            .child(
                Icon::new(ZqlzIcon::Folder)
                    .size(px(40.0))
                    .text_color(theme.muted_foreground),
            )
            .child(label("No projects yet").text_color(theme.muted_foreground))
            .child(
                body_small("Create a project to organize your SQL models")
                    .text_color(theme.muted_foreground)
                    .text_center(),
            )
            .child(
                Button::new("create-first-project")
                    .label("Create Project")
                    .primary()
                    .small()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_new_project_modal(window, cx);
                    })),
            )
    }
}

impl Focusable for ProjectManagerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ProjectManagerPanel {}
impl EventEmitter<ProjectManagerEvent> for ProjectManagerPanel {}

impl Panel for ProjectManagerPanel {
    fn panel_name(&self) -> &'static str {
        "ProjectManagerPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Projects"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}

impl Render for ProjectManagerPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Pre-render project items BEFORE getting theme
        let project_items: Vec<_> = self
            .filtered_projects
            .clone()
            .into_iter()
            .map(|project| self.render_project_item(&project, cx))
            .collect();

        let is_empty = self.filtered_projects.is_empty();
        let has_no_projects = self.projects.is_empty();

        // Pre-render modals BEFORE getting theme
        let project_modal = if self.show_project_modal {
            Some(self.render_project_modal(cx))
        } else {
            None
        };
        let model_modal = if self.show_model_modal {
            Some(self.render_model_modal(cx))
        } else {
            None
        };
        let source_modal = if self.show_source_modal {
            Some(self.render_source_modal(cx))
        } else {
            None
        };

        // Pre-render empty state and no-match message
        let empty_state = if is_empty && has_no_projects {
            Some(self.render_empty_state(cx))
        } else {
            None
        };

        let no_match_message =
            if is_empty && !has_no_projects {
                let theme = cx.theme();
                Some(v_flex().items_center().py_4().child(
                    muted_small("No matching projects", cx).text_color(theme.muted_foreground),
                ))
            } else {
                None
            };

        // NOW get theme for final layout (after all mutable borrows done)
        let theme = cx.theme();

        v_flex()
            .id("project-manager-panel")
            .key_context("ProjectManagerPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            // Header
            .child(
                h_flex()
                    .p_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Input::new(&self.search_input).w_full().small())
                    .child(
                        Button::new("new-project")
                            .icon(ZqlzIcon::Plus)
                            .primary()
                            .small()
                            .tooltip("New Project")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_new_project_modal(window, cx);
                            })),
                    ),
            )
            // Content
            .child(
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .p_2()
                    .when_some(empty_state, |this, state| this.child(state))
                    .when_some(no_match_message, |this, msg| this.child(msg))
                    .when(!is_empty, |this| {
                        this.child(v_flex().gap_1().children(project_items))
                    }),
            )
            // Modals
            .when_some(project_modal, |this, modal| this.child(modal))
            .when_some(model_modal, |this, modal| this.child(modal))
            .when_some(source_modal, |this, modal| this.child(modal))
    }
}
