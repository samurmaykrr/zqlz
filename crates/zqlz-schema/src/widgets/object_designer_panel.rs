use std::collections::BTreeMap;

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{
    ObjectFormDdlRequest, ObjectFormField, ObjectFormFieldKind, ObjectFormSpec, ObjectFormValue,
    ObjectsPanelObjectRef,
};
use zqlz_ui::widgets::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariant, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent},
    form::{field, v_form},
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex,
};

#[derive(Clone, Debug)]
pub enum ObjectDesignerPanelEvent {
    GenerateDdl {
        connection_id: Uuid,
        execute: bool,
        request: ObjectFormDdlRequest,
    },
}

struct ObjectFormInput {
    field: ObjectFormField,
    input: Option<Entity<InputState>>,
    checked: bool,
}

pub struct ObjectDesignerPanel {
    connection_id: Uuid,
    spec: ObjectFormSpec,
    object_ref: Option<ObjectsPanelObjectRef>,
    inputs: Vec<ObjectFormInput>,
    ddl_preview: String,
    error: Option<String>,
    executing: bool,
    dirty: bool,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<PanelEvent> for ObjectDesignerPanel {}
impl EventEmitter<ObjectDesignerPanelEvent> for ObjectDesignerPanel {}

impl Focusable for ObjectDesignerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl ObjectDesignerPanel {
    pub fn new(
        connection_id: Uuid,
        spec: ObjectFormSpec,
        object_ref: Option<ObjectsPanelObjectRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut inputs = Vec::new();
        let mut subscriptions = Vec::new();

        for form_field in spec
            .sections
            .iter()
            .flat_map(|section| section.fields.iter())
        {
            let (input, checked) = match &form_field.default_value {
                ObjectFormValue::Bool(value) => (None, *value),
                ObjectFormValue::StringList(values) => {
                    let input = cx.new(|cx| {
                        InputState::new(window, cx)
                            .code_editor("text")
                            .placeholder(form_field.placeholder.clone().unwrap_or_default())
                    });
                    input.update(cx, |state, cx| {
                        state.set_value(values.join("\n"), window, cx);
                    });
                    (Some(input), false)
                }
                ObjectFormValue::KeyValueList(values) => {
                    let input = cx.new(|cx| {
                        InputState::new(window, cx)
                            .code_editor("text")
                            .placeholder(form_field.placeholder.clone().unwrap_or_default())
                    });
                    input.update(cx, |state, cx| {
                        let value = values
                            .iter()
                            .map(|(key, value)| format!("{key}={value}"))
                            .collect::<Vec<_>>()
                            .join("\n");
                        state.set_value(value, window, cx);
                    });
                    (Some(input), false)
                }
                ObjectFormValue::String(value) => {
                    let is_multiline = matches!(
                        form_field.kind,
                        ObjectFormFieldKind::TextArea | ObjectFormFieldKind::SqlExpression
                    );
                    let input = cx.new(|cx| {
                        let state = InputState::new(window, cx)
                            .placeholder(form_field.placeholder.clone().unwrap_or_default());
                        if is_multiline {
                            state.code_editor("sql")
                        } else {
                            state
                        }
                    });
                    input.update(cx, |state, cx| {
                        state.set_value(value.clone(), window, cx);
                    });
                    (Some(input), false)
                }
            };

            if let Some(input) = &input {
                subscriptions.push(cx.subscribe_in(
                    input,
                    window,
                    |this, _, event: &InputEvent, _window, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.dirty = true;
                            this.error = None;
                            cx.notify();
                        }
                    },
                ));
            }

            inputs.push(ObjectFormInput {
                field: form_field.clone(),
                input,
                checked,
            });
        }

        Self {
            connection_id,
            spec,
            object_ref,
            inputs,
            ddl_preview: String::new(),
            error: None,
            executing: false,
            dirty: false,
            focus_handle: cx.focus_handle(),
            _subscriptions: subscriptions,
        }
    }

    pub fn set_ddl_preview(&mut self, ddl: String, cx: &mut Context<Self>) {
        self.ddl_preview = ddl;
        self.error = None;
        self.executing = false;
        self.dirty = false;
        cx.notify();
    }

    pub fn set_error(&mut self, error: String, cx: &mut Context<Self>) {
        self.error = Some(error);
        self.executing = false;
        cx.notify();
    }

    pub fn set_executing(&mut self, executing: bool, cx: &mut Context<Self>) {
        self.executing = executing;
        cx.notify();
    }

    fn request(&self, cx: &App) -> ObjectFormDdlRequest {
        let mut values = BTreeMap::new();

        for input in &self.inputs {
            let value = match input.field.kind {
                ObjectFormFieldKind::Checkbox => ObjectFormValue::Bool(input.checked),
                ObjectFormFieldKind::StringList => ObjectFormValue::StringList(
                    input
                        .input
                        .as_ref()
                        .map(|state| state.read(cx).text().to_string())
                        .unwrap_or_default()
                        .lines()
                        .map(|line| line.trim().to_string())
                        .filter(|line| !line.is_empty())
                        .collect(),
                ),
                ObjectFormFieldKind::KeyValueList => ObjectFormValue::KeyValueList(
                    input
                        .input
                        .as_ref()
                        .map(|state| state.read(cx).text().to_string())
                        .unwrap_or_default()
                        .lines()
                        .filter_map(|line| {
                            let (key, value) = line.split_once('=')?;
                            Some((key.trim().to_string(), value.trim().to_string()))
                        })
                        .filter(|(key, _)| !key.is_empty())
                        .collect(),
                ),
                _ => ObjectFormValue::String(
                    input
                        .input
                        .as_ref()
                        .map(|state| state.read(cx).text().to_string())
                        .unwrap_or_default(),
                ),
            };
            values.insert(input.field.id.clone(), value);
        }

        ObjectFormDdlRequest {
            kind_id: self.spec.kind_id.clone(),
            mode: self.spec.mode,
            object_ref: self.object_ref.clone(),
            values,
        }
    }

    fn emit_generate_ddl(&mut self, execute: bool, cx: &mut Context<Self>) {
        self.error = None;
        self.executing = execute;
        cx.emit(ObjectDesignerPanelEvent::GenerateDdl {
            connection_id: self.connection_id,
            execute,
            request: self.request(cx),
        });
        cx.notify();
    }

    fn render_form(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_form().children(self.inputs.iter().map(|input| {
            let field_id = input.field.id.clone();
            let label = input.field.label.clone();
            let required = input.field.required;
            let help_text = input.field.help_text.clone();

            let child = match input.field.kind {
                ObjectFormFieldKind::Checkbox => Checkbox::new(format!("object-form-{field_id}"))
                    .checked(input.checked)
                    .disabled(input.field.read_only)
                    .on_click(cx.listener(move |this, checked, _, cx| {
                        if let Some(input) = this
                            .inputs
                            .iter_mut()
                            .find(|input| input.field.id == field_id)
                        {
                            input.checked = *checked;
                            this.dirty = true;
                            this.error = None;
                        }
                        cx.notify();
                    }))
                    .into_any_element(),
                ObjectFormFieldKind::TextArea
                | ObjectFormFieldKind::SqlExpression
                | ObjectFormFieldKind::StringList
                | ObjectFormFieldKind::KeyValueList => input
                    .input
                    .as_ref()
                    .map(|state| {
                        Input::new(state)
                            .h(px(120.0))
                            .disabled(input.field.read_only)
                            .into_any_element()
                    })
                    .unwrap_or_else(|| div().into_any_element()),
                _ => input
                    .input
                    .as_ref()
                    .map(|state| {
                        Input::new(state)
                            .disabled(input.field.read_only)
                            .into_any_element()
                    })
                    .unwrap_or_else(|| div().into_any_element()),
            };

            field()
                .label(label)
                .required(required)
                .when_some(help_text, |field, help_text| field.description(help_text))
                .child(child)
        }))
    }
}

impl Panel for ObjectDesignerPanel {
    fn panel_name(&self) -> &'static str {
        "ObjectDesignerPanel"
    }

    fn tab_name(&self, _cx: &App) -> Option<SharedString> {
        Some(self.spec.title.clone().into())
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.spec.title.clone()
    }

    fn has_unsaved_changes(&self, _cx: &App) -> bool {
        self.dirty
    }
}

impl Render for ObjectDesignerPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let danger = cx.theme().danger;
        let border = cx.theme().border;
        let secondary = cx.theme().secondary;
        let execute_label = if self.executing {
            "Executing..."
        } else {
            "Execute"
        };
        let preview = if self.ddl_preview.is_empty() {
            "-- SQL preview".to_string()
        } else {
            self.ddl_preview.clone()
        };
        let preview_lines = preview
            .lines()
            .map(|line| {
                div()
                    .min_h_5()
                    .child(if line.is_empty() {
                        " ".to_string()
                    } else {
                        line.to_string()
                    })
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        v_flex()
            .id("object-designer-panel")
            .size_full()
            .gap_3()
            .p_4()
            .track_focus(&self.focus_handle)
            .child(self.render_form(cx))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("object-form-preview")
                            .secondary()
                            .label("Preview SQL")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.emit_generate_ddl(false, cx);
                            })),
                    )
                    .child(
                        Button::new("object-form-execute")
                            .with_variant(ButtonVariant::Primary)
                            .label(execute_label)
                            .disabled(self.executing)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.emit_generate_ddl(true, cx);
                            })),
                    ),
            )
            .when_some(self.error.clone(), |this, error| {
                this.child(div().text_color(danger).text_sm().child(error))
            })
            .child(
                div()
                    .rounded_md()
                    .border_1()
                    .border_color(border)
                    .bg(secondary)
                    .p_3()
                    .font_family("monospace")
                    .text_sm()
                    .children(preview_lines),
            )
    }
}
