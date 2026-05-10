//! Sequence Designer Panel.

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Sizable,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    tab::{Tab, TabBar},
    typography::code,
    v_flex,
};

use crate::events::SequenceDesignerEvent;
use crate::models::SequenceDesign;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DesignerTab {
    #[default]
    General,
    Comment,
    SqlPreview,
}

pub struct SequenceDesignerPanel {
    focus_handle: FocusHandle,
    connection_id: Uuid,
    design: SequenceDesign,
    original_name: Option<String>,
    active_tab: DesignerTab,
    name_input: Entity<InputState>,
    owner_input: Entity<InputState>,
    data_type_input: Entity<InputState>,
    start_input: Entity<InputState>,
    current_input: Entity<InputState>,
    increment_input: Entity<InputState>,
    min_input: Entity<InputState>,
    max_input: Entity<InputState>,
    cache_input: Entity<InputState>,
    owned_by_table_input: Entity<InputState>,
    owned_by_column_input: Entity<InputState>,
    comment_input: Entity<InputState>,
    cycle: bool,
    no_min_value: bool,
    no_max_value: bool,
    ddl_preview: Option<String>,
    is_dirty: bool,
    _subscriptions: Vec<Subscription>,
}

impl SequenceDesignerPanel {
    pub fn new(
        connection_id: Uuid,
        schema: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::create(connection_id, SequenceDesign::new(schema), None, window, cx)
    }

    pub fn edit(
        connection_id: Uuid,
        design: SequenceDesign,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let original_name = Some(design.name.clone());
        Self::create(connection_id, design, original_name, window, cx)
    }

    fn create(
        connection_id: Uuid,
        design: SequenceDesign,
        original_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut subscriptions = Vec::new();
        let input = |value: String,
                     placeholder: &'static str,
                     window: &mut Window,
                     cx: &mut Context<Self>| {
            cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder(placeholder);
                state.set_value(&value, window, cx);
                state
            })
        };

        let name_input = input(design.name.clone(), "sequence_name", window, cx);
        let owner_input = input(
            design.owner.clone().unwrap_or_default(),
            "owner",
            window,
            cx,
        );
        let data_type_input = input(design.data_type.clone(), "bigint", window, cx);
        let start_input = input(design.start_value.to_string(), "1", window, cx);
        let current_input = input(
            design
                .current_value
                .map(|value| value.to_string())
                .unwrap_or_default(),
            "current value",
            window,
            cx,
        );
        let increment_input = input(design.increment_by.to_string(), "1", window, cx);
        let min_input = input(
            design
                .min_value
                .map(|value| value.to_string())
                .unwrap_or_default(),
            "no minimum",
            window,
            cx,
        );
        let max_input = input(
            design
                .max_value
                .map(|value| value.to_string())
                .unwrap_or_default(),
            "no maximum",
            window,
            cx,
        );
        let cache_input = input(design.cache_size.to_string(), "1", window, cx);
        let owned_by_table_input = input(
            design.owned_by_table.clone().unwrap_or_default(),
            "owned by table",
            window,
            cx,
        );
        let owned_by_column_input = input(
            design.owned_by_column.clone().unwrap_or_default(),
            "owned by column",
            window,
            cx,
        );
        let comment_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder("Comment")
                .multi_line(true);
            state.set_value(design.comment.clone().unwrap_or_default(), window, cx);
            state
        });

        for input in [
            &name_input,
            &owner_input,
            &data_type_input,
            &start_input,
            &current_input,
            &increment_input,
            &min_input,
            &max_input,
            &cache_input,
            &owned_by_table_input,
            &owned_by_column_input,
            &comment_input,
        ] {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
        }

        Self {
            focus_handle: cx.focus_handle(),
            connection_id,
            cycle: design.cycle,
            no_min_value: design.min_value.is_none(),
            no_max_value: design.max_value.is_none(),
            design,
            original_name,
            active_tab: DesignerTab::General,
            name_input,
            owner_input,
            data_type_input,
            start_input,
            current_input,
            increment_input,
            min_input,
            max_input,
            cache_input,
            owned_by_table_input,
            owned_by_column_input,
            comment_input,
            ddl_preview: None,
            is_dirty: false,
            _subscriptions: subscriptions,
        }
    }

    fn mark_dirty(&mut self, cx: &mut Context<Self>) {
        self.is_dirty = true;
        self.ddl_preview = None;
        cx.notify();
    }

    fn sync_from_inputs(&mut self, cx: &Context<Self>) {
        self.design.name = self.name_input.read(cx).value().to_string();
        self.design.owner = optional_string(self.owner_input.read(cx).value());
        self.design.data_type = self.data_type_input.read(cx).value().to_string();
        self.design.start_value = parse_i64(self.start_input.read(cx).value(), 1);
        self.design.current_value = optional_i64(self.current_input.read(cx).value());
        self.design.increment_by = parse_i64(self.increment_input.read(cx).value(), 1);
        self.design.min_value = if self.no_min_value {
            None
        } else {
            Some(parse_i64(self.min_input.read(cx).value(), 1))
        };
        self.design.max_value = if self.no_max_value {
            None
        } else {
            Some(parse_i64(
                self.max_input.read(cx).value(),
                9_223_372_036_854_775_807,
            ))
        };
        self.design.cache_size = parse_i64(self.cache_input.read(cx).value(), 1);
        self.design.cycle = self.cycle;
        self.design.owned_by_table = optional_string(self.owned_by_table_input.read(cx).value());
        self.design.owned_by_column = optional_string(self.owned_by_column_input.read(cx).value());
        self.design.comment = optional_string(self.comment_input.read(cx).value());
    }

    fn handle_save(&mut self, cx: &mut Context<Self>) {
        self.sync_from_inputs(cx);
        if self.design.validate().is_some() {
            cx.notify();
            return;
        }
        cx.emit(SequenceDesignerEvent::Save {
            connection_id: self.connection_id,
            design: self.design.clone(),
            is_new: self.design.is_new,
            original_name: self.original_name.clone(),
        });
    }

    fn generate_preview(&mut self, cx: &mut Context<Self>) {
        self.sync_from_inputs(cx);
        self.ddl_preview = Some(self.design.to_ddl());
        cx.notify();
    }

    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        TabBar::new("sequence-designer-tabs")
            .small()
            .w_full()
            .selected_index(match self.active_tab {
                DesignerTab::General => 0,
                DesignerTab::Comment => 1,
                DesignerTab::SqlPreview => 2,
            })
            .on_click(cx.listener(|this, ix: &usize, _window, cx| {
                this.active_tab = match ix {
                    0 => DesignerTab::General,
                    1 => DesignerTab::Comment,
                    2 => {
                        this.generate_preview(cx);
                        DesignerTab::SqlPreview
                    }
                    _ => DesignerTab::General,
                };
                cx.notify();
            }))
            .child(Tab::new().label("General"))
            .child(Tab::new().label("Comment"))
            .child(Tab::new().label("SQL Preview"))
    }

    fn labeled_input(
        label: &'static str,
        input: &Entity<InputState>,
        width: Pixels,
    ) -> impl IntoElement {
        h_flex()
            .gap_2()
            .items_center()
            .child(
                div()
                    .w(px(160.0))
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(label),
            )
            .child(Input::new(input).small().w(width))
    }

    fn render_general_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .p_4()
            .gap_3()
            .child(Self::labeled_input("Name:", &self.name_input, px(260.0)))
            .child(Self::labeled_input("Owner:", &self.owner_input, px(260.0)))
            .child(Self::labeled_input(
                "Data Type:",
                &self.data_type_input,
                px(260.0),
            ))
            .child(Self::labeled_input(
                "Starting Value:",
                &self.start_input,
                px(260.0),
            ))
            .child(Self::labeled_input(
                "Current Value:",
                &self.current_input,
                px(260.0),
            ))
            .child(Self::labeled_input(
                "Increment:",
                &self.increment_input,
                px(260.0),
            ))
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(Self::labeled_input(
                        "Minimum Value:",
                        &self.min_input,
                        px(260.0),
                    ))
                    .child(
                        Checkbox::new("no-min-value")
                            .checked(self.no_min_value)
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.no_min_value = !this.no_min_value;
                                this.mark_dirty(cx);
                            })),
                    )
                    .child(div().text_sm().child("No minimum value")),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(Self::labeled_input(
                        "Maximum Value:",
                        &self.max_input,
                        px(260.0),
                    ))
                    .child(
                        Checkbox::new("no-max-value")
                            .checked(self.no_max_value)
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.no_max_value = !this.no_max_value;
                                this.mark_dirty(cx);
                            })),
                    )
                    .child(div().text_sm().child("No maximum value")),
            )
            .child(Self::labeled_input(
                "Cache Size:",
                &self.cache_input,
                px(260.0),
            ))
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(160.0)))
                    .child(
                        Checkbox::new("sequence-cycle")
                            .checked(self.cycle)
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.cycle = !this.cycle;
                                this.mark_dirty(cx);
                            })),
                    )
                    .child(div().text_sm().child("Cyclic")),
            )
            .child(Self::labeled_input(
                "Owned By Table:",
                &self.owned_by_table_input,
                px(260.0),
            ))
            .child(Self::labeled_input(
                "Owned By Column:",
                &self.owned_by_column_input,
                px(260.0),
            ))
    }

    fn render_comment_tab(&self) -> impl IntoElement {
        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Comment"),
            )
            .child(
                div()
                    .flex_1()
                    .child(Input::new(&self.comment_input).w_full().h_full()),
            )
    }

    fn render_sql_preview_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let ddl = self
            .ddl_preview
            .clone()
            .unwrap_or_else(|| "-- Click 'Refresh Preview' to see DDL".to_string());

        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                h_flex()
                    .justify_between()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Generated DDL"),
                    )
                    .child(
                        Button::new("sequence-refresh-preview")
                            .label("Refresh Preview")
                            .xsmall()
                            .ghost()
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.generate_preview(cx);
                            })),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .p_3()
                    .rounded_md()
                    .bg(theme.secondary)
                    .border_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(code(&ddl)),
            )
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let mut design = self.design.clone();
        design.name = self.name_input.read(cx).value().to_string();
        let error = design.validate();

        h_flex()
            .w_full()
            .justify_between()
            .p_3()
            .border_t_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().danger)
                    .child(error.clone().unwrap_or_default()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("sequence-cancel")
                            .label("Cancel")
                            .small()
                            .ghost()
                            .on_click(cx.listener(|_this, _, _window, cx| {
                                cx.emit(SequenceDesignerEvent::Cancel);
                            })),
                    )
                    .child(
                        Button::new("sequence-save")
                            .label(if self.design.is_new {
                                "Create Sequence"
                            } else {
                                "Save Changes"
                            })
                            .small()
                            .primary()
                            .disabled(error.is_some() || !self.is_dirty)
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.handle_save(cx);
                            })),
                    ),
            )
    }
}

impl Render for SequenceDesignerPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tab_content = match self.active_tab {
            DesignerTab::General => self.render_general_tab(cx).into_any_element(),
            DesignerTab::Comment => self.render_comment_tab().into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview_tab(cx).into_any_element(),
        };

        v_flex()
            .id("sequence-designer-panel")
            .key_context("SequenceDesignerPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().background)
            .child(self.render_tab_bar(cx))
            .child(div().flex_1().overflow_hidden().child(tab_content))
            .child(self.render_footer(cx))
    }
}

impl Focusable for SequenceDesignerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for SequenceDesignerPanel {}
impl EventEmitter<SequenceDesignerEvent> for SequenceDesignerPanel {}

impl Panel for SequenceDesignerPanel {
    fn panel_name(&self) -> &'static str {
        "SequenceDesignerPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.design.is_new {
            SharedString::from("New Sequence")
        } else {
            SharedString::from(format!("Sequence: {}", self.design.name))
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}

fn optional_string(value: impl AsRef<str>) -> Option<String> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn optional_i64(value: impl AsRef<str>) -> Option<i64> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        None
    } else {
        value.parse().ok()
    }
}

fn parse_i64(value: impl AsRef<str>, default: i64) -> i64 {
    value.as_ref().trim().parse().unwrap_or(default)
}
