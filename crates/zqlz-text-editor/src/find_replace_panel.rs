use gpui::prelude::FluentBuilder as _;
use gpui::{
    App, AppContext as _, Context, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement as _, IntoElement, KeyBinding, Keystroke, ParentElement as _, Render,
    SharedString, Styled, Subscription, Window, actions, div,
};

use crate::find::FindOptions;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, IconName, Selectable, Sizable,
    button::{Button, ButtonVariants},
    h_flex,
    input::{Input, InputEvent, InputState},
    kbd::Kbd,
    label::Label,
    v_flex,
};

const CONTEXT: &str = "FindReplacePanel";

actions!(
    find_replace_panel,
    [FindNextMatch, FindPrevMatch, ClosePanel,]
);

pub fn init(cx: &mut App) {
    cx.bind_keys(vec![
        KeyBinding::new("shift-enter", FindPrevMatch, Some(CONTEXT)),
        KeyBinding::new("escape", ClosePanel, Some(CONTEXT)),
        KeyBinding::new("enter", FindNextMatch, Some(CONTEXT)),
    ]);
}

#[derive(Clone)]
pub enum FindReplacePanelEvent {
    QueryChanged { query: String, options: FindOptions },
    NextMatch,
    PrevMatch,
    ReplaceCurrent { replacement: String },
    ReplaceAll { replacement: String },
    SelectAllMatches,
    Closed,
}

pub struct FindReplacePanel {
    search_input: Entity<InputState>,
    replace_input: Entity<InputState>,
    case_sensitive: bool,
    whole_word: bool,
    use_regex: bool,
    #[allow(dead_code)]
    search_in_selection: bool,
    replace_mode: bool,

    total_matches: usize,
    current_match_display: usize,
    regex_error: Option<String>,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<FindReplacePanelEvent> for FindReplacePanel {}

impl FindReplacePanel {
    pub fn new(
        show_replace: bool,
        initial_query: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let search_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_placeholder("Find", window, cx);
            state
        });
        let replace_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_placeholder("Replace", window, cx);
            state
        });

        if let Some(query) = &initial_query
            && !query.is_empty()
        {
            search_input.update(cx, |state: &mut InputState, cx| {
                state.set_value(query.clone(), window, cx);
            });
        }

        search_input.read(cx).focus_handle(cx).focus(window, cx);

        let mut subscriptions = Vec::new();

        subscriptions.push(cx.subscribe(
            &search_input,
            |this: &mut Self, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    this.emit_query_changed(cx);
                }
            },
        ));

        Self {
            search_input,
            replace_input,
            case_sensitive: false,
            whole_word: false,
            use_regex: false,
            search_in_selection: false,
            replace_mode: show_replace,
            total_matches: 0,
            current_match_display: 0,
            regex_error: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn query(&self, cx: &App) -> String {
        self.search_input.read(cx).value().to_string()
    }

    pub fn find_options(&self) -> FindOptions {
        FindOptions {
            case_sensitive: self.case_sensitive,
            whole_word: self.whole_word,
            use_regex: self.use_regex,
        }
    }

    pub fn update_match_info(
        &mut self,
        total_matches: usize,
        current_match_display: usize,
        regex_error: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.total_matches = total_matches;
        self.current_match_display = current_match_display;
        self.regex_error = regex_error;
        cx.notify();
    }

    pub fn focus_search(&self, window: &mut Window, cx: &mut App) {
        self.search_input
            .read(cx)
            .focus_handle(cx)
            .focus(window, cx);
    }

    pub fn set_replace_mode(&mut self, replace_mode: bool) {
        self.replace_mode = replace_mode;
    }

    fn emit_query_changed(&mut self, cx: &mut Context<Self>) {
        let query = self.search_input.read(cx).value().to_string();
        let options = self.find_options();
        cx.emit(FindReplacePanelEvent::QueryChanged { query, options });
    }
}

impl Focusable for FindReplacePanel {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.search_input.read(cx).focus_handle(cx)
    }
}

impl Render for FindReplacePanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let has_matches = self.total_matches > 0;
        let query_text = self.search_input.read(cx).value().to_string();
        let has_query = !query_text.is_empty();

        let match_label: SharedString = if self.regex_error.is_some() {
            "Regex error".into()
        } else if self.total_matches == 0 {
            if has_query {
                "No results".into()
            } else {
                "".into()
            }
        } else {
            format!("{}/{}", self.current_match_display, self.total_matches).into()
        };

        let label_is_error = self.regex_error.is_some() || (has_query && self.total_matches == 0);
        let show_label = !match_label.is_empty();

        let replace_focused = self
            .replace_input
            .read(cx)
            .focus_handle(cx)
            .is_focused(window);
        let top_primary_label = if self.replace_mode && replace_focused {
            "Replace"
        } else {
            "Next"
        };

        v_flex()
            .id("find-replace-panel")
            .occlude()
            .track_focus(&self.focus_handle(cx))
            .key_context(CONTEXT)
            .on_action(cx.listener(|_, _: &FindPrevMatch, _, cx| {
                cx.emit(FindReplacePanelEvent::PrevMatch);
            }))
            .on_action(cx.listener(|_, _: &ClosePanel, _, cx| {
                cx.emit(FindReplacePanelEvent::Closed);
            }))
            .on_action(cx.listener({
                move |this, _: &FindNextMatch, _, cx| {
                    if this.replace_mode && replace_focused {
                        let replacement = this.replace_input.read(cx).value().to_string();
                        cx.emit(FindReplacePanelEvent::ReplaceCurrent { replacement });
                    } else {
                        cx.emit(FindReplacePanelEvent::NextMatch);
                    }
                }
            }))
            .font_family(cx.theme().font_family.clone())
            .py_1p5()
            .px_3()
            .w_full()
            .gap_1()
            .bg(cx.theme().popover)
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .justify_end()
                    .w_full()
                    .gap_3()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(Self::shortcut_hint(top_primary_label, "enter"))
                    .child(Self::shortcut_hint("Previous", "shift-enter"))
                    .child(Self::shortcut_hint("In Selection", "alt-l"))
                    .child(Self::shortcut_hint("Close", "escape")),
            )
            // Row 1: Search row
            .child(
                h_flex()
                    .w_full()
                    .gap_1()
                    .items_center()
                    .child(
                        Button::new("toggle-replace")
                            .xsmall()
                            .ghost()
                            .icon(if self.replace_mode {
                                IconName::ChevronDown
                            } else {
                                IconName::ChevronRight
                            })
                            .tooltip("Toggle Replace")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.replace_mode = !this.replace_mode;
                                if this.replace_mode {
                                    this.replace_input
                                        .read(cx)
                                        .focus_handle(cx)
                                        .focus(window, cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        div().flex_1().child(
                            Input::new(&self.search_input)
                                .focus_bordered(false)
                                .suffix(
                                    h_flex()
                                        .gap_0p5()
                                        .child(
                                            Button::new("case-sensitive")
                                                .selected(self.case_sensitive)
                                                .xsmall()
                                                .compact()
                                                .ghost()
                                                .icon(IconName::CaseSensitive)
                                                .tooltip("Match Case (Alt+C)")
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.case_sensitive = !this.case_sensitive;
                                                    this.emit_query_changed(cx);
                                                    cx.notify();
                                                })),
                                        )
                                        .child(
                                            Button::new("whole-word")
                                                .selected(self.whole_word)
                                                .xsmall()
                                                .compact()
                                                .ghost()
                                                .icon(IconName::WholeWord)
                                                .tooltip("Match Whole Word (Alt+W)")
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.whole_word = !this.whole_word;
                                                    this.emit_query_changed(cx);
                                                    cx.notify();
                                                })),
                                        )
                                        .child(
                                            Button::new("regex")
                                                .selected(self.use_regex)
                                                .xsmall()
                                                .compact()
                                                .ghost()
                                                .icon(IconName::Regex)
                                                .tooltip("Use Regular Expression (Alt+R)")
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.use_regex = !this.use_regex;
                                                    this.emit_query_changed(cx);
                                                    cx.notify();
                                                })),
                                        ),
                                )
                                .small()
                                .w_full()
                                .shadow_none(),
                        ),
                    )
                    .child(
                        Button::new("prev-match")
                            .xsmall()
                            .ghost()
                            .icon(IconName::ArrowUp)
                            .tooltip("Previous Match")
                            .disabled(!has_matches)
                            .on_click(cx.listener(|_, _, _, cx| {
                                cx.emit(FindReplacePanelEvent::PrevMatch);
                            })),
                    )
                    .child(
                        Button::new("next-match")
                            .xsmall()
                            .ghost()
                            .icon(IconName::ArrowDown)
                            .tooltip("Next Match")
                            .disabled(!has_matches)
                            .on_click(cx.listener(|_, _, _, cx| {
                                cx.emit(FindReplacePanelEvent::NextMatch);
                            })),
                    )
                    .when(show_label, |this| {
                        this.child(
                            Label::new(match_label)
                                .when(label_is_error, |label| label.text_color(cx.theme().danger))
                                .when(!label_is_error, |label| {
                                    label.text_color(cx.theme().muted_foreground)
                                })
                                .min_w_12(),
                        )
                    })
                    .child(
                        Button::new("close-find")
                            .xsmall()
                            .ghost()
                            .icon(IconName::Close)
                            .tooltip("Close")
                            .on_click(cx.listener(|_, _, _, cx| {
                                cx.emit(FindReplacePanelEvent::Closed);
                            })),
                    ),
            )
            // Row 2: Replace row
            .when(self.replace_mode, |this| {
                this.child(
                    h_flex()
                        .w_full()
                        .gap_1()
                        .items_center()
                        .child(div().w_6())
                        .child(
                            div().flex_1().child(
                                Input::new(&self.replace_input)
                                    .focus_bordered(false)
                                    .small()
                                    .w_full()
                                    .shadow_none(),
                            ),
                        )
                        .child(
                            Button::new("replace-one")
                                .xsmall()
                                .ghost()
                                .icon(IconName::Replace)
                                .tooltip("Replace")
                                .disabled(!has_matches)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    let replacement =
                                        this.replace_input.read(cx).value().to_string();
                                    cx.emit(FindReplacePanelEvent::ReplaceCurrent { replacement });
                                })),
                        )
                        .child(
                            Button::new("replace-all")
                                .xsmall()
                                .ghost()
                                .label("All")
                                .tooltip("Replace All")
                                .disabled(!has_matches)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    let replacement =
                                        this.replace_input.read(cx).value().to_string();
                                    cx.emit(FindReplacePanelEvent::ReplaceAll { replacement });
                                })),
                        ),
                )
            })
    }
}

impl FindReplacePanel {
    fn shortcut_hint(label: impl Into<SharedString>, keystroke: &str) -> impl IntoElement {
        let label = label.into();
        h_flex().gap_1().items_center().child(label).when_some(
            Keystroke::parse(keystroke).ok().map(Kbd::new),
            |this, kbd| this.child(kbd),
        )
    }
}
