//! Window-level interaction tests for the command palette.
//!
//! These open a real GPUI window so they exercise the actual keyboard wiring
//! between the search input, the underlying `ListState`, and the delegate —
//! not just the delegate's filtering logic in isolation.

use std::cell::RefCell;
use std::rc::Rc;

use gpui::{Entity, TestAppContext, VisualTestContext, actions};

use crate::{Command, CommandCategory, CommandPalette, CommandPaletteEvent};

actions!(
    command_palette_render_tests,
    [FirstFilteredAction, SecondFilteredAction, UnrelatedAction]
);

fn init_test(cx: &mut TestAppContext) {
    cx.update(zqlz_ui::init);
}

/// Open the palette with `commands` in a real window and paint it.
fn open_palette(
    cx: &mut TestAppContext,
    commands: Vec<Command>,
) -> (Entity<CommandPalette>, VisualTestContext) {
    zqlz_test_support::open_in_window(cx, (600.0, 500.0), |window, cx| {
        CommandPalette::new(commands, None, None, window, cx)
    })
}

/// The command id of the currently selected row, read straight off the
/// delegate rather than inferred from painted geometry.
fn selected_command_id(
    palette: &Entity<CommandPalette>,
    cx: &mut VisualTestContext,
) -> Option<String> {
    palette.read_with(cx, |palette, cx| {
        let delegate = palette.list_state.read(cx).delegate();
        delegate
            .selected_index
            .and_then(|ix| delegate.command_index_at(ix))
            .and_then(|index| delegate.commands.get(index))
            .map(|command| command.id.clone())
    })
}

#[gpui::test]
fn typing_filters_arrow_keys_navigate_and_enter_confirms_the_navigated_command(
    cx: &mut TestAppContext,
) {
    init_test(cx);

    let commands = vec![
        Command::new_static(
            "alpha-one",
            "Alpha One",
            CommandCategory::Application,
            FirstFilteredAction,
        ),
        Command::new_static(
            "alpha-two",
            "Alpha Two",
            CommandCategory::Editor,
            SecondFilteredAction,
        ),
        Command::new_static(
            "beta-three",
            "Beta Three",
            CommandCategory::Layout,
            UnrelatedAction,
        ),
    ];

    let (palette, mut cx) = open_palette(cx, commands);

    palette.update_in(&mut cx, |palette, window, cx| {
        palette.focus(window, cx);
    });
    cx.run_until_parked();

    let recorded_events: Rc<RefCell<Vec<CommandPaletteEvent>>> = Rc::default();
    let subscription = {
        let recorded_events = recorded_events.clone();
        cx.update(|_window, cx| {
            cx.subscribe(&palette, move |_palette, event, _cx| {
                recorded_events.borrow_mut().push(event.clone());
            })
        })
    };

    cx.simulate_input("alpha");

    let visible_after_filter = palette.read_with(&cx, |palette, cx| {
        palette
            .list_state
            .read(cx)
            .delegate()
            .visible_commands_count()
    });
    assert_eq!(
        visible_after_filter, 2,
        "typing \"alpha\" should filter out Beta Three"
    );
    assert_eq!(
        selected_command_id(&palette, &mut cx).as_deref(),
        Some("alpha-one"),
        "the first filtered result should be selected by default"
    );

    cx.simulate_keystrokes("down");
    assert_eq!(
        selected_command_id(&palette, &mut cx).as_deref(),
        Some("alpha-two"),
        "arrow-down should move the selection to the next filtered row"
    );

    cx.simulate_keystrokes("enter");

    let events = recorded_events.borrow().clone();
    assert!(
        events.iter().any(|event| matches!(
            event,
            CommandPaletteEvent::CommandExecuted(id) if id == "alpha-two"
        )),
        "confirming should execute the command that arrow-down navigated to, not the default \
         selection: got {events:?}"
    );
    assert!(
        matches!(events.last(), Some(CommandPaletteEvent::Dismissed)),
        "confirming should dismiss the palette"
    );

    drop(subscription);
}
