//! Construction and window-level tests for [`TriggerDesignerPanel`].
//!
//! The construction test pins the derived state the two entry points
//! (`new`/`edit`) are responsible for computing. The window tests open a real
//! GPUI window, paint the panel, and drive it by clicking painted pixels so
//! they cover the wiring a plain state test can't: that the tab bar, event
//! checkboxes, and footer actions actually reach the screen, and that a click
//! on them lands on the handler that mutates the panel's own state.

use gpui::{Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;
use zqlz_test_support::click;

use super::{DesignerTab, TriggerDesignerPanel};
use crate::models::{DatabaseDialect, TriggerDesign, TriggerEvent};

const WINDOW_SIZE: (f32, f32) = (900., 700.);

fn init_test(cx: &mut TestAppContext) {
    cx.update(zqlz_ui::init);
}

/// Open a window showing a new trigger designer for `dialect` and paint it.
fn open_panel(
    cx: &mut TestAppContext,
    connection_id: Uuid,
    dialect: DatabaseDialect,
) -> (Entity<TriggerDesignerPanel>, VisualTestContext) {
    zqlz_test_support::open_in_window(cx, WINDOW_SIZE, move |window, cx| {
        TriggerDesignerPanel::new(
            connection_id,
            dialect,
            vec!["orders".to_string(), "users".to_string()],
            vec!["status".to_string(), "total".to_string()],
            vec!["audit.log_change".to_string()],
            window,
            cx,
        )
    })
}

#[gpui::test]
fn constructing_new_and_edit_panels_derives_the_expected_state(cx: &mut TestAppContext) {
    init_test(cx);
    let connection_id = Uuid::new_v4();

    let window = cx.add_window(|window, cx| {
        TriggerDesignerPanel::new(
            connection_id,
            DatabaseDialect::Postgres,
            vec!["orders".to_string()],
            vec!["status".to_string()],
            Vec::new(),
            window,
            cx,
        )
    });
    let panel = window
        .root(cx)
        .expect("trigger designer panel should be the window root");
    panel.read_with(cx, |panel, cx| {
        assert_eq!(panel.connection_id, connection_id);
        assert!(panel.original_name.is_none());
        assert_eq!(panel.active_tab, DesignerTab::General);
        assert!(!panel.is_dirty);
        assert!(panel.design.is_new);
        assert_eq!(panel.design.dialect, DatabaseDialect::Postgres);
        // A fresh design starts with a single INSERT event and row-level
        // granularity, which the checkboxes should mirror.
        assert!(panel.insert_checked);
        assert!(!panel.update_checked);
        assert!(!panel.delete_checked);
        assert!(!panel.truncate_checked);
        assert!(panel.for_each_row);
        assert!(panel.enabled);
        assert!(panel.ddl_preview.is_none());
        assert_eq!(panel.name_input.read(cx).value().as_ref(), "");
    });

    let mut design = TriggerDesign::new(DatabaseDialect::Mysql);
    design.name = "audit_orders".to_string();
    design.table_name = "orders".to_string();
    design.events = vec![TriggerEvent::Delete];
    design.is_new = false;
    let window = cx.add_window(|window, cx| {
        TriggerDesignerPanel::edit(
            connection_id,
            design.clone(),
            vec!["orders".to_string()],
            Vec::new(),
            Vec::new(),
            window,
            cx,
        )
    });
    let panel = window
        .root(cx)
        .expect("trigger designer panel should be the window root");
    panel.read_with(cx, |panel, cx| {
        // Editing an existing trigger should remember its original name (for
        // rename detection on save) and preload its checked events from the
        // design rather than defaulting to INSERT.
        assert_eq!(panel.original_name.as_deref(), Some("audit_orders"));
        assert!(!panel.design.is_new);
        assert!(!panel.insert_checked);
        assert!(panel.delete_checked);
        assert!(!panel.is_dirty);
        assert_eq!(panel.name_input.read(cx).value().as_ref(), "audit_orders");
    });
}

#[gpui::test]
fn panel_paints_the_tab_bar_general_fields_and_footer_actions(cx: &mut TestAppContext) {
    init_test(cx);
    let (_panel, mut cx) = open_panel(cx, Uuid::new_v4(), DatabaseDialect::Postgres);

    cx.debug_bounds("tab:general")
        .expect("the General tab should be painted");
    cx.debug_bounds("tab:body")
        .expect("the Function Body tab should be painted");
    cx.debug_bounds("tab:sql-preview")
        .expect("the SQL Preview tab should be painted");

    // The General tab is selected by default; the other tabs' content should
    // not be painted until selected.
    assert!(
        cx.debug_bounds("panel:general-tab").is_some(),
        "the general tab's fields should be painted by default"
    );
    assert!(
        cx.debug_bounds("panel:body-tab").is_none(),
        "the body tab's content should not paint until selected"
    );
    assert!(
        cx.debug_bounds("panel:sql-preview-tab").is_none(),
        "the SQL preview tab's content should not paint until selected"
    );

    cx.debug_bounds("checkbox:event-insert")
        .expect("the INSERT event checkbox should be painted");
    cx.debug_bounds("button:cancel")
        .expect("the footer's Cancel action should be painted");
    cx.debug_bounds("button:save")
        .expect("the footer's Save action should be painted");
}

#[gpui::test]
fn switching_tabs_generates_a_ddl_preview_and_restores_the_general_tab(cx: &mut TestAppContext) {
    init_test(cx);
    let (panel, mut cx) = open_panel(cx, Uuid::new_v4(), DatabaseDialect::Postgres);

    click(&mut cx, "tab:sql-preview");
    panel.read_with(&cx, |panel, _cx| {
        assert_eq!(panel.active_tab, DesignerTab::SqlPreview);
        assert!(
            panel.ddl_preview.is_some(),
            "selecting the SQL preview tab should generate a preview"
        );
    });
    assert!(
        cx.debug_bounds("panel:sql-preview-tab").is_some(),
        "the SQL preview tab's content should paint once selected"
    );
    assert!(
        cx.debug_bounds("panel:general-tab").is_none(),
        "the general tab's content should stop painting once another tab is selected"
    );

    click(&mut cx, "tab:general");
    panel.read_with(&cx, |panel, _cx| {
        assert_eq!(panel.active_tab, DesignerTab::General);
    });
    assert!(
        cx.debug_bounds("panel:general-tab").is_some(),
        "the general tab's content should paint again once reselected"
    );
}

#[gpui::test]
fn clicking_event_checkboxes_respects_the_dialects_multi_event_support(cx: &mut TestAppContext) {
    init_test(cx);

    // Postgres allows several events to fire the same trigger, so toggling one
    // event checkbox must leave the others untouched.
    let (multi_panel, mut multi_cx) = open_panel(cx, Uuid::new_v4(), DatabaseDialect::Postgres);
    click(&mut multi_cx, "checkbox:event-update");
    multi_panel.read_with(&multi_cx, |panel, _cx| {
        assert!(
            panel.is_dirty,
            "toggling an event should mark the panel dirty"
        );
        assert!(
            panel.insert_checked,
            "multi-event dialects should keep other events checked"
        );
        assert!(panel.update_checked);
    });
    click(&mut multi_cx, "checkbox:event-insert");
    multi_panel.read_with(&multi_cx, |panel, _cx| {
        assert!(
            !panel.insert_checked,
            "unchecking one event should not affect a different event"
        );
        assert!(panel.update_checked);
    });

    // SQLite only fires on a single event, so clicking another event checkbox
    // should radio-select it instead of adding to the set.
    let (single_panel, mut single_cx) = open_panel(cx, Uuid::new_v4(), DatabaseDialect::Sqlite);
    click(&mut single_cx, "checkbox:event-update");
    single_panel.read_with(&single_cx, |panel, _cx| {
        assert!(
            !panel.insert_checked,
            "single-event dialects should clear the previous selection"
        );
        assert!(panel.update_checked);
        assert!(!panel.delete_checked);
    });
}
