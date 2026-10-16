//! Construction and window-level tests for [`SequenceDesignerPanel`].
//!
//! The construction test pins the derived state the two entry points
//! (`new`/`edit`) are responsible for computing. The window tests open a real
//! GPUI window, paint the panel, and drive it by clicking painted pixels so
//! they cover the wiring a plain state test can't: that the tab bar, general
//! fields, and footer actions actually reach the screen, and that a click on
//! them lands on the handler that mutates the panel's own state.

use gpui::{Entity, Modifiers, TestAppContext, VisualTestContext, px, size};
use uuid::Uuid;

use super::{DesignerTab, SequenceDesignerPanel};
use crate::models::SequenceDesign;

const WINDOW_SIZE: (f32, f32) = (700., 640.);

fn init_test(cx: &mut TestAppContext) {
    cx.update(zqlz_ui::init);
}

fn valid_design() -> SequenceDesign {
    let mut design = SequenceDesign::new(Some("public".to_string()));
    design.name = "orders_id_seq".to_string();
    design.owner = Some("postgres".to_string());
    design.is_new = false;
    design
}

/// Open a window showing an editing panel for `design` and paint it.
fn open_panel(
    cx: &mut TestAppContext,
    connection_id: Uuid,
    design: SequenceDesign,
) -> (Entity<SequenceDesignerPanel>, VisualTestContext) {
    let window =
        cx.add_window(|window, cx| SequenceDesignerPanel::edit(connection_id, design, window, cx));
    let entity = window
        .root(cx)
        .expect("sequence designer panel should be the window root");
    let visual_cx = VisualTestContext::from_window(window.into(), cx);
    visual_cx.simulate_resize(size(px(WINDOW_SIZE.0), px(WINDOW_SIZE.1)));
    visual_cx.run_until_parked();
    (entity, visual_cx)
}

fn click(cx: &mut VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("{selector:?} should be painted"));
    cx.simulate_click(bounds.center(), Modifiers::none());
    cx.run_until_parked();
}

#[gpui::test]
fn constructing_new_and_edit_panels_derives_the_expected_state(cx: &mut TestAppContext) {
    init_test(cx);
    let connection_id = Uuid::new_v4();

    let window = cx.add_window(|window, cx| {
        SequenceDesignerPanel::new(connection_id, Some("public".to_string()), window, cx)
    });
    let panel = window
        .root(cx)
        .expect("sequence designer panel should be the window root");
    panel.read_with(cx, |panel, cx| {
        assert_eq!(panel.connection_id, connection_id);
        assert!(panel.original_name.is_none());
        assert_eq!(panel.active_tab, DesignerTab::General);
        assert!(!panel.is_dirty);
        assert!(panel.design.is_new);
        // A fresh design starts with an explicit min/max, so the "no
        // minimum/maximum" checkboxes should not be pre-checked.
        assert!(!panel.no_min_value);
        assert!(!panel.no_max_value);
        assert!(!panel.cycle);
        assert_eq!(panel.name_input.read(cx).value().as_ref(), "");
        assert_eq!(panel.data_type_input.read(cx).value().as_ref(), "bigint");
    });

    let mut design = valid_design();
    design.min_value = None;
    design.max_value = None;
    design.cycle = true;
    let window = cx.add_window(|window, cx| {
        SequenceDesignerPanel::edit(connection_id, design.clone(), window, cx)
    });
    let panel = window
        .root(cx)
        .expect("sequence designer panel should be the window root");
    panel.read_with(cx, |panel, cx| {
        assert_eq!(panel.original_name.as_deref(), Some("orders_id_seq"));
        assert!(!panel.design.is_new);
        // The "no minimum/maximum" checkboxes are derived from whether the
        // design already carries a bound, not passed in explicitly.
        assert!(panel.no_min_value);
        assert!(panel.no_max_value);
        assert!(panel.cycle);
        assert_eq!(panel.name_input.read(cx).value().as_ref(), "orders_id_seq");
        assert_eq!(panel.owner_input.read(cx).value().as_ref(), "postgres");
    });
}

#[gpui::test]
fn panel_paints_the_tab_bar_general_fields_and_footer_actions(cx: &mut TestAppContext) {
    init_test(cx);
    let (_panel, mut cx) = open_panel(cx, Uuid::new_v4(), valid_design());

    cx.debug_bounds("tab:general")
        .expect("the General tab should be painted");
    cx.debug_bounds("tab:comment")
        .expect("the Comment tab should be painted");
    cx.debug_bounds("tab:sql-preview")
        .expect("the SQL Preview tab should be painted");

    // The General tab is selected by default; the other tabs' content should
    // not be painted until selected.
    assert!(
        cx.debug_bounds("panel:general-tab").is_some(),
        "the general tab's fields should be painted by default"
    );
    assert!(
        cx.debug_bounds("panel:comment-tab").is_none(),
        "the comment tab's content should not paint until selected"
    );
    assert!(
        cx.debug_bounds("panel:sql-preview-tab").is_none(),
        "the SQL preview tab's content should not paint until selected"
    );

    cx.debug_bounds("button:cancel")
        .expect("the footer's Cancel action should be painted");
    cx.debug_bounds("button:save")
        .expect("the footer's Save action should be painted");
}

#[gpui::test]
fn switching_tabs_and_toggling_checkboxes_updates_panel_state(cx: &mut TestAppContext) {
    init_test(cx);
    let (panel, mut cx) = open_panel(cx, Uuid::new_v4(), valid_design());

    panel.read_with(&cx, |panel, _cx| {
        assert_eq!(panel.active_tab, DesignerTab::General);
        assert!(!panel.is_dirty);
        assert!(!panel.cycle);
        assert!(!panel.no_min_value);
    });

    click(&mut cx, "checkbox:cycle");
    click(&mut cx, "checkbox:no-min-value");
    panel.read_with(&cx, |panel, _cx| {
        assert!(panel.cycle, "clicking the cycle checkbox should toggle it");
        assert!(
            panel.no_min_value,
            "clicking the no-minimum checkbox should toggle it"
        );
        assert!(
            panel.is_dirty,
            "toggling a checkbox should mark the panel dirty"
        );
    });

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
