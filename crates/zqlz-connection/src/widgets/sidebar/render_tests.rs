//! Window-level tests for the sidebar tree.
//!
//! The golden tests in `sidebar.rs` pin the row model without rendering it.
//! These open a real GPUI window, paint the tree, and drive it by clicking
//! painted pixels — so they cover the wiring the row model can't: that rows
//! actually lay out, that a click lands on the row it looks like it lands on,
//! and that the handler behind it mutates the scope the row was rendered from.

use gpui::{Bounds, Modifiers, Pixels, TestAppContext, VisualTestContext, px, size};
use uuid::Uuid;

use crate::widgets::sidebar::ConnectionSidebar;
use crate::widgets::sidebar::types::{ConnectionEntry, DatabaseSchemaData, SidebarDatabaseInfo};

/// Wide enough that row labels are not clipped, tall enough that the fixtures
/// below fit without the uniform list virtualizing any of them away.
const WINDOW_SIZE: (f32, f32) = (320., 800.);

fn init_test(cx: &mut TestAppContext) {
    cx.update(zqlz_ui::init);
}

fn connection(driver: &str) -> ConnectionEntry {
    let mut connection = ConnectionEntry::new(
        Uuid::new_v4(),
        format!("{driver}-conn"),
        driver.to_string(),
    );
    connection.is_connected = true;
    connection.is_expanded = true;
    connection
}

fn database(name: &str, schema: Option<DatabaseSchemaData>) -> SidebarDatabaseInfo {
    SidebarDatabaseInfo {
        name: name.to_string(),
        size_bytes: None,
        is_active: true,
        is_expanded: true,
        is_loading: false,
        schema,
        collections: Vec::new(),
        indexes: Vec::new(),
        functions: Vec::new(),
        gridfs_buckets: Vec::new(),
        users: Vec::new(),
        roles: Vec::new(),
        search_indexes: Vec::new(),
        vector_indexes: Vec::new(),
        server: Vec::new(),
        sharding: Vec::new(),
        collections_expanded: false,
        collections_loading: false,
        document_expanded_sections: std::collections::HashSet::new(),
    }
}

/// Open a window showing `connection` and paint it.
fn open_sidebar(
    cx: &mut TestAppContext,
    connection: ConnectionEntry,
) -> (gpui::Entity<ConnectionSidebar>, VisualTestContext) {
    let window = cx.add_window(|_window, cx| {
        let mut sidebar = ConnectionSidebar::new(cx);
        sidebar.selected_connection = Some(connection.id);
        sidebar.connections.push(connection);
        sidebar
    });
    let entity = window
        .root(cx)
        .expect("sidebar view should be the window root");
    let visual_cx = VisualTestContext::from_window(window.into(), cx);
    visual_cx.simulate_resize(size(px(WINDOW_SIZE.0), px(WINDOW_SIZE.1)));
    visual_cx.run_until_parked();
    (entity, visual_cx)
}

/// Bounds of a painted row, by the `debug_selector` its renderer sets.
fn row_bounds(cx: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
    cx.debug_bounds(selector)
        .unwrap_or_else(|| panic!("row {selector:?} should be painted"))
}

fn click_row(cx: &mut VisualTestContext, selector: &'static str) {
    let bounds = row_bounds(cx, selector);
    cx.simulate_click(bounds.center(), Modifiers::none());
    cx.run_until_parked();
}

fn mysql_connection_with_tables() -> ConnectionEntry {
    let mut connection = connection("mysql");
    connection.databases = vec![database(
        "art-of-protest",
        Some(DatabaseSchemaData {
            // MySQL reports the database as its own schema, which is what used
            // to make the tree grow a folder repeating the database name.
            schema_name: Some("art-of-protest".to_string()),
            schema_names: vec!["art-of-protest".to_string()],
            tables: vec!["orders".to_string(), "users".to_string()],
            views: vec!["recent_orders".to_string()],
            ..DatabaseSchemaData::default()
        }),
    )];
    connection
}

#[gpui::test]
fn mysql_database_paints_sections_directly_under_the_database(cx: &mut TestAppContext) {
    init_test(cx);
    let (_sidebar, mut cx) = open_sidebar(cx, mysql_connection_with_tables());

    let database = row_bounds(&mut cx, "database:art-of-protest");
    let tables = row_bounds(&mut cx, "section:Tables");

    // The reported bug painted a folder repeating the database name between
    // these two rows; the sections must sit directly beneath the database.
    assert!(
        cx.debug_bounds("section:art-of-protest").is_none(),
        "a MySQL database must not paint a schema folder repeating its own name"
    );
    assert!(
        cx.debug_bounds("schema-node:art-of-protest").is_none(),
        "a MySQL database must not paint a schema node repeating its own name"
    );
    // Indentation is padding inside a full-width row, so depth is not visible
    // in row bounds; the golden row-model tests assert it. What geometry can
    // see is the vertical order.
    assert!(
        tables.origin.y > database.origin.y,
        "Tables should paint directly below the database row"
    );
    assert_eq!(
        tables.origin.y - database.origin.y,
        database.size.height,
        "nothing should paint between the database row and its first section"
    );
}

#[gpui::test]
fn clicking_a_section_header_expands_it_in_place(cx: &mut TestAppContext) {
    init_test(cx);
    let (sidebar, mut cx) = open_sidebar(cx, mysql_connection_with_tables());

    assert!(
        cx.debug_bounds("leaf:users").is_none(),
        "tables start collapsed"
    );

    click_row(&mut cx, "section:Tables");

    let users = row_bounds(&mut cx, "leaf:users");
    let tables = row_bounds(&mut cx, "section:Tables");
    assert!(
        users.origin.y > tables.origin.y,
        "table leaves should paint below their section header"
    );
    assert!(
        row_bounds(&mut cx, "section:Views").origin.y > users.origin.y,
        "the next section should paint after this section's leaves"
    );

    // The click must have landed on the database's own schema state, not on a
    // connection-level copy that nothing renders.
    sidebar.update(&mut cx, |sidebar, _cx| {
        let connection = &sidebar.connections[0];
        assert!(
            connection.databases[0]
                .schema
                .as_ref()
                .expect("the database owns its schema")
                .expanded_sections
                .contains(&super::types::SidebarSection::Tables)
        );
    });

    click_row(&mut cx, "section:Tables");
    assert!(
        cx.debug_bounds("leaf:users").is_none(),
        "clicking the header again should collapse it"
    );
}

#[gpui::test]
fn clicking_a_database_row_collapses_its_subtree(cx: &mut TestAppContext) {
    init_test(cx);
    let (_sidebar, mut cx) = open_sidebar(cx, mysql_connection_with_tables());

    row_bounds(&mut cx, "section:Tables");
    click_row(&mut cx, "database:art-of-protest");

    assert!(
        cx.debug_bounds("section:Tables").is_none(),
        "collapsing the database should hide its sections"
    );
    row_bounds(&mut cx, "database:art-of-protest");
}

#[gpui::test]
fn postgres_paints_schema_folders_above_their_sections(cx: &mut TestAppContext) {
    init_test(cx);

    let mut connection = connection("postgres");
    connection.databases = vec![database(
        "app",
        Some(DatabaseSchemaData {
            schema_name: Some("public".to_string()),
            schema_names: vec!["audit".to_string(), "public".to_string()],
            tables: vec!["public.users".to_string(), "audit.events".to_string()],
            ..DatabaseSchemaData::default()
        }),
    )];
    let (_sidebar, mut cx) = open_sidebar(cx, connection);

    let audit = row_bounds(&mut cx, "section:audit");
    let public = row_bounds(&mut cx, "section:public");
    assert!(
        audit.origin.y < public.origin.y,
        "schema folders should paint in name order"
    );

    // Expanding one schema folder must not expand the other: the section rows
    // inside are keyed per schema, which is what the shared element ids used to
    // get wrong.
    click_row(&mut cx, "section:public");
    let tables = row_bounds(&mut cx, "section:Tables");
    assert!(
        tables.origin.y > public.origin.y,
        "public's sections should paint below the public folder"
    );
    assert_eq!(
        row_bounds(&mut cx, "section:audit").origin.y,
        audit.origin.y,
        "expanding public must not move or expand the audit folder"
    );
}
