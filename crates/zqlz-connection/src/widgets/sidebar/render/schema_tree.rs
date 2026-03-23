//! SQL schema tree rendering
//!
//! Handles rendering of SQL database schema trees with support for both
//! single-database and multi-database modes.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::BTreeMap;
use std::collections::HashSet;

use super::SqlSchemaTreeProps;
use crate::widgets::sidebar::ConnectionSidebar;
use zqlz_ui::widgets::{caption, h_flex, v_flex, ActiveTheme, Icon, IconName, ZqlzIcon};

#[derive(Default)]
struct SchemaSectionGroup {
    tables: Vec<String>,
    views: Vec<String>,
    materialized_views: Vec<String>,
    triggers: Vec<String>,
    functions: Vec<String>,
    procedures: Vec<String>,
}

impl ConnectionSidebar {
    fn split_schema_qualified_name(name: &str) -> Option<(&str, &str)> {
        let (schema_name, object_name) = name.split_once('.')?;
        if schema_name.is_empty() || object_name.is_empty() {
            return None;
        }

        Some((schema_name, object_name))
    }

    #[allow(clippy::too_many_arguments)]
    fn group_schema_sections(
        tables: &[String],
        views: &[String],
        materialized_views: &[String],
        triggers: &[String],
        functions: &[String],
        procedures: &[String],
        schema_names: &[String],
        fallback_schema_name: Option<&str>,
    ) -> Option<Vec<(String, SchemaSectionGroup)>> {
        let mut groups: BTreeMap<String, SchemaSectionGroup> = BTreeMap::new();
        let mut saw_schema_qualified_name = false;

        let fallback_schema = fallback_schema_name.unwrap_or("public").to_string();

        for schema_name in schema_names {
            groups.entry(schema_name.clone()).or_default();
        }

        for table_name in tables {
            if let Some((schema_name, object_name)) = Self::split_schema_qualified_name(table_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .tables
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .tables
                    .push(table_name.clone());
            }
        }

        for view_name in views {
            if let Some((schema_name, object_name)) = Self::split_schema_qualified_name(view_name) {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .views
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .views
                    .push(view_name.clone());
            }
        }

        for view_name in materialized_views {
            if let Some((schema_name, object_name)) = Self::split_schema_qualified_name(view_name) {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .materialized_views
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .materialized_views
                    .push(view_name.clone());
            }
        }

        for trigger_name in triggers {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name(trigger_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .triggers
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .triggers
                    .push(trigger_name.clone());
            }
        }

        for function_name in functions {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name(function_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .functions
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .functions
                    .push(function_name.clone());
            }
        }

        for procedure_name in procedures {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name(procedure_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .procedures
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .procedures
                    .push(procedure_name.clone());
            }
        }

        if !saw_schema_qualified_name {
            return None;
        }

        Some(groups.into_iter().collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn render_grouped_schema_objects_tree(
        &self,
        conn_id: uuid::Uuid,
        object_capabilities: crate::widgets::sidebar::SidebarObjectCapabilities,
        id_suffix: &str,
        database_name: Option<String>,
        groups: &[(String, SchemaSectionGroup)],
        queries: &[crate::widgets::sidebar::SavedQueryInfo],
        queries_expanded: bool,
        tables_loading: bool,
        views_loading: bool,
        materialized_views_loading: bool,
        triggers_loading: bool,
        functions_loading: bool,
        procedures_loading: bool,
        expanded_schema_groups: &HashSet<String>,
        expanded_schema_section_keys: &HashSet<String>,
        depth: usize,
        toggle_schema_group: impl Fn(&mut Self, &str, &mut Context<Self>) + Clone + 'static,
        toggle_schema_section: impl Fn(&mut Self, &str, &str, &mut Context<Self>) + Clone + 'static,
        cx: &mut Context<Self>,
    ) -> Div {
        let muted_foreground = cx.theme().muted_foreground;
        let list_hover = cx.theme().list_hover;
        let font_family = cx.theme().font_family.clone();
        let has_search = !self.search_query.is_empty();
        let leaf_depth = depth + 2;

        let mut tree = v_flex().w_full().gap_px().font_family(font_family);

        for (schema_name, group) in groups {
            let filtered_tables = self.filter_by_search(&group.tables);
            let filtered_views = self.filter_by_search(&group.views);
            let filtered_materialized_views = self.filter_by_search(&group.materialized_views);
            let filtered_triggers = self.filter_by_search(&group.triggers);
            let filtered_functions = self.filter_by_search(&group.functions);
            let filtered_procedures = self.filter_by_search(&group.procedures);
            let schema_has_matches = self.matches_search(schema_name)
                || !filtered_tables.is_empty()
                || !filtered_views.is_empty()
                || !filtered_materialized_views.is_empty()
                || !filtered_triggers.is_empty()
                || !filtered_functions.is_empty()
                || !filtered_procedures.is_empty();

            if has_search && !schema_has_matches {
                continue;
            }

            let schema_name_for_id = schema_name.clone();
            let schema_name_for_toggle = schema_name.clone();
            let schema_name_for_table_menu = schema_name.clone();
            let schema_name_for_view_menu = schema_name.clone();
            let schema_name_for_trigger_click = schema_name.clone();
            let schema_name_for_trigger_menu = schema_name.clone();
            let schema_name_for_function_click = schema_name.clone();
            let schema_name_for_function_menu = schema_name.clone();
            let schema_name_for_procedure_click = schema_name.clone();
            let schema_name_for_procedure_menu = schema_name.clone();

            let schema_is_expanded =
                expanded_schema_groups.contains(schema_name) || (has_search && schema_has_matches);
            let schema_total_count = group.tables.len()
                + group.views.len()
                + group.materialized_views.len()
                + group.triggers.len()
                + group.functions.len()
                + group.procedures.len();
            let schema_filtered_count = filtered_tables.len()
                + filtered_views.len()
                + filtered_materialized_views.len()
                + filtered_triggers.len()
                + filtered_functions.len()
                + filtered_procedures.len();

            let schema_header = self.render_section_header(
                super::SectionHeaderProps {
                    element_id: SharedString::from(format!(
                        "schema-group-{}-{}",
                        id_suffix, schema_name_for_id
                    )),
                    icon: Icon::new(IconName::Folder).size_3().into_any_element(),
                    label: schema_name,
                    total_count: schema_total_count,
                    filtered_count: schema_filtered_count,
                    is_expanded: schema_is_expanded,
                    on_click: {
                        let toggle_schema_group = toggle_schema_group.clone();
                        move |this: &mut Self,
                              _: &ClickEvent,
                              _: &mut Window,
                              cx: &mut Context<Self>| {
                            toggle_schema_group(this, &schema_name_for_toggle, cx);
                        }
                    },
                    on_right_click: None::<
                        fn(&mut Self, &MouseDownEvent, &mut Window, &mut Context<Self>),
                    >,
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut schema_tree = v_flex().w_full().child(schema_header);

            if schema_is_expanded {
                // Tables
                let tables_section_key = format!("{}::tables", schema_name);
                let tables_expanded = expanded_schema_section_keys.contains(&tables_section_key)
                    || (has_search && !filtered_tables.is_empty());

                if !has_search || tables_loading || !filtered_tables.is_empty() {
                    let tables_header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "tables-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::Table).size_3().into_any_element(),
                            label: "Tables",
                            total_count: group.tables.len(),
                            filtered_count: filtered_tables.len(),
                            is_expanded: tables_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "tables",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "tables",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(tables_header);
                    if tables_expanded {
                        if tables_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("tables-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for table_name in &filtered_tables {
                                let table = format!("{}.{}", schema_name, table_name);
                                let name_for_menu = (*table_name).clone();
                                let db_name_for_click = database_name.clone();
                                let db_name_for_menu = database_name.clone();
                                let object_schema_for_menu =
                                    Some(schema_name_for_table_menu.clone());
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "table-{}-{}-{}",
                                            id_suffix, schema_name, table_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::Table)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*table_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenTable {
                                                connection_id: conn_id,
                                                table_name: table.clone(),
                                                database_name: db_name_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_table_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    object_schema_for_menu.clone(),
                                                    db_name_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }

                // Views
                let views_section_key = format!("{}::views", schema_name);
                let views_expanded = expanded_schema_section_keys.contains(&views_section_key)
                    || (has_search && !filtered_views.is_empty());

                if object_capabilities.supports_views
                    && (!has_search || views_loading || !filtered_views.is_empty())
                {
                    let views_header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "views-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::Eye).size_3().into_any_element(),
                            label: "Views",
                            total_count: group.views.len(),
                            filtered_count: filtered_views.len(),
                            is_expanded: views_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "views",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "views",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(views_header);
                    if views_expanded {
                        if views_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("views-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for view_name in &filtered_views {
                                let view = format!("{}.{}", schema_name, view_name);
                                let name_for_menu = (*view_name).clone();
                                let db_name_for_click = database_name.clone();
                                let db_name_for_menu = database_name.clone();
                                let object_schema_for_menu =
                                    Some(schema_name_for_view_menu.clone());
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "view-{}-{}-{}",
                                            id_suffix, schema_name, view_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::Eye)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*view_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenView {
                                                connection_id: conn_id,
                                                view_name: view.clone(),
                                                database_name: db_name_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_view_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    object_schema_for_menu.clone(),
                                                    db_name_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }

                // Materialized views
                let mat_views_section_key = format!("{}::materialized_views", schema_name);
                let mat_views_expanded = expanded_schema_section_keys
                    .contains(&mat_views_section_key)
                    || (has_search && !filtered_materialized_views.is_empty());

                if object_capabilities.supports_materialized_views
                    && (!has_search
                        || materialized_views_loading
                        || !filtered_materialized_views.is_empty())
                {
                    let header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "matviews-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::TreeStructure)
                                .size_3()
                                .into_any_element(),
                            label: "Materialized Views",
                            total_count: group.materialized_views.len(),
                            filtered_count: filtered_materialized_views.len(),
                            is_expanded: mat_views_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "materialized_views",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "materialized_views",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(header);
                    if mat_views_expanded {
                        if materialized_views_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("matviews-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for view_name in &filtered_materialized_views {
                                let view = format!("{}.{}", schema_name, view_name);
                                let name_for_menu = (*view_name).clone();
                                let db_name_for_click = database_name.clone();
                                let db_name_for_menu = database_name.clone();
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "matview-{}-{}-{}",
                                            id_suffix, schema_name, view_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::TreeStructure)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*view_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenView {
                                                connection_id: conn_id,
                                                view_name: view.clone(),
                                                database_name: db_name_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_materialized_view_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    db_name_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }

                // Triggers
                let triggers_section_key = format!("{}::triggers", schema_name);
                let triggers_expanded = expanded_schema_section_keys
                    .contains(&triggers_section_key)
                    || (has_search && !filtered_triggers.is_empty());

                if object_capabilities.supports_triggers
                    && (!has_search || triggers_loading || !filtered_triggers.is_empty())
                {
                    let header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "triggers-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::LightningBolt)
                                .size_3()
                                .into_any_element(),
                            label: "Triggers",
                            total_count: group.triggers.len(),
                            filtered_count: filtered_triggers.len(),
                            is_expanded: triggers_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "triggers",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "triggers",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(header);
                    if triggers_expanded {
                        if triggers_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("triggers-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for trigger_name in &filtered_triggers {
                                let trigger = (*trigger_name).clone();
                                let name_for_menu = (*trigger_name).clone();
                                let object_schema_for_click =
                                    Some(schema_name_for_trigger_click.clone());
                                let object_schema_for_menu =
                                    Some(schema_name_for_trigger_menu.clone());
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "trigger-{}-{}-{}",
                                            id_suffix, schema_name, trigger_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::LightningBolt)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*trigger_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::DesignTrigger {
                                                connection_id: conn_id,
                                                trigger_name: trigger.clone(),
                                                object_schema: object_schema_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_trigger_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    object_schema_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }

                // Functions
                let functions_section_key = format!("{}::functions", schema_name);
                let functions_expanded = expanded_schema_section_keys
                    .contains(&functions_section_key)
                    || (has_search && !filtered_functions.is_empty());

                if object_capabilities.supports_functions
                    && (!has_search || functions_loading || !filtered_functions.is_empty())
                {
                    let header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "functions-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::Function).size_3().into_any_element(),
                            label: "Functions",
                            total_count: group.functions.len(),
                            filtered_count: filtered_functions.len(),
                            is_expanded: functions_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "functions",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "functions",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(header);
                    if functions_expanded {
                        if functions_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("functions-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for function_name in &filtered_functions {
                                let function = (*function_name).clone();
                                let name_for_menu = (*function_name).clone();
                                let object_schema_for_click =
                                    Some(schema_name_for_function_click.clone());
                                let object_schema_for_menu =
                                    Some(schema_name_for_function_menu.clone());
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "function-{}-{}-{}",
                                            id_suffix, schema_name, function_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::Function)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*function_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenFunction {
                                                connection_id: conn_id,
                                                function_name: function.clone(),
                                                object_schema: object_schema_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_function_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    object_schema_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }

                // Procedures
                let procedures_section_key = format!("{}::procedures", schema_name);
                let procedures_expanded = expanded_schema_section_keys
                    .contains(&procedures_section_key)
                    || (has_search && !filtered_procedures.is_empty());

                if object_capabilities.supports_procedures
                    && (!has_search || procedures_loading || !filtered_procedures.is_empty())
                {
                    let header = self.render_section_header(
                        super::SectionHeaderProps {
                            element_id: SharedString::from(format!(
                                "procedures-header-{}-{}",
                                id_suffix, schema_name
                            )),
                            icon: Icon::new(ZqlzIcon::Gear).size_3().into_any_element(),
                            label: "Procedures",
                            total_count: group.procedures.len(),
                            filtered_count: filtered_procedures.len(),
                            is_expanded: procedures_expanded,
                            on_click: {
                                let toggle_schema_section = toggle_schema_section.clone();
                                let schema_name_for_section = schema_name.clone();
                                move |this: &mut Self,
                                      _: &ClickEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    toggle_schema_section(
                                        this,
                                        &schema_name_for_section,
                                        "procedures",
                                        cx,
                                    );
                                }
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_section_context_menu(
                                        conn_id,
                                        "procedures",
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            muted_foreground,
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    );

                    let mut section = v_flex().w_full().child(header);
                    if procedures_expanded {
                        if procedures_loading {
                            section = section.child(Self::render_loading_row(
                                id_suffix,
                                &format!("procedures-{schema_name}"),
                                muted_foreground,
                                leaf_depth,
                            ));
                        } else {
                            for procedure_name in &filtered_procedures {
                                let procedure = (*procedure_name).clone();
                                let name_for_menu = (*procedure_name).clone();
                                let object_schema_for_click =
                                    Some(schema_name_for_procedure_click.clone());
                                let object_schema_for_menu =
                                    Some(schema_name_for_procedure_menu.clone());
                                section = section.child(self.render_leaf_item(
                                    super::LeafItemProps {
                                        element_id: SharedString::from(format!(
                                            "procedure-{}-{}-{}",
                                            id_suffix, schema_name, procedure_name
                                        )),
                                        icon: Icon::new(ZqlzIcon::Gear)
                                            .size_3()
                                            .text_color(muted_foreground)
                                            .into_any_element(),
                                        label: (*procedure_name).clone(),
                                        on_click: move |_this: &mut Self,
                                                        _: &ClickEvent,
                                                        _: &mut Window,
                                                        cx: &mut Context<Self>| {
                                            cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenProcedure {
                                                connection_id: conn_id,
                                                procedure_name: procedure.clone(),
                                                object_schema: object_schema_for_click.clone(),
                                            });
                                        },
                                        on_right_click: Some(
                                            move |this: &mut Self,
                                                  event: &MouseDownEvent,
                                                  window: &mut Window,
                                                  cx: &mut Context<Self>| {
                                                this.show_procedure_context_menu(
                                                    conn_id,
                                                    name_for_menu.clone(),
                                                    object_schema_for_menu.clone(),
                                                    event.position,
                                                    window,
                                                    cx,
                                                );
                                            },
                                        ),
                                        list_hover,
                                        depth: leaf_depth,
                                    },
                                    cx,
                                ));
                            }
                        }
                    }
                    schema_tree = schema_tree.child(section);
                }
            }

            tree = tree.child(schema_tree);
        }

        let filtered_queries: Vec<_> = queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();
        let queries_expanded = queries_expanded || (has_search && !filtered_queries.is_empty());

        if !queries.is_empty() && (!has_search || !filtered_queries.is_empty()) {
            let queries_header = self.render_section_header(
                super::SectionHeaderProps {
                    element_id: SharedString::from(format!("queries-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::FileSql).size_3().into_any_element(),
                    label: "Queries",
                    total_count: queries.len(),
                    filtered_count: filtered_queries.len(),
                    is_expanded: queries_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        this.toggle_queries_expand(conn_id, cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "queries",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut queries_section = v_flex().w_full().child(queries_header);
            if queries_expanded {
                for query in &filtered_queries {
                    let query_id = query.id;
                    let query_name = query.name.clone();
                    let query_name_for_click = query.name.clone();
                    let query_name_for_menu = query.name.clone();
                    queries_section = queries_section.child(self.render_leaf_item(
                        super::LeafItemProps {
                            element_id: SharedString::from(format!(
                                "query-{}-{}",
                                id_suffix, query_id
                            )),
                            icon: Icon::new(ZqlzIcon::FileSql)
                                .size_3()
                                .text_color(muted_foreground)
                                .into_any_element(),
                            label: query_name,
                            on_click: move |_this: &mut Self,
                                            _: &ClickEvent,
                                            _: &mut Window,
                                            cx: &mut Context<Self>| {
                                tracing::info!(
                                    query_id = %query_id,
                                    query_name = %query_name_for_click,
                                    "Sidebar query item clicked"
                                );
                                cx.emit(crate::widgets::sidebar::ConnectionSidebarEvent::OpenSavedQuery {
                                    connection_id: conn_id,
                                    query_id,
                                    query_name: query_name_for_click.clone(),
                                });
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_query_context_menu(
                                        conn_id,
                                        query_id,
                                        query_name_for_menu.clone(),
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            list_hover,
                            depth: depth + 1,
                        },
                        cx,
                    ));
                }
            }

            tree = tree.child(queries_section);
        }

        tree
    }

    /// Render the schema tree for SQL database connections.
    ///
    /// This function handles both simple (single-database) and complex
    /// (multi-database) scenarios:
    ///
    /// **Single-database mode** (SQLite, single PostgreSQL database):
    /// - Renders schema objects directly under the connection
    /// - No intermediate database nodes
    ///
    /// **Multi-database mode** (PostgreSQL with multiple DBs, MySQL):
    /// - Renders a database node for each database on the server
    /// - Each database can be expanded to show its schema
    /// - Schema is loaded on-demand when user expands a database
    /// - Active database shows existing connection-level schema
    ///
    /// # Schema Node Optimization
    ///
    /// In some cases, the schema folder is redundant and skipped:
    /// - MySQL: The schema name typically matches the database name
    /// - When schema name == database name, objects render directly under database
    /// - This reduces unnecessary nesting in the tree
    ///
    /// # Parameters
    ///
    /// - `conn_id`: Connection UUID
    /// - `tables`, `views`, etc.: Connection-level schema (for active DB or single DB)
    /// - `*_expanded`: Expansion states for connection-level sections
    /// - `databases`: List of all databases on server (empty for single-DB mode)
    /// - `schema_name`: Schema name for hierarchy display (e.g., "public")
    /// - `schema_expanded`: Whether schema-level node is expanded
    /// - `_window`: Window context (unused but kept for consistency)
    /// - `cx`: App context
    ///
    /// # Returns
    ///
    /// An `AnyElement` containing the complete schema tree for this connection.
    pub(super) fn render_schema_tree(
        &self,
        props: SqlSchemaTreeProps<'_>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let SqlSchemaTreeProps {
            conn_id,
            object_capabilities,
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            queries,
            tables_expanded,
            views_expanded,
            materialized_views_expanded,
            triggers_expanded,
            functions_expanded,
            procedures_expanded,
            queries_expanded,
            tables_loading,
            views_loading,
            materialized_views_loading,
            triggers_loading,
            functions_loading,
            procedures_loading,
            databases,
            schema_name,
            schema_names,
            schema_expanded,
            collapsed_schema_groups,
            collapsed_schema_section_keys,
        } = props;
        // Simple case: no multi-database — render objects tree directly
        if databases.is_empty() {
            let objects_tree = self.render_objects_tree(
                conn_id,
                object_capabilities,
                &conn_id.to_string(),
                None,
                tables,
                views,
                materialized_views,
                triggers,
                functions,
                procedures,
                queries,
                tables_expanded,
                views_expanded,
                materialized_views_expanded,
                triggers_expanded,
                functions_expanded,
                procedures_expanded,
                queries_expanded,
                tables_loading,
                views_loading,
                materialized_views_loading,
                triggers_loading,
                functions_loading,
                procedures_loading,
                move |this: &mut ConnectionSidebar, section, cx| match section {
                    "tables" => this.toggle_tables_expand(conn_id, cx),
                    "views" => this.toggle_views_expand(conn_id, cx),
                    "materialized_views" => this.toggle_materialized_views_expand(conn_id, cx),
                    "triggers" => this.toggle_triggers_expand(conn_id, cx),
                    "functions" => this.toggle_functions_expand(conn_id, cx),
                    "procedures" => this.toggle_procedures_expand(conn_id, cx),
                    "queries" => this.toggle_queries_expand(conn_id, cx),
                    _ => {}
                },
                1,
                cx,
            );
            return objects_tree.into_any_element();
        }

        // Multi-database: build per-database tree nodes
        let muted_fg = cx.theme().muted_foreground;
        let muted_fg_half = muted_fg.opacity(0.5);
        let muted_fg_dim = muted_fg.opacity(0.4);
        let list_hover = cx.theme().list_hover;

        let mut db_nodes: Vec<AnyElement> = Vec::with_capacity(databases.len());

        // Build fallback tree for active database
        let active_db_name = databases
            .iter()
            .find(|d| d.is_active)
            .map(|d| d.name.as_str());
        let fallback_shows_schema = schema_name.is_some_and(|s| active_db_name != Some(s));
        let fallback_depth: usize = if fallback_shows_schema { 3 } else { 2 };

        let mut fallback_tree: Option<AnyElement> = Some(
            self.render_objects_tree(
                conn_id,
                object_capabilities,
                &conn_id.to_string(),
                active_db_name.map(|s| s.to_string()),
                tables,
                views,
                materialized_views,
                triggers,
                functions,
                procedures,
                queries,
                tables_expanded,
                views_expanded,
                materialized_views_expanded,
                triggers_expanded,
                functions_expanded,
                procedures_expanded,
                queries_expanded,
                tables_loading,
                views_loading,
                materialized_views_loading,
                triggers_loading,
                functions_loading,
                procedures_loading,
                move |this: &mut ConnectionSidebar, section, cx| match section {
                    "tables" => this.toggle_tables_expand(conn_id, cx),
                    "views" => this.toggle_views_expand(conn_id, cx),
                    "materialized_views" => this.toggle_materialized_views_expand(conn_id, cx),
                    "triggers" => this.toggle_triggers_expand(conn_id, cx),
                    "functions" => this.toggle_functions_expand(conn_id, cx),
                    "procedures" => this.toggle_procedures_expand(conn_id, cx),
                    "queries" => this.toggle_queries_expand(conn_id, cx),
                    _ => {}
                },
                fallback_depth,
                cx,
            )
            .into_any_element(),
        );

        for db in databases {
            let db_name = db.name.clone();
            let db_name_click = db.name.clone();
            let is_expanded = db.is_expanded;
            let has_schema = db.schema.is_some();
            let is_active_database = db.is_active;
            let size_label = db.size_bytes.map(Self::format_database_size);

            let mut node = v_flex().w_full();

            // Database row
            let db_row = h_flex()
                .id(SharedString::from(format!(
                    "db-node-{}-{}",
                    conn_id, &db_name
                )))
                .w_full()
                .pl(px(20.0))
                .pr_2()
                .h(px(24.0))
                .gap_1p5()
                .items_center()
                .text_xs()
                .text_color(if is_expanded || is_active_database {
                    muted_fg
                } else {
                    muted_fg_half
                })
                .cursor_pointer()
                .hover(|el| el.bg(list_hover))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.toggle_database_expand(conn_id, &db_name_click, cx);
                }))
                .child(
                    Icon::new(if is_expanded {
                        IconName::ChevronDown
                    } else {
                        IconName::ChevronRight
                    })
                    .size_3(),
                )
                .child(
                    Icon::new(ZqlzIcon::Database)
                        .size_3()
                        .when(!has_schema && !is_expanded && !is_active_database, |el| {
                            el.text_color(muted_fg_half)
                        }),
                )
                .child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .text_ellipsis()
                        .whitespace_nowrap()
                        .child(db_name.clone()),
                )
                .when_some(size_label, |el, size| {
                    el.child(caption(size).color(muted_fg_dim).flex_shrink_0())
                });

            node = node.child(db_row);

            if is_expanded {
                if has_schema || db.is_active {
                    let db_schema = db.schema.as_ref();
                    let has_database_schema = db_schema.is_some();
                    let sch_name = db_schema
                        .and_then(|s| s.schema_name.clone())
                        .or_else(|| schema_name.map(|s| s.to_string()));
                    let sch_expanded = db_schema.map_or(schema_expanded, |s| s.schema_expanded);
                    let db_name_for_toggle = db.name.clone();
                    let mut show_schema_node = sch_name.as_ref().is_some_and(|s| s != &db_name);
                    let grouped_objects_depth: usize = 2;
                    let non_grouped_objects_depth: usize = if show_schema_node { 3 } else { 2 };
                    let mut tree_is_grouped = false;

                    let tree: Option<AnyElement> = if let Some(schema) = db_schema {
                        let grouped_schema_sections = Self::group_schema_sections(
                            &schema.tables,
                            &schema.views,
                            &schema.materialized_views,
                            &schema.triggers,
                            &schema.functions,
                            &schema.procedures,
                            &schema.schema_names,
                            schema.schema_name.as_deref(),
                        );

                        if let Some(groups) = grouped_schema_sections {
                            tree_is_grouped = true;
                            let db_name_for_group_toggle = db.name.clone();
                            let db_name_for_section_toggle = db.name.clone();
                            Some(
                                self.render_grouped_schema_objects_tree(
                                    conn_id,
                                    object_capabilities,
                                    &format!("{}-{}", conn_id, db.name),
                                    Some(db_name.clone()),
                                    &groups,
                                    queries,
                                    queries_expanded,
                                    schema.tables_loading,
                                    schema.views_loading,
                                    schema.materialized_views_loading,
                                    schema.triggers_loading,
                                    schema.functions_loading,
                                    schema.procedures_loading,
                                    &schema.collapsed_schema_groups,
                                    &schema.collapsed_schema_section_keys,
                                    grouped_objects_depth,
                                    move |this: &mut ConnectionSidebar, group_name, cx| {
                                        this.toggle_db_schema_group_expand(
                                            conn_id,
                                            &db_name_for_group_toggle,
                                            group_name,
                                            cx,
                                        );
                                    },
                                    move |this: &mut ConnectionSidebar, group_name, section, cx| {
                                        this.toggle_db_schema_section_expand(
                                            conn_id,
                                            &db_name_for_section_toggle,
                                            group_name,
                                            section,
                                            cx,
                                        );
                                    },
                                    cx,
                                )
                                .into_any_element(),
                            )
                        } else {
                            let db_name_for_closure = db.name.clone();
                            Some(
                                self.render_objects_tree(
                                    conn_id,
                                    object_capabilities,
                                    &format!("{}-{}", conn_id, db.name),
                                    Some(db_name.clone()),
                                    &schema.tables,
                                    &schema.views,
                                    &schema.materialized_views,
                                    &schema.triggers,
                                    &schema.functions,
                                    &schema.procedures,
                                    queries,
                                    schema.tables_expanded,
                                    schema.views_expanded,
                                    schema.materialized_views_expanded,
                                    schema.triggers_expanded,
                                    schema.functions_expanded,
                                    schema.procedures_expanded,
                                    queries_expanded,
                                    schema.tables_loading,
                                    schema.views_loading,
                                    schema.materialized_views_loading,
                                    schema.triggers_loading,
                                    schema.functions_loading,
                                    schema.procedures_loading,
                                    move |this: &mut ConnectionSidebar, section, cx| {
                                        if section == "queries" {
                                            this.toggle_queries_expand(conn_id, cx);
                                        } else {
                                            this.toggle_db_section(
                                                conn_id,
                                                &db_name_for_closure,
                                                section,
                                                cx,
                                            );
                                        }
                                    },
                                    non_grouped_objects_depth,
                                    cx,
                                )
                                .into_any_element(),
                            )
                        }
                    } else if let Some(groups) = Self::group_schema_sections(
                        tables,
                        views,
                        materialized_views,
                        triggers,
                        functions,
                        procedures,
                        schema_names,
                        schema_name,
                    ) {
                        tree_is_grouped = true;
                        Some(
                            self.render_grouped_schema_objects_tree(
                                conn_id,
                                object_capabilities,
                                &conn_id.to_string(),
                                Some(db_name.clone()),
                                &groups,
                                queries,
                                queries_expanded,
                                tables_loading,
                                views_loading,
                                materialized_views_loading,
                                triggers_loading,
                                functions_loading,
                                procedures_loading,
                                collapsed_schema_groups,
                                collapsed_schema_section_keys,
                                grouped_objects_depth,
                                move |this: &mut ConnectionSidebar, group_name, cx| {
                                    this.toggle_schema_group_expand(conn_id, group_name, cx);
                                },
                                move |this: &mut ConnectionSidebar, group_name, section, cx| {
                                    this.toggle_schema_section_expand(
                                        conn_id, group_name, section, cx,
                                    );
                                },
                                cx,
                            )
                            .into_any_element(),
                        )
                    } else {
                        fallback_tree.take()
                    };

                    if tree_is_grouped {
                        show_schema_node = false;
                    }

                    if show_schema_node {
                        node = node.child(
                            v_flex()
                                .w_full()
                                .when_some(sch_name, |el, sch| {
                                    el.child(
                                        h_flex()
                                            .id(SharedString::from(format!(
                                                "schema-node-{}-{}",
                                                conn_id, &db_name
                                            )))
                                            .w_full()
                                            .pl(px(32.0))
                                            .pr_2()
                                            .h(px(24.0))
                                            .gap_1p5()
                                            .items_center()
                                            .text_xs()
                                            .text_color(if sch_expanded {
                                                muted_fg
                                            } else {
                                                muted_fg_half
                                            })
                                            .cursor_pointer()
                                            .hover(|el| el.bg(list_hover))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                if has_database_schema {
                                                    this.toggle_db_section(
                                                        conn_id,
                                                        &db_name_for_toggle,
                                                        "schema",
                                                        cx,
                                                    );
                                                } else {
                                                    this.toggle_schema_expand(conn_id, cx);
                                                }
                                            }))
                                            .child(
                                                Icon::new(if sch_expanded {
                                                    IconName::ChevronDown
                                                } else {
                                                    IconName::ChevronRight
                                                })
                                                .size_3(),
                                            )
                                            .child(Icon::new(IconName::Folder).size_3())
                                            .child(sch),
                                    )
                                })
                                .when_some(tree, |el, objects| {
                                    el.when(sch_expanded, |el| el.child(objects))
                                }),
                        );
                    } else {
                        node = node.when_some(tree, |el, objects| el.child(objects));
                    }
                } else {
                    node = node.child(
                        h_flex()
                            .w_full()
                            .pl(px(32.0))
                            .pr_2()
                            .h(px(24.0))
                            .gap_1p5()
                            .items_center()
                            .text_xs()
                            .text_color(muted_fg_dim)
                            .child(caption("Loading schema...").color(muted_fg_dim)),
                    );
                }
            }

            db_nodes.push(node.into_any_element());
        }

        v_flex()
            .w_full()
            .gap_px()
            .children(db_nodes)
            .into_any_element()
    }

    /// Format byte count as a human-readable database size string.
    ///
    /// Converts raw byte counts into user-friendly units (B, KB, MB, GB)
    /// with appropriate decimal precision.
    ///
    /// # Examples
    ///
    /// ```
    /// format_database_size(512);           // "512 B"
    /// format_database_size(1536);          // "1.5 KB"
    /// format_database_size(1048576);       // "1.0 MB"
    /// format_database_size(5368709120);    // "5.0 GB"
    /// ```
    ///
    /// # Parameters
    ///
    /// - `bytes`: Size in bytes
    ///
    /// # Returns
    ///
    /// A formatted string with appropriate unit (B, KB, MB, or GB)
    fn format_database_size(bytes: i64) -> String {
        if bytes < 1024 {
            format!("{} B", bytes)
        } else if bytes < 1024 * 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else if bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        }
    }
}
