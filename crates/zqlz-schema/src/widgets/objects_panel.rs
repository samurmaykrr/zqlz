//! Objects panel - displays database objects using the Table component

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{
    DriverCategory, ObjectsPanelColumn, ObjectsPanelColumnAlignment, ObjectsPanelData,
    ObjectsPanelRow,
};
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{PopupMenu, PopupMenuItem},
    table::{Column, ColumnSort, Table, TableDelegate, TableEvent, TableState},
    typography::body_small,
    v_flex, ActiveTheme, Icon, IconName, Sizable, Size, ZqlzIcon,
};

/// Events emitted by the objects panel
#[derive(Clone, Debug)]
pub enum ObjectsPanelEvent {
    /// User wants to open table(s) (view data)
    OpenTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to design/edit table structure(s)
    DesignTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to create a new table
    NewTable {
        connection_id: Uuid,
        database_name: Option<String>,
    },
    /// User wants to delete table(s)
    DeleteTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to empty table(s) (DELETE FROM)
    EmptyTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to duplicate table(s)
    DuplicateTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to import data (single table only)
    ImportData {
        connection_id: Uuid,
        table_name: String,
        database_name: Option<String>,
    },
    /// User wants to export data from table(s)
    ExportTables {
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to dump table SQL (structure and optionally data)
    DumpTablesSql {
        connection_id: Uuid,
        table_names: Vec<String>,
        include_data: bool,
        database_name: Option<String>,
    },
    /// User wants to copy table name(s) to clipboard
    CopyTableNames { table_names: Vec<String> },
    /// User wants to rename a table (single table only)
    RenameTable {
        connection_id: Uuid,
        table_name: String,
        database_name: Option<String>,
    },
    /// User wants to refresh the objects list
    Refresh,
    // ============================================
    // Redis-related events
    // ============================================
    /// User wants to open a Redis database to view all keys in table viewer
    OpenRedisDatabase {
        connection_id: Uuid,
        database_index: u16,
    },
    /// User wants to delete Redis key(s)
    DeleteKeys {
        connection_id: Uuid,
        key_names: Vec<String>,
    },
    /// User wants to copy Redis key name(s) to clipboard
    CopyKeyNames { key_names: Vec<String> },
    // ============================================
    // View-related events
    // ============================================
    /// User wants to open view(s) (execute the view's query)
    OpenViews {
        connection_id: Uuid,
        view_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to design/edit view definition(s)
    DesignViews {
        connection_id: Uuid,
        view_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to create a new view
    NewView {
        connection_id: Uuid,
        database_name: Option<String>,
    },
    /// User wants to delete view(s)
    DeleteViews {
        connection_id: Uuid,
        view_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to duplicate view(s)
    DuplicateViews {
        connection_id: Uuid,
        view_names: Vec<String>,
        database_name: Option<String>,
    },
    /// User wants to copy view name(s) to clipboard
    CopyViewNames { view_names: Vec<String> },
    /// User wants to rename a view (single view only)
    RenameView {
        connection_id: Uuid,
        view_name: String,
        database_name: Option<String>,
    },
    /// User wants to view version history for a database object (single object only)
    ViewHistory {
        connection_id: Uuid,
        object_name: String,
        object_type: String, // "view", "function", "procedure", "trigger"
    },
}

/// Delegate for the objects table
pub struct ObjectsTableDelegate {
    /// Column definitions from the driver
    columns: Vec<Column>,
    /// Column IDs in order (parallel to `columns`), used for value lookups
    column_ids: Vec<String>,
    /// All objects (unfiltered)
    objects: Vec<ObjectsPanelRow>,
    /// Filtered objects (after search)
    filtered_objects: Vec<ObjectsPanelRow>,
    /// UI size (reserved for future use)
    #[allow(dead_code)]
    size: Size,
    /// Connection ID (needed for operations)
    connection_id: Option<Uuid>,
    /// Database name for MySQL multi-database browsing
    database_name: Option<String>,
    /// Weak reference back to the panel (to emit events)
    panel: WeakEntity<ObjectsPanel>,
    /// Whether we're loading
    is_loading: bool,
    /// Current driver category (determines context menu behavior)
    driver_category: DriverCategory,
    /// Cached selected row indices for context menu (populated when context menu opens)
    context_menu_selected_rows: Vec<usize>,
}

impl ObjectsTableDelegate {
    pub fn new(panel: WeakEntity<ObjectsPanel>) -> Self {
        // Start with default relational columns; will be replaced on first data load
        let default_data = ObjectsPanelData::from_table_infos(Vec::new());
        let (columns, column_ids) = Self::build_ui_columns(&default_data.columns);

        Self {
            columns,
            column_ids,
            objects: Vec::new(),
            filtered_objects: Vec::new(),
            size: Size::Small,
            connection_id: None,
            database_name: None,
            panel,
            is_loading: false,
            driver_category: DriverCategory::Relational,
            context_menu_selected_rows: Vec::new(),
        }
    }

    /// Convert driver-provided column definitions to UI table columns
    fn build_ui_columns(panel_columns: &[ObjectsPanelColumn]) -> (Vec<Column>, Vec<String>) {
        let ids: Vec<String> = panel_columns.iter().map(|c| c.id.clone()).collect();
        let columns: Vec<Column> = panel_columns
            .iter()
            .map(|panel_col| {
                let mut col = Column::new(panel_col.id.clone(), panel_col.title.clone())
                    .width(panel_col.width)
                    .min_width(panel_col.min_width)
                    .resizable(panel_col.resizable);

                if panel_col.sortable {
                    col = col.sortable();
                }

                if panel_col.alignment == ObjectsPanelColumnAlignment::Right {
                    col = col.text_right();
                }

                col
            })
            .collect();

        (columns, ids)
    }

    /// Load extended data directly from the driver
    pub fn set_extended_data(&mut self, connection_id: Uuid, data: ObjectsPanelData) {
        self.connection_id = Some(connection_id);
        let (columns, column_ids) = Self::build_ui_columns(&data.columns);
        self.columns = columns;
        self.column_ids = column_ids;
        self.objects = data.rows;
        self.filtered_objects = self.objects.clone();
    }

    /// Set the driver category (determines context menu behavior)
    pub fn set_driver_category(&mut self, category: DriverCategory) {
        self.driver_category = category;
    }

    /// Set the database name for multi-database browsing (MySQL)
    pub fn set_database_name(&mut self, database_name: Option<String>) {
        self.database_name = database_name;
    }

    #[allow(dead_code)]
    pub fn database_name(&self) -> Option<String> {
        self.database_name.clone()
    }

    /// Get the current driver category
    pub fn driver_category(&self) -> DriverCategory {
        self.driver_category
    }

    /// Set key-value columns for Redis databases listing
    pub fn set_key_value_columns(&mut self) {
        let panel_columns = vec![
            ObjectsPanelColumn::new("name", "Database")
                .width(300.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("key_count", "Keys")
                .width(100.0)
                .min_width(60.0)
                .resizable(false)
                .sortable()
                .text_right(),
        ];
        let (columns, column_ids) = Self::build_ui_columns(&panel_columns);
        self.columns = columns;
        self.column_ids = column_ids;
    }

    /// Filter objects by search text
    pub fn filter(&mut self, search_text: &str) {
        if search_text.is_empty() {
            self.filtered_objects = self.objects.clone();
        } else {
            let search_lower = search_text.to_lowercase();
            self.filtered_objects = self
                .objects
                .iter()
                .filter(|obj| obj.name.to_lowercase().contains(&search_lower))
                .cloned()
                .collect();
        }
    }

    /// Clear all objects
    pub fn clear(&mut self) {
        self.connection_id = None;
        self.objects.clear();
        self.filtered_objects.clear();
        self.driver_category = DriverCategory::Relational;
        let default_data = ObjectsPanelData::from_table_infos(Vec::new());
        let (columns, column_ids) = Self::build_ui_columns(&default_data.columns);
        self.columns = columns;
        self.column_ids = column_ids;
    }

    /// Set loading state
    #[allow(dead_code)]
    pub fn set_loading(&mut self, loading: bool) {
        self.is_loading = loading;
    }

    /// Get the filtered objects (for external access)
    pub fn filtered_objects(&self) -> &[ObjectsPanelRow] {
        &self.filtered_objects
    }

    /// Get names of selected tables (including right-clicked if not in selection).
    fn get_selected_table_names(&self, right_clicked_name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .context_menu_selected_rows
            .iter()
            .filter_map(|&row_ix| self.filtered_objects.get(row_ix))
            .filter(|obj| obj.object_type == "table")
            .map(|obj| obj.name.clone())
            .collect();

        if !names.contains(&right_clicked_name.to_string()) {
            names.push(right_clicked_name.to_string());
        }
        names
    }

    /// Get names of selected views (including right-clicked if not in selection).
    fn get_selected_view_names(&self, right_clicked_name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .context_menu_selected_rows
            .iter()
            .filter_map(|&row_ix| self.filtered_objects.get(row_ix))
            .filter(|obj| obj.object_type == "view")
            .map(|obj| obj.name.clone())
            .collect();

        if !names.contains(&right_clicked_name.to_string()) {
            names.push(right_clicked_name.to_string());
        }
        names
    }

    /// Get names of selected keys (for Redis, including right-clicked if not in selection).
    fn get_selected_key_names(&self, right_clicked_name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .context_menu_selected_rows
            .iter()
            .filter_map(|&row_ix| self.filtered_objects.get(row_ix))
            .filter(|obj| obj.object_type == "key")
            .map(|obj| obj.name.clone())
            .collect();

        if !names.contains(&right_clicked_name.to_string()) {
            names.push(right_clicked_name.to_string());
        }
        names
    }
}

impl TableDelegate for ObjectsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.filtered_objects.len()
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns
            .get(col_ix)
            .cloned()
            .unwrap_or_else(|| Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix)))
    }

    fn loading(&self, _cx: &App) -> bool {
        self.is_loading
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx == col_ix {
                *col = col.clone().sort(sort);
            } else {
                *col = col.clone().sort(ColumnSort::Default);
            }
        }

        let Some(col_id) = self.column_ids.get(col_ix).cloned() else {
            cx.notify();
            return;
        };

        self.filtered_objects.sort_by(|a, b| {
            let val_a = a.values.get(&col_id).map(|s| s.as_str()).unwrap_or("");
            let val_b = b.values.get(&col_id).map(|s| s.as_str()).unwrap_or("");

            // Try numeric comparison first, fall back to string comparison
            let ordering = match (val_a.parse::<i64>(), val_b.parse::<i64>()) {
                (Ok(num_a), Ok(num_b)) => num_a.cmp(&num_b),
                _ => val_a.cmp(val_b),
            };

            match sort {
                ColumnSort::Ascending => ordering,
                ColumnSort::Descending => ordering.reverse(),
                ColumnSort::Default => std::cmp::Ordering::Equal,
            }
        });

        cx.notify();
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let Some(obj) = self.filtered_objects.get(row_ix) else {
            return div().into_any_element();
        };

        let Some(col_id) = self.column_ids.get(col_ix) else {
            return div().into_any_element();
        };

        // The "name" column (always first) gets an icon prefix
        if col_id == "name" {
            let icon = match obj.object_type.as_str() {
                "view" => ZqlzIcon::Eye,
                "redis_database" => ZqlzIcon::Database,
                _ => ZqlzIcon::Table,
            };

            let name = obj
                .values
                .get("name")
                .cloned()
                .unwrap_or_else(|| obj.name.clone());

            return h_flex()
                .h_full()
                .items_center()
                .gap_2()
                .px_2()
                .child(Icon::new(icon).size_4().text_color(theme.accent))
                .child(
                    div()
                        .text_sm()
                        .font_family(theme.mono_font_family.clone())
                        .child(name),
                )
                .into_any_element();
        }

        // All other columns: render the value string from the row's BTreeMap
        let text = obj
            .values
            .get(col_id)
            .cloned()
            .unwrap_or_else(|| "-".to_string());

        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .text_color(theme.muted_foreground)
            .child(text)
            .into_any_element()
    }

    fn context_menu(
        &mut self,
        row_ix: usize,
        _col_ix: Option<usize>,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> PopupMenu {
        let Some(obj) = self.filtered_objects.get(row_ix) else {
            return menu;
        };

        let Some(connection_id) = self.connection_id else {
            return menu;
        };

        let object_name = obj.name.clone();
        let object_type = obj.object_type.clone();
        let menu_entity = cx.entity();
        let panel = self.panel.clone();

        // Show different menu based on driver category and object type
        match self.driver_category {
            DriverCategory::KeyValue => {
                // For Redis databases, show a database-specific menu
                if object_type == "redis_database" {
                    self.build_redis_database_context_menu(
                        menu,
                        connection_id,
                        &obj,
                        menu_entity,
                        panel,
                        window,
                    )
                } else {
                    self.build_key_context_menu(
                        menu,
                        connection_id,
                        object_name,
                        menu_entity,
                        panel,
                        window,
                    )
                }
            }
            _ => {
                // Relational database menu
                if object_type == "view" {
                    self.build_view_context_menu(
                        menu,
                        connection_id,
                        object_name,
                        menu_entity,
                        panel,
                        window,
                    )
                } else {
                    self.build_table_context_menu(
                        menu,
                        connection_id,
                        object_name,
                        menu_entity,
                        panel,
                        window,
                    )
                }
            }
        }
    }

    fn set_context_menu_selection(&mut self, selected_rows: Vec<usize>) {
        self.context_menu_selected_rows = selected_rows;
    }
}

impl ObjectsTableDelegate {
    /// Build context menu for tables
    fn build_table_context_menu(
        &self,
        menu: PopupMenu,
        connection_id: Uuid,
        table_name: String,
        menu_entity: Entity<TableState<Self>>,
        panel: WeakEntity<ObjectsPanel>,
        window: &mut Window,
    ) -> PopupMenu {
        // Get all selected tables (including the right-clicked one)
        let selected_tables = self.get_selected_table_names(&table_name);
        let count = selected_tables.len();
        let is_multi = count > 1;
        let database_name = self.database_name.clone();

        menu
            // Open Table(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Open {} Tables", count)
                } else {
                    "Open Table".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::OpenTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Design Table(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Design {} Tables", count)
                } else {
                    "Design Table".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DesignTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // New Table (always single)
            .item({
                let panel = panel.clone();
                let database_name = database_name.clone();
                PopupMenuItem::new("New Table").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::NewTable {
                                connection_id,
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Delete Table(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Delete {} Tables", count)
                } else {
                    "Delete Table".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DeleteTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Empty Table(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Empty {} Tables", count)
                } else {
                    "Empty Table".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::EmptyTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Duplicate Table(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Duplicate {} Tables", count)
                } else {
                    "Duplicate Table".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DuplicateTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Import Wizard (single table only - use right-clicked table)
            .item({
                let panel = panel.clone();
                let table_name = table_name.clone();
                let database_name = database_name.clone();
                PopupMenuItem::new("Import Wizard...").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::ImportData {
                                connection_id,
                                table_name: table_name.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Export Wizard
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Export {} Tables...", count)
                } else {
                    "Export Wizard...".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::ExportTables {
                                connection_id,
                                table_names: tables.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Dump SQL (Structure + Data)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Dump SQL {} Tables (Structure + Data)", count)
                } else {
                    "Dump SQL (Structure + Data)".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DumpTablesSql {
                                connection_id,
                                table_names: tables.clone(),
                                include_data: true,
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Dump SQL (Structure Only)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Dump SQL {} Tables (Structure Only)", count)
                } else {
                    "Dump SQL (Structure Only)".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DumpTablesSql {
                                connection_id,
                                table_names: tables.clone(),
                                include_data: false,
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Copy Table Name(s)
            .item({
                let panel = panel.clone();
                let tables = selected_tables.clone();
                let label = if is_multi {
                    format!("Copy {} Table Names", count)
                } else {
                    "Copy Table Name".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::CopyTableNames {
                                table_names: tables.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Rename (single table only - show only when single selection)
            .when(!is_multi, |menu| {
                menu.item({
                    let panel = panel.clone();
                    let table_name = table_name.clone();
                    let database_name = database_name.clone();
                    PopupMenuItem::new("Rename").on_click(window.listener_for(
                        &menu_entity,
                        move |_this, _, _, cx| {
                            _ = panel.update(cx, |_panel, cx| {
                                cx.emit(ObjectsPanelEvent::RenameTable {
                                    connection_id,
                                    table_name: table_name.clone(),
                                    database_name: database_name.clone(),
                                });
                            });
                        },
                    ))
                })
            })
            // Refresh
            .item({
                let panel = panel.clone();
                PopupMenuItem::new("Refresh").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::Refresh);
                        });
                    },
                ))
            })
    }

    /// Build context menu for views
    fn build_view_context_menu(
        &self,
        menu: PopupMenu,
        connection_id: Uuid,
        view_name: String,
        menu_entity: Entity<TableState<Self>>,
        panel: WeakEntity<ObjectsPanel>,
        window: &mut Window,
    ) -> PopupMenu {
        // Get all selected views (including the right-clicked one)
        let selected_views = self.get_selected_view_names(&view_name);
        let count = selected_views.len();
        let is_multi = count > 1;
        let database_name = self.database_name.clone();

        menu
            // Open View(s)
            .item({
                let panel = panel.clone();
                let views = selected_views.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Open {} Views", count)
                } else {
                    "Open View".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::OpenViews {
                                connection_id,
                                view_names: views.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Design View(s)
            .item({
                let panel = panel.clone();
                let views = selected_views.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Design {} Views", count)
                } else {
                    "Design View".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DesignViews {
                                connection_id,
                                view_names: views.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // New View (always single)
            .item({
                let panel = panel.clone();
                let database_name = database_name.clone();
                PopupMenuItem::new("New View").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::NewView {
                                connection_id,
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Delete View(s)
            .item({
                let panel = panel.clone();
                let views = selected_views.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Delete {} Views", count)
                } else {
                    "Delete View".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DeleteViews {
                                connection_id,
                                view_names: views.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            // Duplicate View(s)
            .item({
                let panel = panel.clone();
                let views = selected_views.clone();
                let database_name = database_name.clone();
                let label = if is_multi {
                    format!("Duplicate {} Views", count)
                } else {
                    "Duplicate View".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DuplicateViews {
                                connection_id,
                                view_names: views.clone(),
                                database_name: database_name.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Copy View Name(s)
            .item({
                let panel = panel.clone();
                let views = selected_views.clone();
                let label = if is_multi {
                    format!("Copy {} View Names", count)
                } else {
                    "Copy View Name".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::CopyViewNames {
                                view_names: views.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // View History (single view only)
            .when(!is_multi, |menu| {
                menu.item({
                    let panel = panel.clone();
                    let view_name = view_name.clone();
                    PopupMenuItem::new("View History").on_click(window.listener_for(
                        &menu_entity,
                        move |_this, _, _, cx| {
                            _ = panel.update(cx, |_panel, cx| {
                                cx.emit(ObjectsPanelEvent::ViewHistory {
                                    connection_id,
                                    object_name: view_name.clone(),
                                    object_type: "view".to_string(),
                                });
                            });
                        },
                    ))
                })
                .separator()
            })
            // Rename View (single view only)
            .when(!is_multi, |menu| {
                menu.item({
                    let panel = panel.clone();
                    let view_name = view_name.clone();
                    let database_name = database_name.clone();
                    PopupMenuItem::new("Rename").on_click(window.listener_for(
                        &menu_entity,
                        move |_this, _, _, cx| {
                            _ = panel.update(cx, |_panel, cx| {
                                cx.emit(ObjectsPanelEvent::RenameView {
                                    connection_id,
                                    view_name: view_name.clone(),
                                    database_name: database_name.clone(),
                                });
                            });
                        },
                    ))
                })
            })
            // Refresh
            .item({
                let panel = panel.clone();
                PopupMenuItem::new("Refresh").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::Refresh);
                        });
                    },
                ))
            })
    }

    /// Build context menu for key-value store keys (Redis, Memcached, Valkey, etc.)
    fn build_key_context_menu(
        &self,
        menu: PopupMenu,
        connection_id: Uuid,
        key_name: String,
        menu_entity: Entity<TableState<Self>>,
        panel: WeakEntity<ObjectsPanel>,
        window: &mut Window,
    ) -> PopupMenu {
        // Get all selected keys (including the right-clicked one)
        let selected_keys = self.get_selected_key_names(&key_name);
        let count = selected_keys.len();
        let is_multi = count > 1;

        menu
            // Open Key(s)
            .item({
                let panel = panel.clone();
                let keys = selected_keys.clone();
                let label = if is_multi {
                    format!("Open {} Keys", count)
                } else {
                    "Open Key".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::OpenTables {
                                connection_id,
                                table_names: keys.clone(),
                                database_name: None,
                            });
                        });
                    },
                ))
            })
            .separator()
            // Delete Key(s)
            .item({
                let panel = panel.clone();
                let keys = selected_keys.clone();
                let label = if is_multi {
                    format!("Delete {} Keys", count)
                } else {
                    "Delete Key".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::DeleteKeys {
                                connection_id,
                                key_names: keys.clone(),
                            });
                        });
                    },
                ))
            })
            .separator()
            // Copy Key Name(s)
            .item({
                let panel = panel.clone();
                let keys = selected_keys.clone();
                let label = if is_multi {
                    format!("Copy {} Key Names", count)
                } else {
                    "Copy Key Name".to_string()
                };
                PopupMenuItem::new(label).on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::CopyKeyNames {
                                key_names: keys.clone(),
                            });
                        });
                    },
                ))
            })
            // Rename Key (single key only)
            .when(!is_multi, |menu| {
                menu.item({
                    let panel = panel.clone();
                    let key_name = key_name.clone();
                    PopupMenuItem::new("Rename Key").on_click(window.listener_for(
                        &menu_entity,
                        move |_this, _, _, cx| {
                            _ = panel.update(cx, |_panel, cx| {
                                cx.emit(ObjectsPanelEvent::RenameTable {
                                    connection_id,
                                    table_name: key_name.clone(),
                                    database_name: None,
                                });
                            });
                        },
                    ))
                })
            })
            .separator()
            // Refresh
            .item({
                let panel = panel.clone();
                PopupMenuItem::new("Refresh").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::Refresh);
                        });
                    },
                ))
            })
    }

    /// Build context menu for Redis databases
    fn build_redis_database_context_menu(
        &self,
        menu: PopupMenu,
        connection_id: Uuid,
        obj: &ObjectsPanelRow,
        menu_entity: Entity<TableState<Self>>,
        panel: WeakEntity<ObjectsPanel>,
        window: &mut Window,
    ) -> PopupMenu {
        let Some(database_index) = obj.redis_database_index else {
            return menu;
        };

        menu
            // Open Database (view all keys in table viewer)
            .item({
                let panel = panel.clone();
                PopupMenuItem::new("Open Database").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::OpenRedisDatabase {
                                connection_id,
                                database_index,
                            });
                        });
                    },
                ))
            })
            .separator()
            // Refresh
            .item({
                let panel = panel.clone();
                PopupMenuItem::new("Refresh").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        _ = panel.update(cx, |_panel, cx| {
                            cx.emit(ObjectsPanelEvent::Refresh);
                        });
                    },
                ))
            })
    }

    #[allow(dead_code)]
    fn render_empty(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_4()
            .child(
                Icon::new(IconName::File)
                    .size(px(48.))
                    .text_color(theme.muted_foreground),
            )
            .child(body_small("No objects found").color(theme.muted_foreground))
            .child(body_small("This database has no tables or views").color(theme.muted_foreground))
            .into_any_element()
    }
}

/// Objects panel showing database objects with search
pub struct ObjectsPanel {
    focus_handle: FocusHandle,
    search_input: Entity<InputState>,
    /// Table state for the objects list
    table_state: Entity<TableState<ObjectsTableDelegate>>,
    /// Whether the panel has a valid connection
    has_connection: bool,
    /// Currently selected connection ID
    selected_connection_id: Option<Uuid>,
    /// Currently selected connection name
    connection_name: Option<String>,
    /// Database name for MySQL multi-database browsing (distinct from connection_name)
    database_name: Option<String>,
    /// Search text for filtering
    search_text: String,
}

impl ObjectsPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search objects..."));

        // Create the weak reference first (we'll update it after creating the table state)
        let weak_self = cx.weak_entity();

        // Create table state with delegate
        let table_state = cx.new(|cx| {
            let delegate = ObjectsTableDelegate::new(weak_self);
            TableState::new(delegate, window, cx)
        });

        // Subscribe to search input changes
        cx.subscribe(&search_input, |panel, _input, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                let search_value = panel.search_input.read(cx).value().to_string();
                panel.search_text = search_value.clone();
                panel.table_state.update(cx, |state, cx| {
                    state.delegate_mut().filter(&search_value);
                    cx.notify();
                });
                cx.notify();
            }
        })
        .detach();

        // Subscribe to table events to handle double-click -> open table/view/database
        cx.subscribe(&table_state, |panel, _table, event: &TableEvent, cx| {
            if let TableEvent::DoubleClickedRow(row_ix) = event {
                // Open the table/view/database on double-click
                let table_state = panel.table_state.read(cx);
                if let Some(obj) = table_state.delegate().filtered_objects().get(*row_ix) {
                    if let Some(connection_id) = panel.selected_connection_id {
                        let database_name = panel.database_name.clone();
                        // Emit appropriate event based on object type
                        if obj.object_type == "redis_database" {
                            if let Some(db_index) = obj.redis_database_index {
                                cx.emit(ObjectsPanelEvent::OpenRedisDatabase {
                                    connection_id,
                                    database_index: db_index,
                                });
                            }
                        } else if obj.object_type == "view" {
                            cx.emit(ObjectsPanelEvent::OpenViews {
                                connection_id,
                                view_names: vec![obj.name.clone()],
                                database_name,
                            });
                        } else {
                            cx.emit(ObjectsPanelEvent::OpenTables {
                                connection_id,
                                table_names: vec![obj.name.clone()],
                                database_name,
                            });
                        }
                    }
                }
            }
        })
        .detach();

        Self {
            focus_handle: cx.focus_handle(),
            search_input,
            table_state,
            has_connection: false,
            selected_connection_id: None,
            connection_name: None,
            database_name: None,
            search_text: String::new(),
        }
    }

    /// Update the objects list with driver-provided extended data
    pub fn load_objects(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        database_name: Option<String>,
        data: ObjectsPanelData,
        driver_category: DriverCategory,
        cx: &mut Context<Self>,
    ) {
        self.selected_connection_id = Some(connection_id);
        self.connection_name = Some(connection_name);
        self.database_name = database_name.clone();
        self.has_connection = true;

        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().set_driver_category(driver_category);
            state.delegate_mut().set_database_name(database_name);
            state.delegate_mut().set_extended_data(connection_id, data);
            if !self.search_text.is_empty() {
                state.delegate_mut().filter(&self.search_text);
            }
            state.refresh(cx);
        });

        cx.notify();
    }

    /// Load Redis databases into the objects panel
    pub fn load_redis_databases(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        databases: Vec<(u16, Option<i64>)>, // (index, key_count)
        cx: &mut Context<Self>,
    ) {
        self.selected_connection_id = Some(connection_id);
        self.connection_name = Some(connection_name);
        self.has_connection = true;

        let objects: Vec<ObjectsPanelRow> = databases
            .into_iter()
            .map(|(index, key_count)| {
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), format!("db{}", index));
                values.insert(
                    "key_count".to_string(),
                    key_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );

                ObjectsPanelRow {
                    name: format!("db{}", index),
                    object_type: "redis_database".to_string(),
                    values,
                    redis_database_index: Some(index),
                    key_value_info: None,
                }
            })
            .collect();

        self.table_state.update(cx, |state, cx| {
            state
                .delegate_mut()
                .set_driver_category(DriverCategory::KeyValue);
            state.delegate_mut().set_key_value_columns();
            state.delegate_mut().connection_id = Some(connection_id);
            state.delegate_mut().objects = objects.clone();
            state.delegate_mut().filtered_objects = objects;
            if !self.search_text.is_empty() {
                state.delegate_mut().filter(&self.search_text);
            }
            state.refresh(cx);
        });

        cx.notify();
    }

    /// Clear the objects list (called when connection is closed)
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.selected_connection_id = None;
        self.connection_name = None;
        self.database_name = None;
        self.has_connection = false;
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().clear();
            state.refresh(cx);
        });
        cx.notify();
    }

    /// Get the currently selected connection ID
    pub fn selected_connection_id(&self) -> Option<Uuid> {
        self.selected_connection_id
    }

    /// Get the current database name (for MySQL multi-database browsing)
    pub fn database_name(&self) -> Option<String> {
        self.database_name.clone()
    }

    /// Set whether the panel has a valid connection
    pub fn set_has_connection(&mut self, has_connection: bool, cx: &mut Context<Self>) {
        self.has_connection = has_connection;
        if !has_connection {
            self.table_state.update(cx, |state, cx| {
                state.delegate_mut().clear();
                state.refresh(cx);
            });
        }
        cx.notify();
    }

    /// Emit a refresh event to reload the objects list
    ///
    /// This is called by MainView when the user presses Cmd+R while the panel is focused.
    pub fn refresh(&self, cx: &mut Context<Self>) {
        tracing::info!("ObjectsPanel: Refreshing objects list");
        cx.emit(ObjectsPanelEvent::Refresh);
    }
}

impl Render for ObjectsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        // Get driver category to conditionally show/hide buttons
        let driver_category = self.table_state.read(cx).delegate().driver_category();
        let is_relational = driver_category == DriverCategory::Relational;

        let refresh_handler = cx.listener(|_this, _, _, cx| {
            cx.emit(ObjectsPanelEvent::Refresh);
        });

        let new_table_handler = cx.listener(|this, _, _, cx| {
            if let Some(conn_id) = this.selected_connection_id {
                cx.emit(ObjectsPanelEvent::NewTable {
                    connection_id: conn_id,
                    database_name: this.database_name.clone(),
                });
            }
        });

        let import_handler = cx.listener(|this, _, _, cx| {
            if let Some(conn_id) = this.selected_connection_id {
                // For toolbar button, import to a new/unspecified table
                cx.emit(ObjectsPanelEvent::ImportData {
                    connection_id: conn_id,
                    table_name: String::new(),
                    database_name: this.database_name.clone(),
                });
            }
        });

        let export_handler = cx.listener(|this, _, _, cx| {
            if let Some(conn_id) = this.selected_connection_id {
                // For toolbar button, export all tables (empty means all)
                cx.emit(ObjectsPanelEvent::ExportTables {
                    connection_id: conn_id,
                    table_names: vec![],
                    database_name: this.database_name.clone(),
                });
            }
        });

        v_flex()
            .id("objects-panel")
            .key_context("ObjectsPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .font_family(theme.font_family.clone())
            .child(
                // Toolbar
                h_flex()
                    .w_full()
                    .p_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        Button::new("refresh-objects")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::ArrowsClockwise)
                            .tooltip("Refresh")
                            .on_click(refresh_handler),
                    )
                    // Only show "New Table" for relational databases (not for Redis/KeyValue)
                    .when(is_relational, |this| {
                        this.child(
                            Button::new("add-object")
                                .ghost()
                                .xsmall()
                                .icon(ZqlzIcon::Plus)
                                .tooltip("New Table")
                                .on_click(new_table_handler),
                        )
                    })
                    .child(
                        Button::new("import-data")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::Import)
                            .tooltip("Import Wizard...")
                            .on_click(import_handler),
                    )
                    .child(
                        Button::new("export-data")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::Export)
                            .tooltip("Export Wizard...")
                            .on_click(export_handler),
                    )
                    .child(div().flex_1())
                    .child(
                        Button::new("list-view")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::ListBullets)
                            .tooltip("List View"),
                    )
                    .child(
                        Button::new("grid-view")
                            .ghost()
                            .xsmall()
                            .icon(IconName::ChevronRight)
                            .tooltip("Grid View"),
                    )
                    .child(
                        div().w(px(200.)).child(
                            Input::new(&self.search_input)
                                .prefix(Icon::new(ZqlzIcon::MagnifyingGlass).size_3()),
                        ),
                    ),
            )
            .child(
                // Content area
                div()
                    .id("objects-content")
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .when(!self.has_connection, |this| {
                        this.child(
                            v_flex()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .gap_4()
                                .child(
                                    Icon::new(IconName::File)
                                        .size(px(48.))
                                        .text_color(theme.muted_foreground),
                                )
                                .child(
                                    body_small("No connection selected")
                                        .color(theme.muted_foreground),
                                )
                                .child(
                                    body_small("Connect to a database to view objects")
                                        .color(theme.muted_foreground),
                                ),
                        )
                    })
                    .when(self.has_connection, |this| {
                        this.child(
                            Table::new(&self.table_state)
                                .stripe(true)
                                .bordered(false)
                                .small(),
                        )
                    }),
            )
    }
}

impl Focusable for ObjectsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ObjectsPanel {}
impl EventEmitter<ObjectsPanelEvent> for ObjectsPanel {}

impl Panel for ObjectsPanel {
    fn panel_name(&self) -> &'static str {
        "ObjectsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(name) = &self.connection_name {
            format!("Objects - {}", name)
        } else {
            "Objects".to_string()
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
