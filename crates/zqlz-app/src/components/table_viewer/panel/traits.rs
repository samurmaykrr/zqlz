use super::*;

impl Focusable for TableViewerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for TableViewerPanel {}
impl EventEmitter<TableViewerEvent> for TableViewerPanel {}

impl Panel for TableViewerPanel {
    fn panel_name(&self) -> &'static str {
        "TableViewer"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let type_indicator = if self.is_view { "[View] " } else { "" };
        let title = match (&self.table_name, &self.connection_name) {
            (Some(table), Some(conn)) => format!("{}{} ({})", type_indicator, table, conn),
            (Some(table), None) => format!("{}{}", type_indicator, table),
            _ => "Table Viewer".to_string(),
        };
        SharedString::from(title)
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }

    fn set_active(&mut self, active: bool, _window: &mut Window, cx: &mut Context<Self>) {
        if active && self.connection_id.is_some() && self.table_name.is_some() {
            cx.emit(TableViewerEvent::BecameActive);
        }
    }

    fn on_removed(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.connection_id.is_some() && self.table_name.is_some() {
            cx.emit(TableViewerEvent::BecameInactive);
        }
    }
}
