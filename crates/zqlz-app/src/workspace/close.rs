use gpui::{App, Entity, WeakEntity};
use uuid::Uuid;
use zqlz_query::widgets::QueryEditor;
use zqlz_ui::widgets::dock::PanelView;

use super::item::{ClosePolicy, WorkspaceItemView, metadata_for_panel};

#[derive(Clone, Debug)]
struct QueryEditorCloseSnapshot {
    saved_query_id: Option<Uuid>,
    connection_id: Option<Uuid>,
    sql: String,
    current_name: String,
}

pub enum WorkspaceItemCloseIntent {
    CloseNow,
    Blocked,
    SavedQuery {
        query_id: Uuid,
        editor: WeakEntity<QueryEditor>,
        sql: String,
    },
    NewQuery {
        editor: WeakEntity<QueryEditor>,
        connection_id: Option<Uuid>,
        sql: String,
        current_name: String,
    },
    DirtyNonQuery,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DirtyQueryClosePrompt {
    pub title: &'static str,
    pub width_px: f32,
    pub intro: &'static str,
    pub message: &'static str,
    pub discard_button: &'static str,
    pub save_button: &'static str,
    pub query_name_label: Option<&'static str>,
    pub query_name_placeholder: Option<&'static str>,
    pub save_location_label: Option<&'static str>,
    pub unknown_connection_name: Option<&'static str>,
    pub missing_connection_message: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirtyNonQueryClosePrompt {
    pub title: &'static str,
    pub message: &'static str,
    pub discard_button: &'static str,
    pub cancel_button: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceSaveCloseDecision {
    CloseTab,
    ShowError(String),
}

pub fn save_close_decision<T>(save_result: Result<T, String>) -> WorkspaceSaveCloseDecision {
    match save_result {
        Ok(_) => WorkspaceSaveCloseDecision::CloseTab,
        Err(error) => WorkspaceSaveCloseDecision::ShowError(error),
    }
}

pub fn dirty_saved_query_close_prompt() -> DirtyQueryClosePrompt {
    DirtyQueryClosePrompt {
        title: "Unsaved Changes",
        width_px: 420.0,
        intro: "This tab has unsaved changes.",
        message: "Do you want to save before closing?",
        discard_button: "Don't Save",
        save_button: "Save",
        query_name_label: None,
        query_name_placeholder: None,
        save_location_label: None,
        unknown_connection_name: None,
        missing_connection_message: None,
    }
}

pub fn dirty_new_query_close_prompt() -> DirtyQueryClosePrompt {
    DirtyQueryClosePrompt {
        title: "Unsaved Changes",
        width_px: 440.0,
        intro: "This tab has unsaved changes.",
        message: "Save it before closing?",
        discard_button: "Don't Save",
        save_button: "Save",
        query_name_label: Some("Query Name:"),
        query_name_placeholder: Some("Enter query name..."),
        save_location_label: Some("Save Location:"),
        unknown_connection_name: Some("Unknown"),
        missing_connection_message: Some(
            "No connection selected. Please connect to a database first.",
        ),
    }
}

pub fn dirty_non_query_close_prompt() -> DirtyNonQueryClosePrompt {
    DirtyNonQueryClosePrompt {
        title: "Unsaved Changes",
        message: "This tab has unsaved changes. Do you want to close it anyway?",
        discard_button: "Don't Save",
        cancel_button: "Cancel",
    }
}

pub fn close_intent_for_panel(panel: &dyn PanelView, cx: &App) -> WorkspaceItemCloseIntent {
    let Some(editor) = panel.view().downcast::<QueryEditor>().ok() else {
        let metadata = metadata_for_panel(panel, cx);
        return non_query_close_intent(metadata.closable, metadata.dirty);
    };

    if editor.close_policy(cx) == ClosePolicy::Close {
        return WorkspaceItemCloseIntent::CloseNow;
    }

    let snapshot = query_editor_close_snapshot(&editor, cx);
    if let Some(query_id) = snapshot.saved_query_id {
        WorkspaceItemCloseIntent::SavedQuery {
            query_id,
            editor: editor.downgrade(),
            sql: snapshot.sql,
        }
    } else {
        WorkspaceItemCloseIntent::NewQuery {
            editor: editor.downgrade(),
            connection_id: snapshot.connection_id,
            sql: snapshot.sql,
            current_name: snapshot.current_name,
        }
    }
}

fn query_editor_close_snapshot(editor: &Entity<QueryEditor>, cx: &App) -> QueryEditorCloseSnapshot {
    editor.read_with(cx, |editor, cx| QueryEditorCloseSnapshot {
        saved_query_id: editor.saved_query_id(),
        connection_id: editor.connection_id(),
        sql: editor.content(cx).to_string(),
        current_name: editor.name(),
    })
}

fn non_query_close_intent(closable: bool, dirty: bool) -> WorkspaceItemCloseIntent {
    if !closable {
        WorkspaceItemCloseIntent::Blocked
    } else if dirty {
        WorkspaceItemCloseIntent::DirtyNonQuery
    } else {
        WorkspaceItemCloseIntent::CloseNow
    }
}

#[cfg(test)]
mod tests {
    use super::{
        WorkspaceItemCloseIntent, WorkspaceSaveCloseDecision, dirty_new_query_close_prompt,
        dirty_non_query_close_prompt, dirty_saved_query_close_prompt, non_query_close_intent,
        save_close_decision,
    };

    #[test]
    fn non_query_close_blocks_non_closable_item() {
        assert!(matches!(
            non_query_close_intent(false, false),
            WorkspaceItemCloseIntent::Blocked
        ));
    }

    #[test]
    fn non_query_close_prompts_for_dirty_item() {
        assert!(matches!(
            non_query_close_intent(true, true),
            WorkspaceItemCloseIntent::DirtyNonQuery
        ));
    }

    #[test]
    fn non_query_close_allows_clean_closable_item() {
        assert!(matches!(
            non_query_close_intent(true, false),
            WorkspaceItemCloseIntent::CloseNow
        ));
    }

    #[test]
    fn dirty_query_close_prompts_describe_saved_and_new_variants() {
        let saved = dirty_saved_query_close_prompt();
        assert_eq!(saved.title, "Unsaved Changes");
        assert_eq!(saved.width_px, 420.0);
        assert_eq!(saved.intro, "This tab has unsaved changes.");
        assert_eq!(saved.discard_button, "Don't Save");
        assert_eq!(saved.save_button, "Save");
        assert_eq!(saved.query_name_label, None);

        let new_query = dirty_new_query_close_prompt();
        assert_eq!(new_query.title, "Unsaved Changes");
        assert_eq!(new_query.width_px, 440.0);
        assert_eq!(new_query.intro, "This tab has unsaved changes.");
        assert_eq!(new_query.discard_button, "Don't Save");
        assert_eq!(new_query.save_button, "Save");
        assert_eq!(new_query.query_name_label, Some("Query Name:"));
        assert_eq!(
            new_query.query_name_placeholder,
            Some("Enter query name...")
        );
        assert_eq!(new_query.save_location_label, Some("Save Location:"));
        assert_eq!(new_query.unknown_connection_name, Some("Unknown"));
        assert_eq!(
            new_query.missing_connection_message,
            Some("No connection selected. Please connect to a database first.")
        );
    }

    #[test]
    fn dirty_non_query_close_prompt_describes_buttons() {
        let prompt = dirty_non_query_close_prompt();

        assert_eq!(prompt.title, "Unsaved Changes");
        assert_eq!(prompt.discard_button, "Don't Save");
        assert_eq!(prompt.cancel_button, "Cancel");
    }

    #[test]
    fn save_close_decision_closes_after_successful_save() {
        assert_eq!(
            save_close_decision(Ok(())),
            WorkspaceSaveCloseDecision::CloseTab
        );
    }

    #[test]
    fn save_close_decision_keeps_dialog_open_after_failed_save() {
        assert_eq!(
            save_close_decision::<()>(Err("Missing query name".to_string())),
            WorkspaceSaveCloseDecision::ShowError("Missing query name".to_string())
        );
    }
}
