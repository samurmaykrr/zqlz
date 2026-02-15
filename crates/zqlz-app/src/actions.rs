//! ZQLZ Actions
//!
//! Defines all actions available in the ZQLZ application.
//! Actions can be triggered via keyboard shortcuts or UI interactions.

use gpui::{actions, Action};
use serde::Deserialize;

actions!(
    zqlz,
    [
        // Application actions
        OpenSettings,
        OpenCommandPalette,
        Quit,
        // Connection actions
        NewConnection,
        RefreshConnection,
        RefreshConnectionsList,
        // Query actions
        NewQuery,
        ExecuteQuery,
        ExecuteSelection,
        ExecuteCurrentStatement,
        ExplainQuery,
        ExplainSelection,
        StopQuery,
        // Layout actions
        ToggleLeftSidebar,
        ToggleRightSidebar,
        ToggleBottomPanel,
        // Focus actions
        FocusEditor,
        FocusResults,
        FocusSidebar,
        // Universal actions
        /// Refresh the currently focused panel (table viewer, schema browser, connections, etc.)
        Refresh,
    ]
);

// Re-export query editor actions from zqlz-query crate
pub use zqlz_query::{
    AcceptCompletion, CancelCompletion, CommentSelection, CopyLineDown, CopyLineUp, DeleteLine,
    DuplicateLine, FindNext, FindPrevious, FormatQuery, GoToDefinition, MoveLineDown, MoveLineUp,
    SaveQuery, SaveQueryAs, ShowHover, ToggleLineComment, ToggleProblemsPanel, TriggerCompletion,
    TriggerParameterHints, UncommentSelection,
};

// Tab management actions
actions!(
    tabs,
    [
        ActivateNextTab,
        ActivatePrevTab,
        CloseActiveTab,
        CloseOtherTabs,
        CloseTabsToRight,
        CloseAllTabs,
        ActivateTab1,
        ActivateTab2,
        ActivateTab3,
        ActivateTab4,
        ActivateTab5,
        ActivateTab6,
        ActivateTab7,
        ActivateTab8,
        ActivateTab9,
        ActivateLastTab,
    ]
);

// Table viewer actions
actions!(
    table_viewer,
    [CancelCellEditing, CommitChanges, DeleteSelectedRows]
);

// Versioning actions
actions!(
    versioning,
    [
        /// Show the version history for the selected database object
        ShowVersionHistory,
        /// Compare two versions of a database object
        CompareVersions,
        /// Restore a previous version of a database object
        RestoreVersion,
        /// Save the current state of a database object as a new version
        SaveVersion,
    ]
);

/// Connection-specific actions that carry connection ID
/// Note: These actions use String for connection_id (represents UUID in string format)

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectToConnection {
    pub connection_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DisconnectFromConnection {
    pub connection_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeleteConnection {
    pub connection_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DuplicateConnection {
    pub connection_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OpenConnectionSettings {
    pub connection_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Action)]
#[action(namespace = zqlz, no_json)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NewQueryForConnection {
    pub connection_id: String,
}
