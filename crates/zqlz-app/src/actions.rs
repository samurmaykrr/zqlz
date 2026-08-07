//! ZQLZ Actions
//!
//! Defines all actions available in the ZQLZ application.
//! Actions can be triggered via keyboard shortcuts or UI interactions.

use gpui::{Action, actions};
use serde::Deserialize;

actions!(
    zqlz,
    [
        // Application actions
        OpenSettings,
        OpenCommandPalette,
        InstallCli,
        Quit,
        NewWindow,
        CloseWindow,
        MinimizeWindow,
        ZoomWindow,
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
        ExplainAnalyzeQuery,
        ExplainAnalyzeSelection,
        StopQuery,
        // Layout actions
        ToggleLeftSidebar,
        ToggleRightSidebar,
        ToggleBottomPanel,
        ToggleAllDocks,
        ShowSchemaInspector,
        ShowCellEditorInspector,
        ShowKeyEditorInspector,
        ShowQueryHistoryInspector,
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
    NextProblem, PreviousProblem, SaveQuery, SaveQueryAs, ShowCodeActions, ShowHover,
    ToggleProblemsPanel, TriggerParameterHints,
};

// Tab management actions
actions!(
    tabs,
    [
        ActivateNextTab,
        ActivatePrevTab,
        CloseEditor,
        CloseActiveTab,
        CloseOtherTabs,
        CloseTabsToRight,
        CloseTabsToLeft,
        CloseCleanTabs,
        CloseAllTabs,
        MoveTabToNewWindow,
        TogglePinActiveTab,
        PinTab,
        UnpinTab,
        MarkActiveTabAsPreview,
        ClearActiveTabPreview,
        NavigateTabBack,
        NavigateTabForward,
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
    [
        CancelCellEditing,
        CommitChanges,
        DeleteSelectedRows,
        /// Undo the last cell edit (batch mode only)
        UndoEdit,
        /// Redo the last undone cell edit (batch mode only)
        RedoEdit,
        /// Export visible rows to a CSV file on disk
        ExportCsvToFile,
        /// Export visible rows to a JSON file on disk
        ExportJsonToFile,
        /// Export visible rows as SQL INSERT to a file on disk
        ExportSqlToFile,
    ]
);

actions!(
    cell_editor,
    [
        /// Save the current cell editor value
        SaveCellEdit,
        /// Cancel the current cell edit
        CancelCellEdit,
        /// Format the current cell editor value
        FormatCellEdit,
        /// Toggle word wrap in the cell editor
        ToggleCellEditorWordWrap,
    ]
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
