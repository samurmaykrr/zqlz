// Stub for Zed's zed_actions crate

// Editor actions
pub mod editor {
    use gpui::actions;

    actions!(editor, [OpenExcerpts, ToggleCodeActions, MoveUp, MoveDown,]);
}

// Preview actions
pub mod preview {
    use gpui::actions;

    actions!(preview, [Open,]);

    pub mod markdown {
        use gpui::actions;
        actions!(markdown, [OpenPreview,]);
    }

    pub mod svg {
        use gpui::actions;
        actions!(svg, [OpenPreview,]);
    }
}

// Assistant actions
pub mod assistant {
    use gpui::actions;

    actions!(assistant, [ToggleAssistant, InlineAssist,]);
}

// Agent actions
pub mod agent {
    use gpui::actions;

    actions!(
        agent,
        [
            ToggleAllLanguageModels,
            ResetAllLanguageModels,
            AddSelectionToThread,
        ]
    );
}

// Outline actions
pub mod outline {
    use gpui::actions;

    actions!(outline, [ToggleOutline,]);

    pub const TOGGLE_OUTLINE: ToggleOutline = ToggleOutline;
}

// Workspace actions
pub mod workspace {
    use gpui::actions;

    actions!(workspace, [NewFile, Open, CopyPath, CopyRelativePath,]);
}

// Additional zed actions
use gpui::actions;

actions!(zed, [OpenZedUrl,]);

pub const OpenKeymapFile: () = ();
