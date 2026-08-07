#[derive(Clone, Debug)]
pub enum TextEditorEvent {
    ContentChanged,
    /// Go-to-definition resolved to something outside the buffer.
    ///
    /// The editor cannot navigate there itself, so the embedder that supplied the
    /// [`crate::DefinitionProvider`] interprets the URI it minted.
    OpenExternalDefinition { uri: String },
}
