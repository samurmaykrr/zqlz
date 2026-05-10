use crate::{SelectionState, TransactionId, buffer};

#[derive(Clone)]
pub(crate) struct TransactionRecord {
    id: TransactionId,
    changes: Vec<buffer::Change>,
    before: SelectionState,
    after: SelectionState,
}

impl TransactionRecord {
    pub(crate) fn new(
        id: TransactionId,
        changes: Vec<buffer::Change>,
        before: SelectionState,
        after: SelectionState,
    ) -> Self {
        Self {
            id,
            changes,
            before,
            after,
        }
    }

    pub(crate) fn id(&self) -> TransactionId {
        self.id
    }

    pub(crate) fn changes(&self) -> &[buffer::Change] {
        &self.changes
    }

    pub(crate) fn before(&self) -> &SelectionState {
        &self.before
    }

    pub(crate) fn after(&self) -> &SelectionState {
        &self.after
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        TransactionId,
        Vec<buffer::Change>,
        SelectionState,
        SelectionState,
    ) {
        (self.id, self.changes, self.before, self.after)
    }
}

#[derive(Default)]
pub(crate) struct EditorHistoryState {
    undo_stack: Vec<TransactionRecord>,
    redo_stack: Vec<TransactionRecord>,
    last_edit_time: Option<std::time::Instant>,
    active_transaction: Option<TransactionRecord>,
}

impl EditorHistoryState {
    pub(crate) fn active_transaction_id(&self) -> Option<TransactionId> {
        self.active_transaction.as_ref().map(TransactionRecord::id)
    }

    pub(crate) fn begin_active_transaction(
        &mut self,
        id: TransactionId,
        selection_state: SelectionState,
    ) {
        self.active_transaction = Some(TransactionRecord::new(
            id,
            Vec::new(),
            selection_state.clone(),
            selection_state,
        ));
    }

    pub(crate) fn finish_active_transaction(
        &mut self,
        id: TransactionId,
        after: SelectionState,
    ) -> bool {
        let Some(mut record) = self.active_transaction.take() else {
            return false;
        };

        if record.id() != id {
            self.active_transaction = Some(record);
            return false;
        }

        record.after = after;
        if !record.changes.is_empty() {
            self.redo_stack.clear();
            self.undo_stack.push(record);
        }
        self.last_edit_time = None;
        true
    }

    pub(crate) fn push_undo(&mut self, record: TransactionRecord) {
        self.undo_stack.push(record);
    }

    pub(crate) fn push_undo_group(
        &mut self,
        id: TransactionId,
        changes: Vec<buffer::Change>,
        before: SelectionState,
        after: SelectionState,
    ) {
        self.undo_stack
            .push(TransactionRecord::new(id, changes, before, after));
    }

    pub(crate) fn pop_undo(&mut self) -> Option<TransactionRecord> {
        self.undo_stack.pop()
    }

    pub(crate) fn push_redo(&mut self, record: TransactionRecord) {
        self.redo_stack.push(record);
    }

    pub(crate) fn push_redo_group(
        &mut self,
        id: TransactionId,
        changes: Vec<buffer::Change>,
        before: SelectionState,
        after: SelectionState,
    ) {
        self.redo_stack
            .push(TransactionRecord::new(id, changes, before, after));
    }

    pub(crate) fn pop_redo(&mut self) -> Option<TransactionRecord> {
        self.redo_stack.pop()
    }

    pub(crate) fn clear_last_edit_time(&mut self) {
        self.last_edit_time = None;
    }

    pub(crate) fn edit_within_grouping_window(&self, now: std::time::Instant) -> bool {
        self.last_edit_time
            .map(|time| now.duration_since(time).as_millis() < 300)
            .unwrap_or(false)
    }

    pub(crate) fn push_change(
        &mut self,
        change: buffer::Change,
        selection_state: SelectionState,
        now: std::time::Instant,
    ) {
        if let Some(record) = self.active_transaction.as_mut() {
            record.changes.push(change);
            record.after = selection_state;
            return;
        }

        self.redo_stack.clear();

        let is_single_char_insert =
            change.new_text.chars().count() == 1 && change.old_text.is_empty();
        let is_single_char_delete =
            change.old_text.chars().count() == 1 && change.new_text.is_empty();

        if (is_single_char_insert || is_single_char_delete)
            && self.edit_within_grouping_window(now)
            && let Some(last_record) = self.undo_stack.last_mut()
        {
            let last_is_same_kind = last_record
                .changes
                .last()
                .map(|last| {
                    if is_single_char_insert {
                        last.is_insertion()
                    } else {
                        last.is_deletion()
                    }
                })
                .unwrap_or(false);

            if last_is_same_kind {
                last_record.changes.push(change);
                last_record.after = selection_state;
                self.last_edit_time = Some(now);
                return;
            }
        }

        self.undo_stack.push(TransactionRecord::new(
            TransactionId(0),
            vec![change],
            selection_state.clone(),
            selection_state,
        ));
        self.last_edit_time = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::{EditorHistoryState, TransactionRecord};
    use crate::{
        Cursor, Position, Selection, SelectionState, SelectionsCollection, TransactionId, buffer,
    };
    use std::time::{Duration, Instant};

    fn record(id: u64) -> TransactionRecord {
        TransactionRecord::new(
            TransactionId(id),
            Vec::new(),
            SelectionState::new(SelectionsCollection::single(
                Cursor::at(Position::zero()),
                Selection::new(),
            )),
            SelectionState::new(SelectionsCollection::single(
                Cursor::at(Position::zero()),
                Selection::new(),
            )),
        )
    }

    #[test]
    fn undo_and_redo_helpers_preserve_stack_order() {
        let mut state = EditorHistoryState::default();

        state.push_undo(record(1));
        state.push_undo(record(2));
        state.push_redo(record(3));

        assert_eq!(state.pop_undo().expect("undo").id(), TransactionId(2));
        assert_eq!(state.pop_undo().expect("undo").id(), TransactionId(1));
        assert_eq!(state.pop_redo().expect("redo").id(), TransactionId(3));
    }

    #[test]
    fn undo_and_redo_group_helpers_create_records() {
        let mut state = EditorHistoryState::default();
        let before = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::zero()),
            Selection::new(),
        ));
        let after = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::new(),
        ));

        state.push_undo_group(
            TransactionId(4),
            vec![buffer::Change::insert(0, "a")],
            before.clone(),
            after.clone(),
        );
        state.push_redo_group(
            TransactionId(5),
            vec![buffer::Change::delete(0, "a")],
            after.clone(),
            before.clone(),
        );

        let undo = state.pop_undo().expect("undo");
        assert_eq!(undo.id(), TransactionId(4));
        assert_eq!(undo.changes().len(), 1);
        assert_eq!(undo.before(), &before);
        assert_eq!(undo.after(), &after);

        let redo = state.pop_redo().expect("redo");
        assert_eq!(redo.id(), TransactionId(5));
        assert_eq!(redo.changes().len(), 1);
    }

    #[test]
    fn begin_active_transaction_records_initial_selection() {
        let mut state = EditorHistoryState::default();
        let selection_state = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::zero()),
            Selection::new(),
        ));

        state.begin_active_transaction(TransactionId(7), selection_state.clone());
        assert_eq!(
            state.active_transaction_id().expect("active"),
            TransactionId(7)
        );
        let active = state.active_transaction.as_ref().expect("active");
        assert_eq!(active.before(), &selection_state);
        assert_eq!(active.after(), &selection_state);
        assert!(active.changes().is_empty());
    }

    #[test]
    fn edit_grouping_window_tracks_recent_edits() {
        let mut state = EditorHistoryState::default();
        let now = Instant::now();

        assert!(!state.edit_within_grouping_window(now));

        state.last_edit_time = Some(now - Duration::from_millis(299));
        assert!(state.edit_within_grouping_window(now));

        state.last_edit_time = Some(now - Duration::from_millis(300));
        assert!(!state.edit_within_grouping_window(now));

        state.clear_last_edit_time();
        assert!(!state.edit_within_grouping_window(now));
    }

    #[test]
    fn push_change_groups_same_kind_single_character_edits() {
        let mut state = EditorHistoryState::default();
        let now = Instant::now();
        let selection_state = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::zero()),
            Selection::new(),
        ));

        state.push_change(buffer::Change::insert(0, "a"), selection_state.clone(), now);
        state.push_change(
            buffer::Change::insert(1, "b"),
            selection_state.clone(),
            now + Duration::from_millis(10),
        );

        let record = state.pop_undo().expect("undo record");
        assert_eq!(record.changes().len(), 2);
        assert_eq!(record.changes()[0].new_text, "a");
        assert_eq!(record.changes()[1].new_text, "b");
    }

    #[test]
    fn push_change_starts_new_group_for_direction_change_or_timeout() {
        let mut state = EditorHistoryState::default();
        let now = Instant::now();
        let selection_state = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::zero()),
            Selection::new(),
        ));

        state.push_change(buffer::Change::insert(0, "a"), selection_state.clone(), now);
        state.push_change(
            buffer::Change::delete(0, "a"),
            selection_state.clone(),
            now + Duration::from_millis(10),
        );
        state.push_change(
            buffer::Change::insert(0, "b"),
            selection_state.clone(),
            now + Duration::from_millis(310),
        );

        assert_eq!(state.pop_undo().expect("third").changes().len(), 1);
        assert_eq!(state.pop_undo().expect("second").changes().len(), 1);
        assert_eq!(state.pop_undo().expect("first").changes().len(), 1);
    }

    #[test]
    fn push_change_appends_to_active_transaction() {
        let mut state = EditorHistoryState::default();
        let mut active = record(9);
        active.changes.push(buffer::Change::insert(0, "a"));
        state.active_transaction = Some(active);
        let selection_state = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::new(),
        ));

        state.push_change(
            buffer::Change::insert(1, "b"),
            selection_state,
            Instant::now(),
        );

        let active = state
            .active_transaction
            .as_ref()
            .expect("active transaction");
        assert_eq!(active.changes().len(), 2);
        assert_eq!(active.changes()[1].new_text, "b");
        assert!(state.pop_undo().is_none());
    }

    #[test]
    fn finish_active_transaction_moves_matching_record_to_undo() {
        let mut state = EditorHistoryState::default();
        let mut active = record(11);
        active.changes.push(buffer::Change::insert(0, "a"));
        state.active_transaction = Some(active);
        state.push_redo(record(99));
        state.last_edit_time = Some(Instant::now());
        let after = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::at(Position::new(0, 1)),
        ));

        assert!(state.finish_active_transaction(TransactionId(11), after.clone()));
        assert!(state.pop_redo().is_none());
        assert!(!state.edit_within_grouping_window(Instant::now()));
        let record = state.pop_undo().expect("undo");
        assert_eq!(record.id(), TransactionId(11));
        assert_eq!(record.after(), &after);
        assert!(state.active_transaction_id().is_none());
    }

    #[test]
    fn finish_active_transaction_preserves_mismatched_record() {
        let mut state = EditorHistoryState {
            active_transaction: Some(record(12)),
            ..Default::default()
        };
        let after = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::at(Position::new(0, 1)),
        ));

        assert!(!state.finish_active_transaction(TransactionId(13), after));
        assert_eq!(
            state.active_transaction_id().expect("active"),
            TransactionId(12)
        );
        assert!(state.pop_undo().is_none());
    }
}
