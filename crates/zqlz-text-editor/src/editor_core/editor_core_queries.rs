use super::*;

impl<'a> EditorCore<'a> {
    pub fn find_all_occurrences(&self, needle: &str) -> Vec<std::ops::Range<usize>> {
        if needle.is_empty() {
            return Vec::new();
        }

        SearchEngine::new(
            needle,
            &TextFindOptions {
                case_sensitive: true,
                whole_word: false,
                regex: false,
            },
        )
        .map(|engine| {
            engine
                .find_all_in_rope(&self.buffer.rope())
                .into_iter()
                .map(|matched| matched.range())
                .collect()
        })
        .unwrap_or_default()
    }

    pub fn find_word_range_at_offset(&self, offset: usize) -> Option<std::ops::Range<usize>> {
        self.find_word_range_at_offset_with_extra_chars(offset, &[])
    }

    pub fn find_word_range_at_offset_with_extra_chars(
        &self,
        offset: usize,
        extra_word_chars: &[char],
    ) -> Option<std::ops::Range<usize>> {
        if offset > self.buffer.len() {
            return None;
        }

        let offset = self.buffer.floor_char_boundary(offset);

        if matches!(self.buffer.char_at(offset), Some('"') | Some('`')) {
            let quote = self.buffer.char_at(offset)?;
            let quote_end = self.buffer.next_char_boundary(offset).ok()?;

            let mut search_offset = quote_end;
            while let Some((start, end, character)) =
                Self::char_at_offset(self.buffer, search_offset)
            {
                if character == quote {
                    return Some(offset..end);
                }
                search_offset = end.max(start + character.len_utf8());
            }

            let mut search_offset = offset;
            while let Some((start, _, character)) =
                Self::char_before_offset(self.buffer, search_offset)
            {
                if character == quote {
                    return Some(start..quote_end);
                }
                search_offset = start;
            }
        }

        if self.buffer.char_at(offset) == Some('[') {
            let close_end = self
                .scan_forward_for_character(offset, ']')
                .and_then(|close_start| self.buffer.next_char_boundary(close_start).ok());
            if let Some(close_end) = close_end {
                return Some(offset..close_end);
            }
        }

        if self.buffer.char_at(offset) == Some(']') {
            let close_end = self.buffer.next_char_boundary(offset).ok()?;
            if let Some(open_start) = self.scan_backward_for_character(offset, '[') {
                return Some(open_start..close_end);
            }
        }

        if let Some(open_start) = self.scan_backward_for_character(offset, '[')
            && let Some(close_start) = self.scan_forward_for_character(open_start, ']')
            && offset <= close_start
        {
            let close_end = self.buffer.next_char_boundary(close_start).ok()?;
            return Some(open_start..close_end);
        }

        let mut start = offset;
        while let Some((previous_start, _, character)) =
            Self::char_before_offset(self.buffer, start)
        {
            if zqlz_core::syntax_completion_word_char(character, extra_word_chars) {
                start = previous_start;
            } else {
                break;
            }
        }

        let mut end = offset;
        while let Some((_, next_end, character)) = Self::char_at_offset(self.buffer, end) {
            if zqlz_core::syntax_completion_word_char(character, extra_word_chars) {
                end = next_end;
            } else {
                break;
            }
        }

        (start < end).then_some(start..end)
    }

    fn scan_forward_for_character(&self, offset: usize, target: char) -> Option<usize> {
        let mut search_offset = offset;
        while let Some((start, end, character)) = Self::char_at_offset(self.buffer, search_offset) {
            if character == target {
                return Some(start);
            }
            if matches!(character, '\n' | '\r') {
                return None;
            }
            search_offset = end.max(start + character.len_utf8());
        }
        None
    }

    fn scan_backward_for_character(&self, offset: usize, target: char) -> Option<usize> {
        let mut search_offset = offset;
        while let Some((start, _, character)) = Self::char_before_offset(self.buffer, search_offset)
        {
            if character == target {
                return Some(start);
            }
            if matches!(character, '\n' | '\r') {
                return None;
            }
            search_offset = start;
        }
        None
    }

    pub fn selection_or_word_under_cursor_text(&self) -> Option<String> {
        if let Some(SelectedTextPlan::Linear(text)) = self.selected_text_plan() {
            return Some(text);
        }

        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let range = self.find_word_range_at_offset(cursor_offset)?;
        self.buffer.text_for_range(range).ok()
    }

    pub fn completion_query_plan(&self) -> CompletionQueryPlan {
        self.completion_query_plan_with_extra_chars(&[])
    }

    pub fn completion_query_plan_with_extra_chars(
        &self,
        extra_word_chars: &[char],
    ) -> CompletionQueryPlan {
        let cursor_offset = self.cursor.offset(self.buffer);
        let trigger_offset = self
            .find_word_range_at_offset_with_extra_chars(cursor_offset, extra_word_chars)
            .map(|range| range.start)
            .unwrap_or(cursor_offset);
        let current_prefix = self
            .buffer
            .text_for_range(trigger_offset..cursor_offset)
            .unwrap_or_default();

        CompletionQueryPlan {
            trigger_offset,
            current_prefix,
        }
    }

    pub fn word_target_at_offset(&self, offset: usize) -> Option<WordTarget> {
        let range = self.find_word_range_at_offset(offset)?;
        let text = self.buffer.text_for_range(range.clone()).ok()?;
        Some(WordTarget { range, text })
    }

    pub fn rename_target_at_cursor(&self) -> Option<WordTarget> {
        let cursor_offset = self.cursor.offset(self.buffer);
        self.word_target_at_offset(cursor_offset)
    }

    pub fn signature_help_query_plan(
        &self,
        structural_open_paren: Option<usize>,
    ) -> Option<SignatureHelpQueryPlan> {
        let cursor_offset = self.cursor.offset(self.buffer);
        let search_start = cursor_offset.saturating_sub(256);
        let search_end = (cursor_offset + 256).min(self.buffer.len());
        let text = self.buffer.text_for_range(search_start..search_end).ok()?;
        let local_cursor_offset = cursor_offset.saturating_sub(search_start).min(text.len());
        let before_cursor = &text[..local_cursor_offset];

        let mut depth = 0usize;
        let mut call_open_paren = None;
        for (index, character) in before_cursor.char_indices().rev() {
            match character {
                ')' => depth = depth.saturating_add(1),
                '(' => {
                    if depth == 0 {
                        call_open_paren = Some(index);
                        break;
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }

        let open_paren = structural_open_paren
            .map(|offset| offset.saturating_sub(search_start))
            .or(call_open_paren)?;

        let before_paren = before_cursor[..open_paren].trim_end();
        let name_end = before_paren.len();
        let mut name_start = name_end;
        for (index, character) in before_paren.char_indices().rev() {
            if character.is_alphanumeric() || character == '_' {
                name_start = index;
            } else {
                break;
            }
        }
        if name_start >= name_end {
            return None;
        }

        let function_name = before_paren[name_start..name_end].to_string();
        let mut nested = 0usize;
        let mut active_parameter = 0u32;
        for character in before_cursor[open_paren + 1..].chars() {
            match character {
                '(' => nested = nested.saturating_add(1),
                ')' => nested = nested.saturating_sub(1),
                ',' if nested == 0 => active_parameter = active_parameter.saturating_add(1),
                _ => {}
            }
        }

        Some(SignatureHelpQueryPlan {
            function_name,
            active_parameter,
        })
    }

    pub fn rename_query_plan(&self, new_name: &str) -> Option<RenameQueryPlan> {
        if !Self::is_valid_identifier(new_name) {
            return None;
        }

        let WordTarget {
            range: _range,
            text: current_name,
        } = self.rename_target_at_cursor()?;
        if current_name == new_name {
            return None;
        }

        let ranges = Self::find_identifier_occurrences(self.buffer, &current_name);
        if ranges.is_empty() {
            return None;
        }

        Some(RenameQueryPlan {
            current_name,
            ranges,
        })
    }

    fn is_valid_identifier(name: &str) -> bool {
        let mut characters = name.chars();
        let Some(first) = characters.next() else {
            return false;
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    }

    fn find_identifier_occurrences(
        buffer: &TextBuffer,
        identifier: &str,
    ) -> Vec<std::ops::Range<usize>> {
        if identifier.is_empty() {
            return Vec::new();
        }

        SearchEngine::new(
            identifier,
            &TextFindOptions {
                case_sensitive: true,
                whole_word: true,
                regex: false,
            },
        )
        .map(|engine| {
            engine
                .find_all_in_rope(&buffer.rope())
                .into_iter()
                .map(|matched| matched.range())
                .collect()
        })
        .unwrap_or_default()
    }
}
