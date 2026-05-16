use super::*;

impl TableViewerDelegate {
    pub(in crate::components::table_viewer) fn apply_key_value_database_column_widths(
        columns: &mut [Column],
        column_meta: &[ColumnMeta],
    ) {
        let is_key_value_database_view = column_meta.len() == 5
            && column_meta[0].name == "Key"
            && column_meta[1].name == "Type"
            && column_meta[2].name == "Value"
            && column_meta[3].name == "Size"
            && column_meta[4].name == "TTL";

        if !is_key_value_database_view {
            return;
        }

        for (data_col_ix, width) in [360.0, 150.0, 560.0, 150.0, 150.0].into_iter().enumerate() {
            if let Some(column) = columns.get_mut(data_col_ix + 1) {
                *column = column.clone().width(column.width.as_f32().max(width));
            }
        }
    }

    pub fn freeze_column(&mut self, col_ix: usize) {
        if col_ix > 0 && col_ix < self.columns.len() {
            self.columns[col_ix] = self.columns[col_ix].clone().fixed(ColumnFixed::Left);
            tracing::info!("Column {} frozen", col_ix);
        }
    }

    pub fn unfreeze_column(&mut self, col_ix: usize) {
        if col_ix > 0 && col_ix < self.columns.len() {
            let mut col = self.columns[col_ix].clone();
            col.fixed = None;
            self.columns[col_ix] = col;
            tracing::info!("Column {} unfrozen", col_ix);
        }
    }

    pub fn calculate_column_width(&self, col_ix: usize) -> f32 {
        if col_ix == 0 {
            return Self::row_number_column_width(self.row_offset + self.rows.len());
        }

        let data_col_ix = col_ix - 1;

        let header_label = self
            .columns
            .get(col_ix)
            .map(|column| column.name.as_ref())
            .or_else(|| {
                self.column_meta
                    .get(data_col_ix)
                    .map(|meta| meta.name.as_str())
            })
            .unwrap_or("Column");

        let cell_padding = self.size.table_cell_padding();
        let horizontal_padding = cell_padding.left.as_f32() + cell_padding.right.as_f32();

        let shows_primary_key_icon = self
            .column_meta
            .get(data_col_ix)
            .is_some_and(|meta| self.primary_key_columns.contains(&meta.name));
        let shows_foreign_key_icon = self.fk_by_column.contains_key(&data_col_ix);
        let shows_nullable_badge = self
            .column_meta
            .get(data_col_ix)
            .is_some_and(|meta| meta.nullable);

        let mut header_chrome_width = horizontal_padding + 30.0;
        if shows_primary_key_icon {
            header_chrome_width += 18.0;
        }
        if shows_foreign_key_icon {
            header_chrome_width += 18.0;
        }
        if shows_nullable_badge {
            header_chrome_width += 28.0;
        }

        let header_width = Self::estimate_text_width(header_label, 8.8) + header_chrome_width;
        let type_width = self
            .column_meta
            .get(data_col_ix)
            .map(|metadata| {
                let presentation = Self::type_presentation_for_meta(metadata);
                Self::estimate_text_width(&presentation.label, 7.2) + horizontal_padding + 22.0
            })
            .unwrap_or(0.0);

        let content_width = Self::sample_content_width(&self.rows, data_col_ix, horizontal_padding);

        let optimal = header_width.max(type_width).max(content_width);
        optimal.clamp(80.0, 900.0)
    }

    pub(in crate::components::table_viewer) fn calculate_initial_column_width(
        data_col_ix: usize,
        metadata: &ColumnMeta,
        rows: &[Vec<Value>],
    ) -> f32 {
        let horizontal_padding = 18.0;
        let header_width =
            Self::estimate_text_width(&metadata.name, 8.8) + horizontal_padding + 30.0;
        let presentation = Self::type_presentation_for_meta(metadata);
        let type_width =
            Self::estimate_text_width(&presentation.label, 7.2) + horizontal_padding + 22.0;
        let content_width = Self::sample_content_width(rows, data_col_ix, horizontal_padding);

        header_width
            .max(type_width)
            .max(content_width)
            .clamp(80.0, 900.0)
    }

    fn sample_content_width(
        rows: &[Vec<Value>],
        data_col_ix: usize,
        horizontal_padding: f32,
    ) -> f32 {
        let sample_size = rows.len().min(100);
        let mut content_widths: Vec<f32> = Vec::with_capacity(sample_size);

        let step = if rows.len() > sample_size {
            rows.len() / sample_size
        } else {
            1
        };

        let mut index = 0;
        while index < rows.len() && content_widths.len() < sample_size {
            if let Some(value) = rows[index].get(data_col_ix) {
                let display = value.display_for_table();
                let first_line = display.lines().next().unwrap_or(&display);
                let measured = if first_line.chars().count() > 60 {
                    first_line.chars().take(60).collect::<String>()
                } else {
                    first_line.to_string()
                };
                let width = Self::estimate_text_width(&measured, 7.4) + horizontal_padding + 12.0;
                content_widths.push(width);
            }
            index += step;
        }

        if content_widths.is_empty() {
            return 0.0;
        }

        content_widths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p90_index = ((content_widths.len() as f32 * 0.9) as usize)
            .min(content_widths.len().saturating_sub(1));
        content_widths[p90_index]
    }

    pub(super) fn estimate_text_width(text: &str, base_width: f32) -> f32 {
        text.chars()
            .map(|c| match c {
                'i' | 'l' | 'j' | '!' | '|' | '.' | ',' | ':' | ';' | '\'' | '1' => {
                    base_width * 0.5
                }
                'f' | 'r' | 't' => base_width * 0.65,
                'm' | 'w' | 'M' | 'W' | 'Q' | 'O' | '@' => base_width * 1.3,
                _ if c.is_uppercase() => base_width * 1.1,
                _ => base_width,
            })
            .sum()
    }

    pub fn row_number_column_width(max_row_number: usize) -> f32 {
        let digit_count = if max_row_number == 0 {
            1
        } else {
            (max_row_number as f64).log10().floor() as u32 + 1
        };
        let computed = digit_count as f32 * 8.0 + 44.0;
        computed.max(50.0)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(name: &str, data_type: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            ordinal: 0,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        }
    }

    #[::core::prelude::v1::test]
    fn initial_width_expands_for_long_column_name() {
        let width = TableViewerDelegate::calculate_initial_column_width(
            0,
            &metadata("very_long_customer_reference_identifier", "uuid"),
            &[],
        );

        assert!(width > 150.0);
    }

    #[::core::prelude::v1::test]
    fn initial_width_accounts_for_type_label() {
        let mut column = metadata("amount", "numeric");
        column.precision = Some(18);
        column.scale = Some(4);

        let width = TableViewerDelegate::calculate_initial_column_width(0, &column, &[]);
        let name_only = TableViewerDelegate::estimate_text_width("amount", 8.8) + 48.0;

        assert!(width > name_only);
    }

    #[::core::prelude::v1::test]
    fn row_number_width_unchanged() {
        assert_eq!(TableViewerDelegate::row_number_column_width(0), 52.0);
        assert_eq!(TableViewerDelegate::row_number_column_width(999), 68.0);
    }

    #[::core::prelude::v1::test]
    fn key_value_database_columns_use_wide_defaults() {
        let mut columns = vec![
            Column::new("row-num", "#").width(52.0),
            Column::new("key", "Key").width(80.0),
            Column::new("type", "Type").width(80.0),
            Column::new("value", "Value").width(80.0),
            Column::new("size", "Size").width(80.0),
            Column::new("ttl", "TTL").width(80.0),
        ];
        let metadata = vec![
            metadata("Key", "key name"),
            metadata("Type", "data type"),
            metadata("Value", "value preview"),
            metadata("Size", "memory size"),
            metadata("TTL", "time to live"),
        ];

        TableViewerDelegate::apply_key_value_database_column_widths(&mut columns, &metadata);

        assert_eq!(columns[1].width.as_f32(), 360.0);
        assert_eq!(columns[2].width.as_f32(), 150.0);
        assert_eq!(columns[3].width.as_f32(), 560.0);
        assert_eq!(columns[4].width.as_f32(), 150.0);
        assert_eq!(columns[5].width.as_f32(), 150.0);
    }
}
