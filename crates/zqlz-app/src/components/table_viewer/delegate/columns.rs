use super::*;
use zqlz_ui::widgets::PixelsExt;

impl TableViewerDelegate {
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

        let sample_size = self.rows.len().min(100);
        let mut content_widths: Vec<f32> = Vec::with_capacity(sample_size);

        let step = if self.rows.len() > sample_size {
            self.rows.len() / sample_size
        } else {
            1
        };

        let mut index = 0;
        while index < self.rows.len() && content_widths.len() < sample_size {
            if let Some(value) = self.rows[index].get(data_col_ix) {
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

        let content_width = if content_widths.is_empty() {
            0.0
        } else {
            content_widths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let p90_index = ((content_widths.len() as f32 * 0.9) as usize)
                .min(content_widths.len().saturating_sub(1));
            content_widths[p90_index]
        };

        let optimal = header_width.max(content_width);
        optimal.clamp(80.0, 900.0)
    }

    fn estimate_text_width(text: &str, base_width: f32) -> f32 {
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
