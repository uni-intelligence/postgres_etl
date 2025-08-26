use deltalake::arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::record_batch::RecordBatch;
use etl::types::{Cell, TableRow, TableSchema};
use std::sync::Arc;
/// Converts TableRows to Arrow RecordBatch for Delta Lake writes
pub struct TableRowEncoder;

impl TableRowEncoder {
    /// Convert a batch of TableRows to Arrow RecordBatch
    pub fn encode_table_rows(
        table_schema: &TableSchema,
        table_rows: Vec<TableRow>,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        if table_rows.is_empty() {
            return Ok(vec![]);
        }

        let record_batch = Self::table_rows_to_record_batch(table_schema, table_rows)?;
        Ok(vec![record_batch])
    }

    /// Convert TableRows to a single RecordBatch
    fn table_rows_to_record_batch(
        table_schema: &TableSchema,
        table_rows: Vec<TableRow>,
    ) -> Result<RecordBatch, ArrowError> {
        let arrays = Self::convert_columns_to_arrays(table_schema, &table_rows)?;

        // Create Arrow schema that MATCHES the actual array types we generated
        let fields: Vec<Field> = table_schema
            .column_schemas
            .iter()
            .zip(arrays.iter())
            .map(|(col_schema, array)| {
                Field::new(
                    &col_schema.name,
                    array.data_type().clone(),
                    col_schema.nullable,
                )
            })
            .collect();

        let arrow_schema = Schema::new(fields);

        let result = RecordBatch::try_new(Arc::new(arrow_schema), arrays);

        result
    }

    /// Convert Delta schema to Arrow schema
    fn delta_schema_to_arrow(
        delta_schema: &deltalake::kernel::StructType,
    ) -> Result<Schema, ArrowError> {
        // For now, create a simple Arrow schema based on the table structure
        // This is a stub implementation - in a full implementation, you'd properly
        // convert Delta schema types to Arrow types
        let fields: Vec<Field> = delta_schema
            .fields()
            .map(|field| {
                // Convert Delta DataType to Arrow DataType
                let arrow_type = match field.data_type() {
                    &deltalake::kernel::DataType::BOOLEAN => DataType::Boolean,
                    &deltalake::kernel::DataType::STRING => DataType::Utf8,
                    &deltalake::kernel::DataType::INTEGER => DataType::Int32,
                    &deltalake::kernel::DataType::LONG => DataType::Int64,
                    &deltalake::kernel::DataType::SHORT => DataType::Int16,
                    &deltalake::kernel::DataType::FLOAT => DataType::Float32,
                    &deltalake::kernel::DataType::DOUBLE => DataType::Float64,
                    &deltalake::kernel::DataType::DATE => DataType::Date32,
                    &deltalake::kernel::DataType::TIMESTAMP => DataType::Timestamp(
                        deltalake::arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
                    &deltalake::kernel::DataType::TIMESTAMP_NTZ => DataType::Timestamp(
                        deltalake::arrow::datatypes::TimeUnit::Microsecond,
                        None,
                    ),
                    &deltalake::kernel::DataType::BINARY => DataType::Binary,
                    // Default to string for complex/unsupported types
                    _ => DataType::Utf8,
                };

                Field::new(field.name(), arrow_type, field.is_nullable())
            })
            .collect();

        Ok(Schema::new(fields))
    }

    /// Convert table columns to Arrow arrays
    fn convert_columns_to_arrays(
        table_schema: &TableSchema,
        table_rows: &[TableRow],
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        let mut arrays = Vec::new();

        for (col_idx, _col_schema) in table_schema.column_schemas.iter().enumerate() {
            let column_data: Vec<&Cell> =
                table_rows.iter().map(|row| &row.values[col_idx]).collect();

            let array = Self::convert_cell_column_to_array(column_data)?;
            arrays.push(array);
        }

        Ok(arrays)
    }

    /// Convert a column of Cells to an Arrow array based on the first non-null value's type
    fn convert_cell_column_to_array(cells: Vec<&Cell>) -> Result<ArrayRef, ArrowError> {
        if cells.is_empty() {
            return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())));
        }

        // Determine the column type from the first non-null cell
        let first_non_null = cells.iter().find(|cell| !matches!(cell, Cell::Null));

        match first_non_null {
            Some(Cell::Bool(_)) => {
                let bool_values: Vec<Option<bool>> = cells
                    .iter()
                    .map(|cell| match cell {
                        Cell::Null => None,
                        Cell::Bool(b) => Some(*b),
                        _ => None, // Invalid conversion, treat as null
                    })
                    .collect();
                Ok(Arc::new(BooleanArray::from(bool_values)))
            }
            Some(Cell::I32(_)) => {
                let int_values: Vec<Option<i32>> = cells
                    .iter()
                    .map(|cell| match cell {
                        Cell::Null => None,
                        Cell::I32(i) => Some(*i),
                        Cell::I16(i) => Some(*i as i32),
                        Cell::U32(i) => Some(*i as i32),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(int_values)))
            }
            Some(Cell::I16(_)) => {
                let int_values: Vec<Option<i32>> = cells
                    .iter()
                    .map(|cell| match cell {
                        Cell::Null => None,
                        Cell::I16(i) => Some(*i as i32),
                        Cell::I32(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(int_values)))
            }
            _ => {
                // For all other types (String, Numeric, etc.), convert to string
                let string_values: Vec<Option<String>> = cells
                    .iter()
                    .map(|cell| match cell {
                        Cell::Null => None,
                        Cell::Bool(b) => Some(b.to_string()),
                        Cell::String(s) => Some(s.clone()),
                        Cell::I16(i) => Some(i.to_string()),
                        Cell::I32(i) => Some(i.to_string()),
                        Cell::U32(i) => Some(i.to_string()),
                        Cell::I64(i) => Some(i.to_string()),
                        Cell::F32(f) => Some(f.to_string()),
                        Cell::F64(f) => Some(f.to_string()),
                        Cell::Numeric(n) => Some(n.to_string()),
                        Cell::Date(d) => Some(d.to_string()),
                        Cell::Time(t) => Some(t.to_string()),
                        Cell::Timestamp(ts) => Some(ts.to_string()),
                        Cell::TimestampTz(ts) => Some(ts.to_string()),
                        Cell::Uuid(u) => Some(u.to_string()),
                        Cell::Json(j) => Some(j.to_string()),
                        Cell::Bytes(b) => Some(format!("{b:?}")),
                        Cell::Array(a) => Some(format!("{a:?}")),
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(string_values)))
            }
        }
    }

    /// Convert Cell values to specific Arrow array types
    fn convert_bool_column(cells: Vec<&Cell>) -> Result<ArrayRef, ArrowError> {
        // todo(abhi): Extract boolean values from cells, handle nulls
        let values: Vec<Option<bool>> = cells
            .iter()
            .map(|cell| match cell {
                Cell::Bool(b) => Some(*b),
                Cell::Null => None,
                _ => None, // todo(abhi): Handle type mismatch errors
            })
            .collect();

        Ok(Arc::new(BooleanArray::from(values)))
    }

    fn convert_string_column(cells: Vec<&Cell>) -> Result<ArrayRef, ArrowError> {
        // todo(abhi): Extract string values from cells, handle nulls and conversions
        let values: Vec<Option<String>> = cells
            .iter()
            .map(|cell| match cell {
                Cell::String(s) => Some(s.clone()),
                Cell::Null => None,
                // todo(abhi): Handle UUID, JSON as strings
                _ => None,
            })
            .collect();

        Ok(Arc::new(StringArray::from(values)))
    }

    fn convert_int32_column(cells: Vec<&Cell>) -> Result<ArrayRef, ArrowError> {
        // todo(abhi): Extract i32 values from cells, handle nulls and conversions
        let values: Vec<Option<i32>> = cells
            .iter()
            .map(|cell| match cell {
                Cell::I32(i) => Some(*i),
                Cell::Null => None,
                // todo(abhi): Handle I16 -> I32 conversion, U32 overflow checks
                _ => None,
            })
            .collect();

        Ok(Arc::new(Int32Array::from(values)))
    }

    fn convert_array_column(cells: Vec<&Cell>) -> Result<ArrayRef, ArrowError> {
        // todo(abhi): Convert ArrayCell variants to Arrow ListArray
        // todo(abhi): Handle nested arrays properly with element type detection
        // todo(abhi): This is complex - arrays need proper element type handling

        // Stub implementation - convert to string array for now
        let values: Vec<Option<String>> = cells
            .iter()
            .map(|cell| match cell {
                Cell::Array(arr) => Some(format!("{arr:?}")),
                Cell::Null => None,
                _ => None,
            })
            .collect();

        Ok(Arc::new(StringArray::from(values)))
    }

    /// Estimate the size in bytes of a RecordBatch
    pub fn estimate_record_batch_size(record_batch: &RecordBatch) -> usize {
        // todo(abhi): Implement accurate size estimation
        // todo(abhi): Sum up array sizes, consider compression
        record_batch.num_rows() * record_batch.num_columns() * 8 // rough estimate
    }

    /// Split TableRows into chunks targeting a specific file size
    pub fn chunk_table_rows(
        table_rows: Vec<TableRow>,
        target_size_mb: usize,
    ) -> Vec<Vec<TableRow>> {
        // todo(abhi): Implement intelligent chunking
        // todo(abhi): Estimate row size and chunk accordingly
        // todo(abhi): Consider maintaining some minimum/maximum chunk sizes

        if table_rows.is_empty() {
            return vec![];
        }

        let target_size_bytes = target_size_mb * 1024 * 1024;
        let estimated_row_size = 100; // todo(abhi): Better row size estimation
        let rows_per_chunk = (target_size_bytes / estimated_row_size).max(1);

        table_rows
            .chunks(rows_per_chunk)
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::{ColumnSchema, TableName, TableSchema};

    #[test]
    fn test_empty_table_rows() {
        let schema = create_test_schema();
        let result = TableRowEncoder::encode_table_rows(&schema, vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_chunk_table_rows() {
        let rows = vec![
            TableRow::new(vec![Cell::I32(1)]),
            TableRow::new(vec![Cell::I32(2)]),
            TableRow::new(vec![Cell::I32(3)]),
        ];

        let chunks = TableRowEncoder::chunk_table_rows(rows, 1);
        assert!(!chunks.is_empty());
        // todo(abhi): Add more specific assertions about chunk sizes
    }

    fn create_test_schema() -> TableSchema {
        TableSchema {
            id: etl::types::TableId(1),
            name: TableName::new("public".to_string(), "test_table".to_string()),
            column_schemas: vec![ColumnSchema::new(
                "id".to_string(),
                etl::types::Type::INT4,
                -1,
                false,
                true,
            )],
        }
    }
}
