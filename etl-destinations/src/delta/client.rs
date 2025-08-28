use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::schema::postgres_to_delta_schema;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::{DeltaOps, DeltaResult, DeltaTable, DeltaTableBuilder, open_table};
use etl::types::{TableSchema, TableRow, Cell};

/// Client for connecting to Delta Lake tables.
#[derive(Clone)]
pub struct DeltaLakeClient {
    storage_options: Option<HashMap<String, String>>,
}

impl Default for DeltaLakeClient {
    fn default() -> Self {
        Self::new(None)
    }
}

impl DeltaLakeClient {
    /// Create a new client.
    pub fn new(storage_options: Option<HashMap<String, String>>) -> Self {
        Self { storage_options }
    }

    fn get_table_with_storage_options(&self, table_uri: &str) -> DeltaResult<DeltaTableBuilder> {
        let mut builder = DeltaTableBuilder::from_valid_uri(table_uri)?;
        if let Some(storage_options) = &self.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }
        Ok(builder)
    }

    /// Returns true if a Delta table exists at the given uri/path.
    pub async fn table_exists(&self, table_uri: &str) -> bool {
        let Ok(builder) = self.get_table_with_storage_options(table_uri) else {
            return false;
        };
        builder.load().await.is_ok()
    }

    /// Create a Delta table at `table_uri` if it doesn't exist, using the provided Postgres schema.
    pub async fn create_table_if_missing(
        &self,
        table_uri: &str,
        table_schema: &TableSchema,
    ) -> DeltaResult<Arc<DeltaTable>> {
        if let Ok(table) = open_table(table_uri).await {
            return Ok(Arc::new(table));
        }

        let delta_schema = postgres_to_delta_schema(table_schema)?;

        let ops = if let Some(storage_options) = &self.storage_options {
            DeltaOps::try_from_uri_with_storage_options(table_uri, storage_options.clone()).await?
        } else {
            DeltaOps::try_from_uri(table_uri).await?
        };

        let table = ops
            .create()
            // TODO(abhi): Figure out how to avoid the clone
            .with_columns(delta_schema.fields().cloned())
            .await?;

        Ok(Arc::new(table))
    }

    /// Open a Delta table at `table_uri`.
    pub async fn open_table(&self, table_uri: &str) -> DeltaResult<Arc<DeltaTable>> {
        let table = self
            .get_table_with_storage_options(table_uri)?
            .load()
            .await?;
        Ok(Arc::new(table))
    }

    /// Append RecordBatch to Delta table
    pub async fn append_to_table(
        &self,
        table: Arc<DeltaTable>,
        record_batches: Vec<RecordBatch>,
    ) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement append operation using delta-rs
        // todo(abhi): Handle partitioning if configured
        // todo(abhi): Use DeltaOps::write() with append mode

        if record_batches.is_empty() {
            return Ok(table);
        }

        let ops = DeltaOps::from(table.as_ref().clone());

        // todo(abhi): Configure write options (partition columns, etc.)
        let write_builder = ops.write(record_batches);

        // todo(abhi): Set up partitioning based on configuration
        // write_builder = write_builder.with_partition_columns(partition_columns);

        let table = write_builder.await?;

        Ok(Arc::new(table))
    }

    /// Delete rows from Delta table using a predicate
    pub async fn delete_rows_where(
        &self,
        table: Arc<DeltaTable>,
        predicate: &str,
    ) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement delete operation using delta-rs
        // todo(abhi): Build proper SQL predicate for primary key matching
        // todo(abhi): Handle composite primary keys

        let ops = DeltaOps::from(table.as_ref().clone());

        // todo(abhi): Use proper predicate syntax
        let table = ops.delete().with_predicate(predicate).await?;

        Ok(Arc::new(table.0))
    }

    /// Execute delete+append transaction atomically
    pub async fn delete_and_append_transaction(
        &self,
        table: Arc<DeltaTable>,
        delete_predicate: Option<&str>,
        record_batches: Vec<RecordBatch>,
        _app_transaction_id: Option<&str>,
    ) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement atomic delete+append transaction
        // todo(abhi): Use Delta transaction features for atomicity
        // todo(abhi): Include app-level transaction ID for idempotency

        let mut current_table = table;

        // First, delete if predicate is provided
        if let Some(predicate) = delete_predicate {
            current_table = self.delete_rows_where(current_table, predicate).await?;
        }

        // Then append new data
        if !record_batches.is_empty() {
            current_table = self.append_to_table(current_table, record_batches).await?;
        }

        // todo(abhi): Implement proper transaction with app ID
        // This should be done as a single atomic operation in the real implementation

        Ok(current_table)
    }

    /// Truncate table by removing all data
    pub async fn truncate_table(&self, table: Arc<DeltaTable>) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement atomic truncate operation
        // todo(abhi): Use delete with predicate "true" or recreate table

        let ops = DeltaOps::from(table.as_ref().clone());

        // Delete all rows using "true" predicate
        let table = ops.delete().with_predicate("true").await?;

        Ok(Arc::new(table.0))
    }

    /// Run OPTIMIZE operation on the table
    #[allow(unused)]
    pub async fn optimize_table(
        &self,
        table: Arc<DeltaTable>,
        z_order_columns: Option<&[String]>,
    ) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement OPTIMIZE operation for small file compaction
        // todo(abhi): Support Z-ordering if columns are specified
        // todo(abhi): Configure optimization parameters

        let ops = DeltaOps::from(table.as_ref().clone());

        // todo(abhi): Use optimize builder
        let optimize_builder = ops.optimize();

        // todo(abhi): Add Z-order columns if specified
        if let Some(columns) = z_order_columns {
            // optimize_builder = optimize_builder.with_z_order(columns);
        }

        // todo(abhi): Execute optimization
        // let table = optimize_builder.await?;

        // For now, return the original table
        Ok(table)
    }

    /// Add columns to existing table (schema evolution)
    pub async fn add_columns_to_table(
        &self,
        table: Arc<DeltaTable>,
        new_columns: &[(&str, &str)], // (column_name, data_type)
    ) -> DeltaResult<Arc<DeltaTable>> {
        // todo(abhi): Implement schema evolution - add missing columns
        // todo(abhi): All new columns should be nullable
        // todo(abhi): Use ALTER TABLE ADD COLUMN equivalent in delta-rs

        if new_columns.is_empty() {
            return Ok(table);
        }

        // todo(abhi): Check if columns already exist
        // todo(abhi): Add only missing columns
        // todo(abhi): Ensure all new columns are nullable

        // For now, return the original table
        Ok(table)
    }

    /// Build predicate string for primary key matching
    pub fn build_pk_predicate(
        &self,
        primary_keys: &HashSet<String>,
        pk_column_names: &[String],
    ) -> String {
        if primary_keys.is_empty() {
            return "false".to_string(); // No rows to match
        }

        if pk_column_names.is_empty() {
            return "false".to_string(); // No PK columns
        }

        if pk_column_names.len() == 1 {
            // Single column primary key: col IN ('val1', 'val2', ...)
            let pk_column = Self::escape_identifier(&pk_column_names[0]);
            let escaped_keys: Vec<String> = primary_keys
                .iter()
                .map(|k| Self::escape_string_literal(k))
                .collect();
            format!("{} IN ({})", pk_column, escaped_keys.join(", "))
        } else {
            // Composite primary key: (col1 = 'val1' AND col2 = 'val2') OR (col1 = 'val3' AND col2 = 'val4') ...
            let conditions: Vec<String> = primary_keys
                .iter()
                .map(|composite_key| {
                    let key_parts = Self::split_composite_key(composite_key);
                    if key_parts.len() != pk_column_names.len() {
                        // Malformed composite key, skip
                        return "false".to_string();
                    }
                    
                    let conditions: Vec<String> = pk_column_names
                        .iter()
                        .zip(key_parts.iter())
                        .map(|(col, val)| {
                            format!(
                                "{} = {}",
                                Self::escape_identifier(col),
                                Self::escape_string_literal(val)
                            )
                        })
                        .collect();
                    
                    format!("({})", conditions.join(" AND "))
                })
                .filter(|cond| cond != "false") // Remove malformed conditions
                .collect();

            if conditions.is_empty() {
                "false".to_string()
            } else {
                conditions.join(" OR ")
            }
        }
    }

    /// Generate app-level transaction ID for idempotency
    pub fn generate_app_transaction_id(
        &self,
        pipeline_id: &str,
        table_name: &str,
        sequence: u64,
    ) -> String {
        // todo(abhi): Generate unique transaction ID for Delta app-level deduplication
        // todo(abhi): Include pipeline ID, table name, and sequence number

        format!("etl-{pipeline_id}-{table_name}-{sequence}")
    }

    /// Check if table schema needs evolution
    pub async fn needs_schema_evolution(
        &self,
        table: &DeltaTable,
        expected_schema: &TableSchema,
    ) -> DeltaResult<Vec<String>> {
        // todo(abhi): Compare current Delta schema with expected schema
        // todo(abhi): Return list of missing columns that need to be added
        // todo(abhi): Validate that existing columns are compatible

        let _current_schema = table.snapshot()?.schema();
        let _expected_delta_schema = postgres_to_delta_schema(expected_schema)?;

        // todo(abhi): Compare schemas and find missing columns
        // todo(abhi): Ensure no incompatible changes (type changes, etc.)

        Ok(vec![]) // No missing columns for now
    }

    /// Extract primary key from a TableRow using the table schema
    pub fn extract_primary_key(
        &self,
        table_row: &TableRow,
        table_schema: &TableSchema,
    ) -> Result<String, String> {
        let pk_columns: Vec<&str> = table_schema
            .column_schemas
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if col.primary {
                    Some((idx, col.name.as_str()))
                } else {
                    None
                }
            })
            .map(|(_, name)| name)
            .collect();

        if pk_columns.is_empty() {
            return Err("No primary key columns found in table schema".to_string());
        }

        let pk_indices: Vec<usize> = table_schema
            .column_schemas
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| if col.primary { Some(idx) } else { None })
            .collect();

        if pk_indices.len() != pk_columns.len() {
            return Err("Mismatch between PK column count and indices".to_string());
        }

        // Check that all PK indices are within bounds
        for &idx in &pk_indices {
            if idx >= table_row.values.len() {
                return Err(format!(
                    "Primary key column index {} out of bounds for row with {} columns",
                    idx,
                    table_row.values.len()
                ));
            }
        }

        if pk_columns.len() == 1 {
            // Single column primary key
            let cell = &table_row.values[pk_indices[0]];
            Ok(Self::cell_to_string(cell))
        } else {
            // Composite primary key - join with delimiter
            let key_parts: Vec<String> = pk_indices
                .iter()
                .map(|&idx| Self::cell_to_string(&table_row.values[idx]))
                .collect();
            Ok(Self::join_composite_key(&key_parts))
        }
    }

    /// Convert a Cell to its string representation for primary key purposes
    fn cell_to_string(cell: &Cell) -> String {
        match cell {
            Cell::Null => "NULL".to_string(),
            Cell::Bool(b) => b.to_string(),
            Cell::String(s) => s.clone(),
            Cell::I16(i) => i.to_string(),
            Cell::I32(i) => i.to_string(),
            Cell::I64(i) => i.to_string(),
            Cell::U32(i) => i.to_string(),
            Cell::F32(f) => f.to_string(),
            Cell::F64(f) => f.to_string(),
            Cell::Numeric(n) => n.to_string(),
            Cell::Date(d) => d.to_string(),
            Cell::Time(t) => t.to_string(),
            Cell::Timestamp(ts) => ts.to_string(),
            Cell::TimestampTz(ts) => ts.to_string(),
            Cell::Uuid(u) => u.to_string(),
            Cell::Json(j) => j.to_string(),
            Cell::Bytes(b) => {
                let hex_string: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("\\x{}", hex_string)
            },
            Cell::Array(_) => "[ARRAY]".to_string(), // Arrays shouldn't be PKs
        }
    }

    /// Join composite key parts with a delimiter
    const COMPOSITE_KEY_DELIMITER: &'static str = "::";
    const COMPOSITE_KEY_ESCAPE_REPLACEMENT: &'static str = "::::";

    fn join_composite_key(parts: &[String]) -> String {
        let escaped_parts: Vec<String> = parts
            .iter()
            .map(|part| {
                part.replace(
                    Self::COMPOSITE_KEY_DELIMITER,
                    Self::COMPOSITE_KEY_ESCAPE_REPLACEMENT,
                )
            })
            .collect();
        escaped_parts.join(Self::COMPOSITE_KEY_DELIMITER)
    }

    /// Split a composite key back into its parts
    fn split_composite_key(composite_key: &str) -> Vec<String> {
        // Split on single delimiter (::) but avoid splitting on escaped delimiter (::::)
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut chars = composite_key.chars().peekable();
        
        while let Some(ch) = chars.next() {
            if ch == ':' {
                if chars.peek() == Some(&':') {
                    chars.next(); // consume second ':'
                    if chars.peek() == Some(&':') {
                        // This is the escaped delimiter "::::" - treat as literal "::"
                        chars.next(); // consume third ':'
                        chars.next(); // consume fourth ':'
                        current_part.push_str(Self::COMPOSITE_KEY_DELIMITER);
                    } else {
                        // This is the actual delimiter "::" - split here
                        parts.push(current_part.clone());
                        current_part.clear();
                    }
                } else {
                    // Single colon, just add it
                    current_part.push(ch);
                }
            } else {
                current_part.push(ch);
            }
        }
        
        // Add the final part
        if !current_part.is_empty() || !parts.is_empty() {
            parts.push(current_part);
        }
        
        parts
    }

    /// Escape SQL identifier (column name)
    fn escape_identifier(identifier: &str) -> String {
        // For Delta Lake, use backticks for identifier escaping
        format!("`{}`", identifier.replace('`', "``"))
    }

    /// Escape string literal for SQL
    fn escape_string_literal(value: &str) -> String {
        // Escape single quotes by doubling them
        format!("'{}'", value.replace('\'', "''"))
    }

    /// Get primary key column names from table schema
    pub fn get_primary_key_columns(table_schema: &TableSchema) -> Vec<String> {
        table_schema
            .column_schemas
            .iter()
            .filter(|col| col.primary)
            .map(|col| col.name.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::{ColumnSchema, TableName, Type, Cell, TableId, TableRow, TableSchema};

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            TableId(1),
            TableName::new("public".to_string(), "test_table".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ],
        )
    }

    fn create_test_row(id: i32, name: &str) -> TableRow {
        TableRow::new(vec![
            Cell::I32(id),
            Cell::String(name.to_string()),
        ])
    }

    #[test]
    fn test_extract_primary_key_single_column() {
        let client = DeltaLakeClient::new(None);
        let schema = create_test_schema();
        let row = create_test_row(42, "test");

        let result = client.extract_primary_key(&row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "42");
    }

    #[test]
    fn test_extract_primary_key_composite() {
        let client = DeltaLakeClient::new(None);
        let mut schema = create_test_schema();
        // Make both columns primary keys
        schema.column_schemas[1].primary = true;
        
        let row = create_test_row(42, "test");

        let result = client.extract_primary_key(&row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "42::test");
    }

    #[test]
    fn test_build_pk_predicate_single_column() {
        let client = DeltaLakeClient::new(None);
        let mut keys = HashSet::new();
        keys.insert("42".to_string());
        keys.insert("43".to_string());
        
        let pk_columns = vec!["id".to_string()];
        let predicate = client.build_pk_predicate(&keys, &pk_columns);
        
        // Should be `id` IN ('42', '43') - order may vary
        assert!(predicate.contains("`id` IN"));
        assert!(predicate.contains("'42'"));
        assert!(predicate.contains("'43'"));
    }

    #[test]
    fn test_build_pk_predicate_composite() {
        let client = DeltaLakeClient::new(None);
        let mut keys = HashSet::new();
        keys.insert("42::test".to_string());
        keys.insert("43::hello".to_string());
        
        let pk_columns = vec!["id".to_string(), "name".to_string()];
        let predicate = client.build_pk_predicate(&keys, &pk_columns);
        
        // Should be (`id` = '42' AND `name` = 'test') OR (`id` = '43' AND `name` = 'hello')
        assert!(predicate.contains("`id` = '42' AND `name` = 'test'"));
        assert!(predicate.contains("`id` = '43' AND `name` = 'hello'"));
        assert!(predicate.contains(" OR "));
    }

    #[test]
    fn test_build_pk_predicate_empty() {
        let client = DeltaLakeClient::new(None);
        let keys = HashSet::new();
        let pk_columns = vec!["id".to_string()];
        
        let predicate = client.build_pk_predicate(&keys, &pk_columns);
        assert_eq!(predicate, "false");
    }

    #[test]
    fn test_composite_key_escape() {
        let parts = vec!["value::with::delimiter".to_string(), "normal".to_string()];
        let composite = DeltaLakeClient::join_composite_key(&parts);
        assert_eq!(composite, "value::::with::::delimiter::normal");
        
        let split_parts = DeltaLakeClient::split_composite_key(&composite);
        assert_eq!(split_parts, parts);
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(DeltaLakeClient::escape_identifier("normal"), "`normal`");
        assert_eq!(DeltaLakeClient::escape_identifier("with`backtick"), "`with``backtick`");
    }

    #[test]
    fn test_escape_string_literal() {
        assert_eq!(DeltaLakeClient::escape_string_literal("normal"), "'normal'");
        assert_eq!(DeltaLakeClient::escape_string_literal("with'quote"), "'with''quote'");
    }
}
