use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::schema::postgres_to_delta_schema;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::{DeltaOps, DeltaResult, DeltaTable, DeltaTableBuilder, open_table};
use etl::types::TableSchema;

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
        let table = open_table(table_uri).await?;
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
        app_transaction_id: Option<&str>,
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
        // todo(abhi): Implement proper predicate building for primary key matching
        // todo(abhi): Handle composite primary keys
        // todo(abhi): Handle SQL injection prevention
        // todo(abhi): Build disjunction for multiple keys

        if primary_keys.is_empty() {
            return "false".to_string(); // No rows to match
        }

        // Simple single-column PK case for now
        if pk_column_names.len() == 1 {
            let pk_column = &pk_column_names[0];
            let keys: Vec<String> = primary_keys.iter().map(|k| format!("'{k}'")).collect();
            return format!("{} IN ({})", pk_column, keys.join(", "));
        }

        // todo(abhi): Handle composite primary keys
        // For composite keys, need to build something like:
        // (col1 = 'val1' AND col2 = 'val2') OR (col1 = 'val3' AND col2 = 'val4') ...

        "false".to_string() // Fallback
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

        let _current_schema = table.schema();
        let _expected_delta_schema = postgres_to_delta_schema(expected_schema)?;

        // todo(abhi): Compare schemas and find missing columns
        // todo(abhi): Ensure no incompatible changes (type changes, etc.)

        Ok(vec![]) // No missing columns for now
    }
}
