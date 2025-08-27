use deltalake::DeltaTable;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableRow};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, trace};

use crate::delta::{DeltaLakeClient, TableRowEncoder};

/// Configuration for Delta Lake destination
#[derive(Debug, Clone)]
pub struct DeltaDestinationConfig {
    /// Base URI for Delta table storage (e.g., "s3://bucket/warehouse", "file:///tmp/delta")
    pub base_uri: String,
    /// Optional storage options passed to underlying object store
    pub storage_options: Option<HashMap<String, String>>,
    /// Columns to use for partitioning (per table)
    pub partition_columns: Option<HashMap<String, Vec<String>>>,
    /// Run OPTIMIZE every N commits (None = disabled)
    pub optimize_after_commits: Option<NonZeroU64>,
}

impl Default for DeltaDestinationConfig {
    fn default() -> Self {
        Self {
            base_uri: "file:///tmp/delta".to_string(),
            storage_options: None,
            partition_columns: None,
            optimize_after_commits: None,
        }
    }
}

/// Delta Lake destination implementation
#[derive(Clone)]
pub struct DeltaLakeDestination<S> {
    client: DeltaLakeClient,
    store: S,
    config: DeltaDestinationConfig,
    /// Cache of opened Delta tables by table path
    table_cache: Arc<RwLock<HashMap<String, Arc<DeltaTable>>>>,
    /// Commit counters for optimization tracking
    commit_counters: Arc<RwLock<HashMap<String, u64>>>,
}

impl<S> DeltaLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Create a new Delta Lake destination
    pub fn new(store: S, config: DeltaDestinationConfig) -> Self {
        Self {
            client: DeltaLakeClient::new(config.storage_options.clone()),
            store,
            config,
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            commit_counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create table path for a given TableId
    async fn get_table_path(&self, table_id: TableId) -> EtlResult<String> {
        // todo(abhi): Implement table path resolution using table mappings
        // todo(abhi): Store mapping in StateStore for persistence across restarts
        // todo(abhi): Use schema name and table name from TableSchema

        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Schema for table {} not found in store", table_id.0)
                )
            })?;

        let table_path = format!("{}/{}", self.config.base_uri, table_schema.name.name);

        Ok(table_path)
    }

    /// Ensure table exists and get reference to it
    async fn ensure_table_exists(&self, table_id: TableId) -> EtlResult<Arc<DeltaTable>> {
        // todo(abhi): Implement table existence check and creation
        // todo(abhi): Handle schema evolution (add missing columns)
        // todo(abhi): Cache table references for performance

        let table_path = self.get_table_path(table_id).await?;

        // Check cache first
        {
            let cache = self.table_cache.read().await;
            if let Some(table) = cache.get(&table_path) {
                return Ok(table.clone());
            }
        }

        // Get table schema from store
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Schema for table {} not found in store", table_id.0)
                )
            })?;

        let table = self
            .client
            .create_table_if_missing(&table_path, &table_schema)
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to create Delta table",
                    format!("Error creating table at {}: {}", table_path, e)
                )
            })?;

        {
            let mut cache = self.table_cache.write().await;
            cache.insert(table_path.clone(), table.clone());
        }

        Ok(table)
    }

    /// Process events grouped by table
    async fn process_events_by_table(&self, events: Vec<Event>) -> EtlResult<()> {
        // todo(abhi): Implement CDC processing as described in PLAN.md
        // todo(abhi): Group events by table_id
        // todo(abhi): For each table: deduplicate by PK with last-wins using LSN
        // todo(abhi): Execute delete+append transaction per table

        let mut events_by_table: HashMap<TableId, Vec<Event>> = HashMap::new();

        // Group events by table
        for event in events {
            match &event {
                Event::Insert(e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Update(e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Delete(e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Truncate(e) => {
                    // todo(abhi): Handle truncate events that affect multiple tables
                    for &rel_id in &e.rel_ids {
                        let table_id = TableId(rel_id);
                        events_by_table
                            .entry(table_id)
                            .or_default()
                            .push(event.clone());
                    }
                }
                Event::Relation(_) => {
                    // todo(abhi): Handle schema changes (add columns)
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {
                    // Skip transaction control events
                }
            }
        }

        // Process each table's events
        for (table_id, table_events) in events_by_table {
            self.process_table_events(table_id, table_events).await?;
        }

        Ok(())
    }

    /// Process events for a specific table
    async fn process_table_events(&self, table_id: TableId, events: Vec<Event>) -> EtlResult<()> {
        // todo(abhi): Implement the last-wins deduplication logic from PLAN.md
        // todo(abhi): Build upserts_by_pk and delete_pks sets
        // todo(abhi): Execute delete+append transaction

        let _table = self.ensure_table_exists(table_id).await?;

        // Deduplicate by PK with last-wins using (commit_lsn, start_lsn)
        let mut upserts_by_pk: HashMap<String, TableRow> = HashMap::new(); // todo(abhi): Use proper PK type
        let mut delete_pks: HashSet<String> = HashSet::new(); // todo(abhi): Use proper PK type

        for event in events.iter() {
            match event {
                Event::Insert(e) => {
                    // todo(abhi): Extract PK from table_row
                    let pk = self.extract_primary_key(&e.table_row, table_id).await?;
                    upserts_by_pk.insert(pk, e.table_row.clone());
                }
                Event::Update(e) => {
                    // todo(abhi): Extract PK from table_row
                    let pk = self.extract_primary_key(&e.table_row, table_id).await?;
                    upserts_by_pk.insert(pk, e.table_row.clone());
                }
                Event::Delete(e) => {
                    // todo(abhi): Extract PK from old_table_row
                    if let Some((_, ref old_row)) = e.old_table_row {
                        let pk = self.extract_primary_key(old_row, table_id).await?;
                        upserts_by_pk.remove(&pk);
                        delete_pks.insert(pk);
                    }
                }
                Event::Truncate(_) => {
                    // todo(abhi): Handle truncate - clear all data
                    return self.truncate_table(table_id).await;
                }
                _ => {} // Skip other events
            }
        }

        // Execute delete+append transaction
        self.execute_delete_append_transaction(table_id, &upserts_by_pk, &delete_pks)
            .await?;

        Ok(())
    }

    /// Extract primary key from a table row
    async fn extract_primary_key(
        &self,
        _table_row: &TableRow,
        _table_id: TableId,
    ) -> EtlResult<String> {
        // todo(abhi): Implement primary key extraction
        // todo(abhi): Get PK columns from table schema
        // todo(abhi): Build composite key string for lookup

        // Stub implementation
        Ok("placeholder_pk".to_string())
    }

    /// Execute delete+append transaction for CDC
    async fn execute_delete_append_transaction(
        &self,
        table_id: TableId,
        upserts_by_pk: &HashMap<String, TableRow>,
        _delete_pks: &HashSet<String>,
    ) -> EtlResult<()> {
        // todo(abhi): Implement the transaction logic from PLAN.md
        // todo(abhi): Delete rows with PK in affected set
        // todo(abhi): Append upserted rows
        // todo(abhi): Use Delta transaction with app-level ID for idempotency

        let table_path = self.get_table_path(table_id).await?;

        // For now, just implement append for upserts (delete logic comes later)
        if !upserts_by_pk.is_empty() {
            let table_rows: Vec<TableRow> = upserts_by_pk.values().cloned().collect();
            self.write_table_rows(table_id, table_rows.clone()).await?;
        }

        // Update commit counter for optimization tracking
        if let Some(optimize_interval) = self.config.optimize_after_commits {
            let mut counters = self.commit_counters.write().await;
            let counter = counters.entry(table_path.clone()).or_insert(0);
            *counter += 1;

            if *counter >= optimize_interval.get() {
                // todo(abhi): Run OPTIMIZE operation
                *counter = 0;
            }
        }

        Ok(())
    }

    /// Run table optimization (OPTIMIZE)
    #[allow(unused)]
    async fn optimize_table(&self, _table_path: &str) -> EtlResult<()> {
        // todo(abhi): Implement OPTIMIZE operation using delta-rs
        // todo(abhi): Small file compaction and Z-ordering

        Ok(())
    }
}

impl<S> Destination for DeltaLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        // todo(abhi): Implement atomic truncate using Delta operations
        // todo(abhi): Prefer atomic empty snapshot or recreate table version

        let _table = self.ensure_table_exists(table_id).await?;

        // Stub implementation - this should be atomic in the real version
        // todo(abhi): Use delta-rs delete operation with predicate `true`
        // todo(abhi): Or recreate table with empty data

        info!("Truncating Delta table for table_id: {}", table_id.0);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let table = self.ensure_table_exists(table_id).await?;

        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Schema for table {} not found in store", table_id.0)
                )
            })?;

        let record_batches = TableRowEncoder::encode_table_rows(&table_schema, table_rows.clone())
            .map_err(|e| {
                etl_error!(
                    ErrorKind::ConversionError,
                    "Failed to encode table rows",
                    format!("Error converting to Arrow: {}", e)
                )
            })?;

        trace!(
            "Writing {} rows ({} batches) to Delta table",
            table_rows.len(),
            record_batches.len()
        );

        let updated_table = self
            .client
            .append_to_table(table, record_batches)
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to write to Delta table",
                    format!("Error writing to table for table_id {}: {}", table_id.0, e)
                )
            })?;

        // Update the cached table with the new version
        let table_path = self.get_table_path(table_id).await?;
        {
            let mut cache = self.table_cache.write().await;
            cache.insert(table_path, updated_table);
        }

        info!(
            "Successfully wrote {} rows to Delta table for table_id: {}",
            table_rows.len(),
            table_id.0
        );

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        // todo(abhi): Implement CDC event processing as described in PLAN.md
        // todo(abhi): Group by table, deduplicate by PK, execute delete+append

        if events.is_empty() {
            return Ok(());
        }

        info!("Processing {} events for Delta destination", events.len());

        self.process_events_by_table(events).await?;

        Ok(())
    }
}
