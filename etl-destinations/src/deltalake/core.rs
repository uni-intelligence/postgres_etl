use dashmap::DashMap;
use dashmap::Entry::{Occupied, Vacant};
use deltalake::datafusion::logical_expr::Expr;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, TableProperty};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableRow, TableSchema};
use etl::{bail, etl_error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use tracing::{info, trace};

use crate::deltalake::TableRowEncoder;
use crate::deltalake::operations::append_to_table;
use crate::deltalake::schema::postgres_to_delta_schema;
use crate::deltalake::table::DeltaTableConfig;

/// Configuration for Delta Lake destination
#[derive(Debug, Clone)]
pub struct DeltaDestinationConfig {
    /// Base URI for Delta table storage (e.g., "s3://bucket/warehouse", "file:///tmp/delta")
    pub base_uri: String,
    /// Optional storage options passed to underlying object store
    pub storage_options: Option<HashMap<String, String>>,
    /// Table configuration (per table)
    pub table_config: HashMap<String, DeltaTableConfig>,
}

/// Delta Lake destination implementation
#[derive(Clone)]
pub struct DeltaLakeDestination<S> {
    store: S,
    config: DeltaDestinationConfig,
    /// Cache of opened Delta tables, keyed by postgres table id
    table_cache: DashMap<TableId, Arc<Mutex<DeltaTable>>>,
}

impl<S> DeltaLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Create a new Delta Lake destination
    pub fn new(store: S, config: DeltaDestinationConfig) -> Self {
        Self {
            store,
            config,
            table_cache: DashMap::new(),
        }
    }

    fn config_for_table_name(&self, table_name: &str) -> DeltaTableConfig {
        self.config
            .table_config
            .get(table_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Gets or creates  a Delta table at `table_uri` if it doesn't exist
    /// This does NOT write or check the cache due to lifetime issues.
    async fn get_or_create_table(&self, table_id: &TableId) -> EtlResult<DeltaTable> {
        let table_name = self.get_table_name(table_id).await?;
        let table_path = format!("{}/{}", self.config.base_uri, table_name);

        let mut table_builder = DeltaTableBuilder::from_uri(table_path);
        if let Some(storage_options) = &self.config.storage_options {
            table_builder = table_builder.with_storage_options(storage_options.clone());
        }
        let mut table = table_builder.build().map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to build Delta table",
                e
            )
        })?;

        let ops: DeltaOps = match table.load().await {
            Ok(_) => return Ok(table),
            Err(DeltaTableError::NotATable(_)) => table.into(),
            Err(e) => {
                bail!(ErrorKind::DestinationError, "Failed to load Delta table", e);
            }
        };

        let table_schema = self
            .store
            .get_table_schema(table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Schema for table {} not found in store", table_id.0)
                )
            })?;

        let delta_schema = postgres_to_delta_schema(&table_schema).map_err(|e| {
            etl_error!(
                ErrorKind::ConversionError,
                "Failed to convert table schema to Delta schema",
                e
            )
        })?;

        let config = self.config_for_table_name(&table_name);

        let mut builder = ops
            .create()
            // TODO(abhi): Figure out how to avoid the clone
            .with_columns(delta_schema.fields().cloned());

        if config.append_only {
            builder = builder
                .with_configuration_property(TableProperty::AppendOnly, Some("true".to_string()));
        }

        let table = builder.await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to create Delta table",
                e
            )
        })?;

        Ok(table)
    }

    /// Get the table path for a given TableId
    async fn get_table_name(&self, table_id: &TableId) -> EtlResult<String> {
        self.store
            .get_table_mapping(table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Schema for table {} not found in store", table_id.0)
                )
            })
    }

    /// Process events grouped by table
    async fn process_events_by_table(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut events_by_table: HashMap<TableId, Vec<Event>> = HashMap::new();

        // Group events by table_id
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
                    // Truncate events affect multiple tables (relation IDs)
                    for &rel_id in &e.rel_ids {
                        let table_id = TableId(rel_id);
                        events_by_table
                            .entry(table_id)
                            .or_default()
                            .push(event.clone());
                    }
                }
                Event::Relation(e) => {
                    // Schema change events - store the table schema
                    let table_id = e.table_schema.id;
                    events_by_table.entry(table_id).or_default().push(event);
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {
                    // Skip transaction control events - they don't affect specific tables
                }
            }
        }

        info!("Processing events for {} tables", events_by_table.len());

        // Process each table sequentially to avoid lifetime issues in tests
        for (table_id, events) in events_by_table.into_iter() {
            self.process_table_events(table_id, events).await?;
        }

        Ok(())
    }

    /// Process events for a specific table, compacting them into a single consistent state
    async fn process_table_events(&self, table_id: TableId, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    table_id
                )
            })?;

        let is_append_only = self
            .config_for_table_name(&table_schema.name.name)
            .append_only;

        let (delete_predicates, upsert_rows) =
            crate::deltalake::events::resolve_events_by_table_id(
                &events,
                table_id,
                &table_schema,
                is_append_only,
            )?;

        self.execute_delete_append_transaction_expr(
            table_id,
            &table_schema,
            delete_predicates,
            upsert_rows,
        )
        .await
    }

    /// Execute delete+append transaction for CDC using DataFusion expressions for keys
    async fn execute_delete_append_transaction_expr(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        delete_predicates: Vec<Expr>,
        upsert_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table = match self.table_cache.entry(table_id) {
            Occupied(entry) => entry.into_ref(),
            Vacant(entry) => {
                let table = self.get_or_create_table(&table_id).await?;
                entry.insert(Arc::new(Mutex::new(table)))
            }
        };

        if !delete_predicates.is_empty() {
            let combined_predicate = delete_predicates
                .into_iter()
                .reduce(|acc, e| acc.or(e))
                .expect("non-empty predicates");

            trace!(
                "Deleting rows from table {} with predicate (Expr)",
                table_id.0
            );

            let table = table.lock().await;
            let ops: DeltaOps = table.clone().into();
            ops.delete()
                .with_predicate(combined_predicate)
                .await
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to delete rows from Delta table",
                        format!(
                            "Error deleting from table for table_id {}: {}",
                            table_id.0, e
                        )
                    )
                })?;
        }

        if !upsert_rows.is_empty() {
            trace!(
                "Appending {} upserted rows to table {}",
                upsert_rows.len(),
                table_id.0
            );

            let record_batch =
                TableRowEncoder::encode_table_rows(table_schema, upsert_rows.iter().collect())
                    .map_err(|e| {
                        etl_error!(
                            ErrorKind::ConversionError,
                            "Failed to encode table rows for append",
                            format!("Error converting to Arrow: {}", e)
                        )
                    })?;

            let config = self.config_for_table_name(&table_schema.name.name);
            let mut table = table.lock().await;
            append_to_table(&mut table, &config, record_batch)
                .await
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to append rows to Delta table",
                        format!(
                            "Error appending to table for table_id {}: {}",
                            table_id.0, e
                        )
                    )
                })?;
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
    fn name() -> &'static str {
        "deltalake"
    }

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        todo!()
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let table = match self.table_cache.entry(table_id) {
            Occupied(entry) => entry.into_ref(),
            Vacant(entry) => {
                let table = self.get_or_create_table(&table_id).await?;
                entry.insert(Arc::new(Mutex::new(table)))
            }
        }
        .downgrade();

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

        {}

        let record_batch =
            TableRowEncoder::encode_table_rows(&table_schema, table_rows.iter().collect())
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::ConversionError,
                        "Failed to encode table rows",
                        format!("Error converting to Arrow: {}", e)
                    )
                })?;

        trace!("Writing {} rows to Delta table", table_rows.len(),);

        let config = self.config_for_table_name(&table_schema.name.name);
        let mut table = table.lock().await;
        append_to_table(&mut table, &config, record_batch)
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to write to Delta table",
                    format!("Error writing to table for table_id {}: {}", table_id.0, e)
                )
            })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use etl::test_utils::notify::NotifyingStore;
    use etl::types::{
        Cell, ColumnSchema, Event, InsertEvent, PgLsn, TableId, TableName, TableRow, TableSchema,
        Type,
    };

    #[allow(unused)]
    fn create_test_table_schema(table_id: TableId) -> TableSchema {
        TableSchema::new(
            table_id,
            TableName::new("public".to_string(), "test_table".to_string()),
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    typ: Type::INT8,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    primary: false,
                    nullable: false,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    typ: Type::INT4,
                    modifier: -1,
                    primary: false,
                    nullable: true,
                },
            ],
        )
    }

    fn create_test_row(id: i64, name: &str, age: Option<i32>) -> TableRow {
        TableRow {
            values: vec![
                Cell::I64(id),
                Cell::String(name.to_string()),
                age.map_or(Cell::Null, Cell::I32),
            ],
        }
    }

    async fn create_test_destination() -> (DeltaLakeDestination<NotifyingStore>, TableId) {
        let table_id = TableId(123);
        let store = NotifyingStore::new();
        let config = DeltaDestinationConfig {
            base_uri: "memory://test".to_string(),
            storage_options: None,
            table_config: HashMap::new(),
        };
        let destination = DeltaLakeDestination::new(store, config);
        (destination, table_id)
    }

    #[tokio::test]
    async fn test_process_table_events_empty_list() {
        let (destination, table_id) = create_test_destination().await;
        let result = destination.process_table_events(table_id, vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_table_events_single_insert_structure() {
        let (destination, table_id) = create_test_destination().await;
        let insert_event = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(1),
            table_id,
            table_row: create_test_row(1, "Alice", Some(25)),
        });
        let events = vec![insert_event];
        let result = destination.process_table_events(table_id, events).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_grouping_by_table_basic() {
        let (_, table_id1) = create_test_destination().await;
        let table_id2 = TableId(456);
        let store = NotifyingStore::new();
        let config = DeltaDestinationConfig {
            base_uri: "memory://test".to_string(),
            storage_options: None,
            table_config: HashMap::new(),
        };
        let destination = DeltaLakeDestination::new(store, config);

        let insert_event1 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(1),
            table_id: table_id1,
            table_row: create_test_row(1, "Alice", Some(25)),
        });
        let insert_event2 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1),
            commit_lsn: PgLsn::from(2),
            table_id: table_id2,
            table_row: create_test_row(1, "Bob", Some(30)),
        });
        let insert_event3 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(2),
            commit_lsn: PgLsn::from(3),
            table_id: table_id1,
            table_row: create_test_row(2, "Charlie", Some(35)),
        });

        let events = vec![insert_event1, insert_event2, insert_event3];
        let result = destination.process_events_by_table(events).await;
        assert!(result.is_err());
    }
}
