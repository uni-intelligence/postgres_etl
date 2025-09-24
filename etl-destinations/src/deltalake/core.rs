use dashmap::DashMap;
use dashmap::Entry::{Occupied, Vacant};
use deltalake::datafusion::logical_expr::Expr;
use deltalake::table::builder::parse_table_uri;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, TableProperty};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableRow as PgTableRow, TableSchema as PgTableSchema};
use etl::{bail, etl_error};
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, trace};

use crate::arrow::rows_to_record_batch;
use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::events::{materialize_events, materialize_events_append_only};
use crate::deltalake::maintenance::TableMaintenanceState;
use crate::deltalake::operations::{append_to_table, delete_from_table, merge_to_table};
use crate::deltalake::schema::postgres_to_delta_schema;

/// Configuration for Delta Lake destination
#[derive(Debug, Clone)]
pub struct DeltaDestinationConfig {
    /// Base URI for Delta table storage (e.g., "s3://bucket/warehouse", "file:///tmp/delta")
    pub base_uri: String,
    /// Optional storage options passed to underlying object store
    pub storage_options: Option<HashMap<String, String>>,
    /// Table configuration (per table)
    pub table_config: HashMap<String, Arc<DeltaTableConfig>>,
}

/// Delta Lake destination implementation
#[derive(Clone)]
pub struct DeltaLakeDestination<S> {
    store: S,
    config: DeltaDestinationConfig,
    /// Cache of opened Delta tables, keyed by postgres table id
    // This isn't using a RWLock because we are overwhelmingly write-heavy
    table_cache: DashMap<TableId, Arc<Mutex<DeltaTable>>>,
    /// Tracks in-flight maintenance tasks and the versions they cover.
    maintenance: DashMap<TableId, Arc<TableMaintenanceState>>,
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
            maintenance: DashMap::new(),
        }
    }

    fn config_for_table_name(&self, table_name: &str) -> Arc<DeltaTableConfig> {
        self.config
            .table_config
            .get(table_name)
            .cloned()
            .unwrap_or_else(|| Arc::new(DeltaTableConfig::default()))
    }

    fn maintenance_state_for(
        &self,
        table_id: TableId,
        current_version: i64,
    ) -> Arc<TableMaintenanceState> {
        match self.maintenance.entry(table_id) {
            Occupied(entry) => Arc::clone(entry.get()),
            Vacant(entry) => {
                let state = Arc::new(TableMaintenanceState::new(current_version));
                entry.insert(state.clone());
                state
            }
        }
    }

    /// Returns a cached table handle or loads it if missing.
    async fn table_handle(&self, table_id: &TableId) -> EtlResult<Arc<Mutex<DeltaTable>>> {
        let handle = match self.table_cache.entry(*table_id) {
            Occupied(entry) => entry.into_ref(),
            Vacant(entry) => {
                let table = self.get_or_create_table(table_id).await?;
                entry.insert(Arc::new(Mutex::new(table)))
            }
        }
        .downgrade();

        Ok(Arc::clone(handle.value()))
    }

    /// Gets or creates a Delta table for a given table id if it doesn't exist.
    async fn get_or_create_table(&self, table_id: &TableId) -> EtlResult<DeltaTable> {
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

        let table_name = &table_schema.name.name;
        let table_path = parse_table_uri(format!("{}/{}", self.config.base_uri, table_name))
            .map_err(|e| {
                etl_error!(ErrorKind::DestinationError, "Failed to parse table path", e)
            })?;

        let mut table_builder = DeltaTableBuilder::from_uri(table_path).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to create Delta table builder",
                e
            )
        })?;
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

        let delta_schema = postgres_to_delta_schema(&table_schema).map_err(|e| {
            etl_error!(
                ErrorKind::ConversionError,
                "Failed to convert table schema to Delta schema",
                e
            )
        })?;

        let config = self.config_for_table_name(table_name);

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

    /// Process events grouped by table
    async fn process_events_by_table(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut events_by_table: HashMap<TableId, Vec<Event>> = HashMap::new();

        for event in events.into_iter() {
            match event {
                Event::Insert(ref e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Update(ref e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Delete(ref e) => {
                    events_by_table.entry(e.table_id).or_default().push(event);
                }
                Event::Truncate(ref e) => {
                    // Truncate events affect multiple tables (relation IDs)
                    for &rel_id in &e.rel_ids {
                        let table_id = TableId(rel_id);
                        events_by_table
                            .entry(table_id)
                            .or_default()
                            .push(event.clone());
                    }
                }
                Event::Relation(ref e) => {
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

        let tasks: Vec<_> = events_by_table
            .into_iter()
            .filter(|(_, events)| !events.is_empty())
            .map(|(table_id, events)| self.process_table_events(table_id, events))
            .collect();

        try_join_all(tasks).await?;

        Ok(())
    }

    /// Process events for a specific table, compacting them into a single consistent state
    async fn process_table_events(&self, table_id: TableId, events: Vec<Event>) -> EtlResult<()> {
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

        if is_append_only {
            let rows = materialize_events_append_only(&events, &table_schema)?;
            self.write_table_rows_internal(&table_id, rows).await?;
        } else {
            let (delete_predicates, rows) = materialize_events(&events, &table_schema)?;
            self.execute_delete_append_transaction_expr(
                table_id,
                &table_schema,
                rows,
                delete_predicates,
            )
            .await?;
        }

        Ok(())
    }

    /// Execute delete+append transaction for CDC using DataFusion expressions for keys
    async fn execute_delete_append_transaction_expr(
        &self,
        table_id: TableId,
        table_schema: &PgTableSchema,
        upsert_rows: Vec<&PgTableRow>,
        delete_predicates: Vec<Expr>,
    ) -> EtlResult<()> {
        let combined_predicate = delete_predicates.into_iter().reduce(|acc, e| acc.or(e));

        if upsert_rows.is_empty() && combined_predicate.is_none() {
            return Ok(());
        }

        let config = self.config_for_table_name(&table_schema.name.name);
        let table = self.table_handle(&table_id).await?;

        if upsert_rows.is_empty() {
            if let Some(combined_predicate) = combined_predicate {
                trace!("Deleting rows from table {}", table_id);

                let mut table_guard = table.lock().await;
                delete_from_table(&mut table_guard, config.as_ref(), combined_predicate)
                    .await
                    .map_err(|e| {
                        etl_error!(
                            ErrorKind::DestinationError,
                            "Failed to delete rows from Delta table",
                            format!("Error deleting from table for table_id {}: {}", table_id, e)
                        )
                    })?;

                let version = table_guard.version().unwrap_or_default();
                drop(table_guard);

                self.maybe_schedule_maintenance(table_id, table, version, config)
                    .await?;
            }
            return Ok(());
        }

        trace!(
            "Appending {} upserted rows to table {}",
            upsert_rows.len(),
            table_id,
        );

        let mut table_guard = table.lock().await;

        merge_to_table(
            &mut table_guard,
            config.as_ref(),
            table_schema,
            upsert_rows,
            combined_predicate,
        )
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

        let version = table_guard.version().ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to get version from Delta table",
                format!("Error getting version from table for table_id {}", table_id)
            )
        })?;
        drop(table_guard);

        self.maybe_schedule_maintenance(table_id, table, version, config)
            .await?;

        Ok(())
    }

    async fn write_table_rows_internal(
        &self,
        table_id: &TableId,
        table_rows: Vec<&PgTableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let table = self.table_handle(table_id).await?;

        let row_length = table_rows.len();
        trace!("Writing {} rows to Delta table", row_length);
            
        let config = self.config_for_table_name(&table_schema.name.name);
        let mut table_guard = table.lock().await;
        let schema = table_guard.snapshot().schema();

        let record_batch =
            rows_to_record_batch(table_rows.iter(), table_schema.clone()).map_err(|e| {
                etl_error!(
                    ErrorKind::ConversionError,
                    "Failed to encode table rows",
                    format!("Error converting to Arrow: {}", e)
                )
            })?;
        append_to_table(&mut table_guard, config.as_ref(), record_batch)
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to write to Delta table",
                    format!("Error writing to table for table_id {}: {}", table_id, e)
                )
            })?;

        info!(
            "Successfully wrote {} rows to Delta table for table_id: {}",
            row_length, table_id.0
        );

        let version = table_guard.version().unwrap_or_default();
        drop(table_guard);

        self.maybe_schedule_maintenance(*table_id, table, version, config)
            .await?;

        Ok(())
    }

    /// Schedules compaction or Z-ordering tasks if thresholds are met.
    async fn maybe_schedule_maintenance(
        &self,
        table_id: TableId,
        table: Arc<Mutex<DeltaTable>>,
        table_version: i64,
        config: Arc<DeltaTableConfig>,
    ) -> EtlResult<()> {
        if table_version < 0 {
            panic!("Table version is less than 0");
        }

        if config.compact_after_commits.is_none() && config.z_order_after_commits.is_none() {
            return Ok(());
        }

        let maintenance_state = self.maintenance_state_for(table_id, table_version);

        maintenance_state
            .maybe_run_compaction(
                table_id,
                Arc::clone(&table),
                Arc::clone(&config),
                table_version,
            )
            .await;
        maintenance_state
            .maybe_run_zorder(
                table_id,
                Arc::clone(&table),
                Arc::clone(&config),
                table_version,
            )
            .await;

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
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<PgTableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_internal(&table_id, table_rows.iter().collect())
            .await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("Processing {} events for Delta destination", events.len());

        self.process_events_by_table(events).await?;

        Ok(())
    }
}
