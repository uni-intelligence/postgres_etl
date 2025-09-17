use dashmap::DashMap;
use dashmap::Entry::{Occupied, Vacant};
use deltalake::datafusion::logical_expr::Expr;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, TableProperty};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableRow as PgTableRow, TableSchema as PgTableSchema};
use etl::{bail, etl_error};
use futures::future::try_join_all;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, trace};

use crate::deltalake::TableRowEncoder;
use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::events::{materialize_events, materialize_events_append_only};
use crate::deltalake::operations::{
    append_to_table, compact_table, delete_from_table, merge_to_table, zorder_table,
};
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

/// Tracks background maintenance progress for a Delta table.
#[derive(Debug)]
struct TableMaintenanceState {
    inner: Mutex<TableMaintenanceInner>,
}

/// Stores the latest versions processed by maintenance tasks.
#[derive(Debug)]
struct TableMaintenanceInner {
    last_compacted_version: i64,
    last_zordered_version: i64,
    compaction_task: Option<JoinHandle<()>>,
    zorder_task: Option<JoinHandle<()>>,
}

impl TableMaintenanceState {
    /// Creates a new maintenance state seeded with the provided table version.
    fn new(initial_version: i64) -> Self {
        Self {
            inner: Mutex::new(TableMaintenanceInner {
                last_compacted_version: initial_version,
                last_zordered_version: initial_version,
                compaction_task: None,
                zorder_task: None,
            }),
        }
    }
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

        let version = table_guard.version().unwrap_or_default();
        drop(table_guard);

        self.maybe_schedule_maintenance(table_id, table, version, config)
            .await?;

        Ok(())
    }

    /// Run table optimization (OPTIMIZE)
    #[allow(unused)]
    async fn optimize_table(&self, _table_path: &str) -> EtlResult<()> {
        // todo(abhi): Implement OPTIMIZE operation using delta-rs
        // todo(abhi): Small file compaction and Z-ordering

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

        let row_length = table_rows.len();
        trace!("Writing {} rows to Delta table", row_length);

        let record_batch =
            TableRowEncoder::encode_table_rows(&table_schema, table_rows).map_err(|e| {
                etl_error!(
                    ErrorKind::ConversionError,
                    "Failed to encode table rows",
                    format!("Error converting to Arrow: {}", e)
                )
            })?;

        let config = self.config_for_table_name(&table_schema.name.name);
        let mut table_guard = table.lock().await;
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
            return Ok(());
        }

        if config.compact_after_commits.is_none() && config.z_order_after_commits.is_none() {
            return Ok(());
        }

        let maintenance_state = self.maintenance_state_for(table_id, table_version);

        let mut schedule_compact = false;
        let mut schedule_zorder: Option<Vec<String>> = None;

        {
            let mut state = maintenance_state.inner.lock().await;

            if let Some(handle) = state.compaction_task.as_ref() {
                if handle.is_finished() {
                    state.compaction_task.take();
                }
            }

            if let Some(handle) = state.zorder_task.as_ref() {
                if handle.is_finished() {
                    state.zorder_task.take();
                }
            }

            if let Some(compact_after) = config.compact_after_commits {
                if let Ok(threshold) = i64::try_from(compact_after.get()) {
                    if table_version.saturating_sub(state.last_compacted_version) >= threshold
                        && state.compaction_task.is_none()
                    {
                        schedule_compact = true;
                    }
                }
            }

            if let (Some(columns), Some(zorder_after)) = (
                config.z_order_columns.as_ref(),
                config.z_order_after_commits,
            ) {
                if !columns.is_empty() {
                    if let Ok(threshold) = i64::try_from(zorder_after.get()) {
                        if table_version.saturating_sub(state.last_zordered_version) >= threshold
                            && state.zorder_task.is_none()
                        {
                            schedule_zorder = Some(columns.clone());
                        }
                    }
                }
            }
        }

        if schedule_compact {
            let task_state = Arc::clone(&maintenance_state);
            let task_table = Arc::clone(&table);
            let task_config = Arc::clone(&config);
            let handle = tokio::spawn(async move {
                Self::run_compaction_task(
                    table_id,
                    task_table,
                    task_config,
                    Arc::clone(&task_state),
                    table_version,
                )
                .await;
            });

            let mut state = maintenance_state.inner.lock().await;
            state.compaction_task = Some(handle);
        }

        if let Some(columns) = schedule_zorder {
            let task_state = Arc::clone(&maintenance_state);
            let task_table = Arc::clone(&table);
            let task_config = Arc::clone(&config);
            let handle = tokio::spawn(async move {
                Self::run_zorder_task(
                    table_id,
                    task_table,
                    task_config,
                    Arc::clone(&task_state),
                    table_version,
                    columns,
                )
                .await;
            });

            let mut state = maintenance_state.inner.lock().await;
            state.zorder_task = Some(handle);
        }

        Ok(())
    }

    /// Executes a compaction task and updates maintenance tracking once finished.
    async fn run_compaction_task(
        table_id: TableId,
        table: Arc<Mutex<DeltaTable>>,
        config: Arc<DeltaTableConfig>,
        maintenance: Arc<TableMaintenanceState>,
        baseline_version: i64,
    ) {
        let result = async {
            trace!(
                table_id = table_id.0,
                "Starting Delta table compaction task"
            );
            let mut table_guard = table.lock().await;
            compact_table(&mut table_guard, config.as_ref()).await?;
            let version = table_guard.version().unwrap_or(baseline_version);
            trace!(
                table_id = table_id.0,
                version, "Finished Delta table compaction task"
            );
            Ok::<i64, DeltaTableError>(version)
        }
        .await;

        let mut state = maintenance.inner.lock().await;
        match result {
            Ok(version) => {
                state.last_compacted_version = version;
                state.compaction_task = None;
            }
            Err(err) => {
                state.compaction_task = None;
                error!(
                    table_id = table_id.0,
                    error = %err,
                    "Delta table compaction task failed"
                );
            }
        }
    }

    /// Executes a Z-order task and updates maintenance tracking once finished.
    async fn run_zorder_task(
        table_id: TableId,
        table: Arc<Mutex<DeltaTable>>,
        config: Arc<DeltaTableConfig>,
        maintenance: Arc<TableMaintenanceState>,
        baseline_version: i64,
        columns: Vec<String>,
    ) {
        let result = async {
            trace!(
                table_id = table_id.0,
                columns = ?columns,
                "Starting Delta table Z-order task"
            );
            let mut table_guard = table.lock().await;
            zorder_table(&mut table_guard, config.as_ref(), columns).await?;
            let version = table_guard.version().unwrap_or(baseline_version);
            trace!(
                table_id = table_id.0,
                version, "Finished Delta table Z-order task"
            );
            Ok::<i64, DeltaTableError>(version)
        }
        .await;

        let mut state = maintenance.inner.lock().await;
        match result {
            Ok(version) => {
                state.last_zordered_version = version;
                state.zorder_task = None;
            }
            Err(err) => {
                state.zorder_task = None;
                error!(
                    table_id = table_id.0,
                    error = %err,
                    "Delta table Z-order task failed"
                );
            }
        }
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
