use std::convert::TryFrom;
use std::sync::Arc;

use deltalake::DeltaTable;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, trace};

use etl::types::TableId;

use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::operations::{compact_table, zorder_table};

#[derive(Debug)]
pub struct TableMaintenanceInner {
    pub last_compacted_version: i64,
    pub last_zordered_version: i64,
    pub compaction_task: Option<JoinHandle<()>>,
    pub zorder_task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct TableMaintenanceState {
    pub(crate) inner: Mutex<TableMaintenanceInner>,
}

impl TableMaintenanceState {
    pub fn new(initial_version: i64) -> Self {
        Self {
            inner: Mutex::new(TableMaintenanceInner {
                last_compacted_version: initial_version,
                last_zordered_version: initial_version,
                compaction_task: None,
                zorder_task: None,
            }),
        }
    }

    /// Await any in-flight compaction, then if the `compact_after_commits` threshold is met,
    /// run compaction. This guarantees serialization of compaction runs relative to table writes.
    pub async fn maybe_run_compaction(
        self: &Arc<Self>,
        table_id: TableId,
        table: Arc<Mutex<DeltaTable>>,
        config: Arc<DeltaTableConfig>,
        table_version: i64,
    ) {
        if let Some(handle) = {
            let mut state = self.inner.lock().await;
            state.compaction_task.take()
        } {
            if let Err(err) = handle.await {
                error!(table_id = table_id.0, error = %err, "Compaction task join failed");
            }
        }

        let should_compact = {
            let state = self.inner.lock().await;
            match config.compact_after_commits {
                Some(compact_after) => {
                    if let Ok(threshold) = i64::try_from(compact_after.get()) {
                        table_version.saturating_sub(state.last_compacted_version) >= threshold
                    } else {
                        false
                    }
                }
                None => false,
            }
        };

        if !should_compact {
            return;
        }

        let task_state = Arc::clone(self);
        let task_table = Arc::clone(&table);
        let task_config = Arc::clone(&config);

        let handle = tokio::spawn(async move {
            trace!(
                table_id = table_id.0,
                "Starting Delta table compaction task"
            );
            let mut table_guard = task_table.lock().await;
            if let Err(err) = compact_table(&mut table_guard, task_config.as_ref()).await {
                error!(table_id = table_id.0, error = %err, "Delta table compaction task failed");
                return;
            }
            let version = table_guard.version().unwrap_or(table_version);
            trace!(
                table_id = table_id.0,
                version, "Finished Delta table compaction task"
            );
            drop(table_guard);

            let mut state = task_state.inner.lock().await;
            state.last_compacted_version = version;
        });

        let mut state = self.inner.lock().await;
        state.compaction_task = Some(handle);
    }

    /// Await any in-flight Z-ordering, then if the `z_order_after_commits` threshold is met,
    /// run Z-order. Serializes Z-order runs relative to table writes.
    pub async fn maybe_run_zorder(
        self: &Arc<Self>,
        table_id: TableId,
        table: Arc<Mutex<DeltaTable>>,
        config: Arc<DeltaTableConfig>,
        table_version: i64,
    ) {
        // Join any finished task to propagate panics and free resources.
        if let Some(handle) = {
            let mut state = self.inner.lock().await;
            state.zorder_task.take()
        } {
            if let Err(err) = handle.await {
                error!(table_id = table_id.0, error = %err, "Z-order task join failed");
            }
        }

        let (should_zorder, columns) = {
            let state = self.inner.lock().await;
            match (
                config.z_order_columns.as_ref(),
                config.z_order_after_commits,
            ) {
                (Some(columns), Some(zorder_after)) if !columns.is_empty() => {
                    if let Ok(threshold) = i64::try_from(zorder_after.get()) {
                        let should =
                            table_version.saturating_sub(state.last_zordered_version) >= threshold;
                        (should, columns.clone())
                    } else {
                        (false, Vec::new())
                    }
                }
                _ => (false, Vec::new()),
            }
        };

        if !should_zorder {
            return;
        }

        let task_state = Arc::clone(self);
        let task_table = Arc::clone(&table);
        let task_config = Arc::clone(&config);

        let handle = tokio::spawn(async move {
            trace!(table_id = table_id.0, columns = ?columns, "Starting Delta table Z-order task");
            let mut table_guard = task_table.lock().await;
            if let Err(err) = zorder_table(&mut table_guard, task_config.as_ref(), columns).await {
                error!(table_id = table_id.0, error = %err, "Delta table Z-order task failed");
                return;
            }
            let version = table_guard.version().unwrap_or(table_version);
            trace!(
                table_id = table_id.0,
                version, "Finished Delta table Z-order task"
            );
            drop(table_guard);

            let mut state = task_state.inner.lock().await;
            state.last_zordered_version = version;
        });

        let mut state = self.inner.lock().await;
        state.zorder_task = Some(handle);
    }
}
