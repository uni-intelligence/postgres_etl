use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{DeltaResult, DeltaTable};
use tracing::{instrument, trace};

use crate::deltalake::config::DeltaTableConfig;

/// Optimizes a Delta table by compacting small files into larger ones.
#[instrument(skip(table, config))]
pub async fn compact_table(table: &mut DeltaTable, config: &DeltaTableConfig) -> DeltaResult<()> {
    trace!("Starting table compaction");
    let writer_properties = WriterProperties::from(config);
    let optimize_builder = OptimizeBuilder::new(table.log_store(), table.snapshot()?.clone());
    let (optimized_table, _metrics) = optimize_builder
        .with_writer_properties(writer_properties)
        .with_type(OptimizeType::Compact)
        .await?;
    *table = optimized_table;
    trace!("Finished table compaction");
    Ok(())
}

/// Optimizes a Delta table by performing Z-order clustering on the provided columns.
#[instrument(skip(table, config, columns), fields(columns = ?columns))]
pub async fn zorder_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    columns: Vec<String>,
) -> DeltaResult<()> {
    trace!("Starting table Z-order optimization");
    let writer_properties = WriterProperties::from(config);
    let optimize_builder = OptimizeBuilder::new(table.log_store(), table.snapshot()?.clone());
    let (optimized_table, _metrics) = optimize_builder
        .with_writer_properties(writer_properties)
        .with_type(OptimizeType::ZOrder(columns))
        .await?;
    *table = optimized_table;
    trace!("Finished table Z-order optimization");
    Ok(())
}
