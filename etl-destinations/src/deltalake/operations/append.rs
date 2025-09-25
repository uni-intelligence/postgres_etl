use deltalake::{
    DeltaResult, DeltaTable,
    arrow::array::RecordBatch,
    writer::{DeltaWriter, RecordBatchWriter},
};
use tracing::{instrument, trace};

use crate::deltalake::config::DeltaTableConfig;

/// Appends a record batch to a Delta table
#[instrument(skip(table, config, record_batch), fields(num_rows = record_batch.num_rows()))]
pub async fn append_to_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    record_batch: RecordBatch,
) -> DeltaResult<()> {
    trace!("Creating RecordBatchWriter for append");
    let mut writer = RecordBatchWriter::for_table(table)?;
    writer = writer.with_writer_properties(config.into());
    trace!("Writing record batch to Delta table");
    writer.write(record_batch).await?;
    trace!("Flushing and committing append");
    writer.flush_and_commit(table).await?;
    Ok(())
}
