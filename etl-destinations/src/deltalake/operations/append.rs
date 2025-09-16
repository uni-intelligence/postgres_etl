use deltalake::{
    DeltaResult, DeltaTable,
    arrow::array::RecordBatch,
    writer::{DeltaWriter, RecordBatchWriter},
};

use crate::deltalake::config::DeltaTableConfig;

/// Appends a record batch to a Delta table
pub async fn append_to_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    record_batch: RecordBatch,
) -> DeltaResult<()> {
    let mut writer = RecordBatchWriter::for_table(table)?;
    writer = writer.with_writer_properties(config.into());
    writer.write(record_batch).await?;
    writer.flush_and_commit(table).await?;
    Ok(())
}
