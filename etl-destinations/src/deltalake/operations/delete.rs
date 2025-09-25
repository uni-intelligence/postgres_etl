use deltalake::{
    DeltaResult, DeltaTable, datafusion::prelude::Expr, operations::delete::DeleteBuilder,
};

use crate::deltalake::config::DeltaTableConfig;
use tracing::{instrument, trace};

#[instrument(skip(table, config, delete_predicate))]
pub async fn delete_from_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    delete_predicate: Expr,
) -> DeltaResult<()> {
    trace!("Building delete builder with predicate");
    let delete_builder = DeleteBuilder::new((*table).log_store(), table.snapshot()?.clone())
        .with_predicate(delete_predicate)
        .with_writer_properties(config.into());
    // TODO(abhi): Do something with the metrics
    trace!("Executing delete operation");
    let (deleted_table, _metrics) = delete_builder.await?;
    *table = deleted_table;
    trace!("Delete operation completed");
    Ok(())
}
