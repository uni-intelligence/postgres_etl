use deltalake::{
    DeltaResult, DeltaTable, datafusion::prelude::Expr, operations::delete::DeleteBuilder,
};

use crate::deltalake::config::DeltaTableConfig;

pub async fn delete_from_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    delete_predicate: Expr,
) -> DeltaResult<()> {
    let delete_builder = DeleteBuilder::new((*table).log_store(), table.snapshot()?.clone())
        .with_predicate(delete_predicate)
        .with_writer_properties(config.into());
    // TODO(abhi): Do something with the metrics
    let (deleted_table, _metrics) = delete_builder.await?;
    *table = deleted_table;
    Ok(())
}
