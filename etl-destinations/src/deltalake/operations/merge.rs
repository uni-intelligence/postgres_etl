use deltalake::DeltaTableError;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::operations::merge::MergeBuilder;
use deltalake::{DeltaResult, DeltaTable, datafusion::prelude::Expr};
use etl::types::{TableRow as PgTableRow, TableSchema as PgTableSchema};

use crate::deltalake::TableRowEncoder;
use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::expr::qualify_primary_keys;

pub async fn merge_to_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    table_schema: &PgTableSchema,
    upsert_rows: Vec<&PgTableRow>,
    delete_predicate: Option<Expr>,
) -> DeltaResult<()> {
    let rows = TableRowEncoder::encode_table_rows(table_schema, upsert_rows)?;

    let ctx = SessionContext::new();
    let batch = ctx.read_batch(rows)?;

    // TODO(abhi): We should proabbly be passing this information in
    let primary_keys = table_schema
        .column_schemas
        .iter()
        .filter(|col| col.primary)
        .map(|col| Expr::Column(Column::new_unqualified(col.name.clone())))
        .collect();

    let qualified_primary_keys = qualify_primary_keys(primary_keys, "source", "target")
        .ok_or(DeltaTableError::generic("Failed to qualify primary keys"))?;

    let merge_builder = MergeBuilder::new(
        // TODO(abhi): Is there a way to do this while avoiding the clone/general hackiness?
        (*table).log_store(),
        table.snapshot()?.clone(),
        qualified_primary_keys,
        batch,
    );

    let mut merge_builder = merge_builder
        .with_writer_properties(config.clone().into())
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| insert)?
        .when_matched_update(|update| update)?;

    if let Some(delete_predicate) = delete_predicate {
        merge_builder = merge_builder
            .when_not_matched_by_source_delete(|delete| delete.predicate(delete_predicate))?;
    }
    // TODO(abhi): Do something with the metrics
    let (merged_table, _metrics) = merge_builder.await?;
    *table = merged_table;
    Ok(())
}
