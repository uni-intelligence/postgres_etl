use deltalake::DeltaOps;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::{DeltaResult, DeltaTable, datafusion::prelude::Expr};
use etl::types::{TableRow as PgTableRow, TableSchema as PgTableSchema};

use crate::deltalake::TableRowEncoder;
use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::expr::qualify_primary_keys;

pub async fn merge_to_table(
    table: DeltaTable,
    config: &DeltaTableConfig,
    table_schema: &PgTableSchema,
    primary_keys: Vec<Expr>,
    upsert_rows: Vec<&PgTableRow>,
    delete_predicate: Option<Expr>,
) -> DeltaResult<DeltaTable> {
    let ops = DeltaOps::from(table);
    let rows = TableRowEncoder::encode_table_rows(table_schema, upsert_rows)?;

    let ctx = SessionContext::new();
    let batch = ctx.read_batch(rows)?;
    let mut merge_builder = ops
        .merge(
            batch,
            qualify_primary_keys(primary_keys, "source", "target"),
        )
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
    let (table, _metrics) = merge_builder.await?;
    Ok(table)
}
