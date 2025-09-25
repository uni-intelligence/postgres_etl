use deltalake::DeltaTableError;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::operations::merge::MergeBuilder;
use deltalake::{DeltaResult, DeltaTable, datafusion::prelude::Expr};
use etl::types::{TableRow as PgTableRow, TableSchema as PgTableSchema};
use tracing::{instrument, trace};

use crate::arrow::rows_to_record_batch;
use crate::deltalake::config::DeltaTableConfig;
use crate::deltalake::expr::qualify_primary_keys;
use crate::deltalake::schema::postgres_to_arrow_schema;

pub(crate) fn source_qualified_column_expr(column_name: &str, source_alias: &str) -> Expr {
    Expr::Column(Column::new(Some(source_alias), column_name))
}

#[instrument(
    skip(table, config, table_schema, upsert_rows, delete_predicate),
    fields(upsert_count = upsert_rows.len(), has_delete = delete_predicate.is_some())
)]
pub async fn merge_to_table(
    table: &mut DeltaTable,
    config: &DeltaTableConfig,
    table_schema: &PgTableSchema,
    upsert_rows: &[PgTableRow],
    delete_predicate: Option<Expr>,
) -> DeltaResult<()> {
    trace!("Building Arrow schema and source batch for merge");
    let arrow_schema = postgres_to_arrow_schema(table_schema)?;
    let rows = rows_to_record_batch(upsert_rows, arrow_schema)?;

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

    trace!("Creating merge builder");
    let merge_builder = MergeBuilder::new(
        // TODO(abhi): Is there a way to do this while avoiding the clone/general hackiness?
        (*table).log_store(),
        table.snapshot()?.clone(),
        qualified_primary_keys,
        batch,
    );

    // TODO(abhi): Clean up this mess
    let all_columns: Vec<&str> = table_schema
        .column_schemas
        .iter()
        .map(|col| col.name.as_str())
        .collect();

    let mut merge_builder = merge_builder
        .with_writer_properties(config.into())
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
            all_columns.iter().fold(insert, |insert, &column| {
                insert.set(
                    column.to_string(),
                    source_qualified_column_expr(column, "source"),
                )
            })
        })?
        .when_matched_update(|update| {
            all_columns.iter().fold(update, |update, &column| {
                update.update(
                    column.to_string(),
                    source_qualified_column_expr(column, "source"),
                )
            })
        })?;

    if let Some(delete_predicate) = delete_predicate {
        merge_builder = merge_builder
            .when_not_matched_by_source_delete(|delete| delete.predicate(delete_predicate))?;
    }
    // TODO(abhi): Do something with the metrics
    trace!("Executing merge operation");
    let (merged_table, _metrics) = merge_builder.await?;
    trace!("Merge operation completed");
    *table = merged_table;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_debug_snapshot;

    #[test]
    fn source_qualified_column_expr_preserves_case_and_alias() {
        let expr = source_qualified_column_expr("CASESensitivecolumn", "source");

        assert_debug_snapshot!(expr, @r#"
        Column(
            Column {
                relation: Some(
                    Bare {
                        table: "source",
                    },
                ),
                name: "CASESensitivecolumn",
            },
        )
        "#);
    }

    #[test]
    fn source_qualified_column_expr_handles_lowercase() {
        let expr = source_qualified_column_expr("lowercasecolumn", "source");

        assert_debug_snapshot!(expr, @r#"
        Column(
            Column {
                relation: Some(
                    Bare {
                        table: "source",
                    },
                ),
                name: "lowercasecolumn",
            },
        )
        "#);
    }
}
