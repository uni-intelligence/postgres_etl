use crdts::LWWReg;
use deltalake::datafusion::{
    common::Column,
    prelude::{Expr, lit},
};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, Event, PgLsn, TableRow, TableSchema},
};
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug, Clone, PartialEq)]
enum RowOp<'a> {
    Upsert(&'a TableRow),
    Delete,
}

/// Convert `Cell` to DataFusion `ScalarValue` wrapped as a literal `Expr`.
fn cell_to_scalar_expr(cell: &Cell, schema: &TableSchema, col_idx: usize) -> EtlResult<Expr> {
    use crate::deltalake::schema::TableRowEncoder;
    let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(
        &schema.column_schemas[col_idx].typ,
        schema.column_schemas[col_idx].modifier,
    );
    let sv = TableRowEncoder::cell_to_scalar_value_for_arrow(cell, &arrow_type)?;
    Ok(lit(sv))
}

/// Build a DataFusion predicate `Expr` representing equality over all primary key columns
/// for the provided `row` according to `table_schema`.
fn build_pk_expr(table_schema: &TableSchema, row: &TableRow) -> EtlResult<Expr> {
    let mut pk_expr: Option<Expr> = None;
    for (idx, column_schema) in table_schema.column_schemas.iter().enumerate() {
        if !column_schema.primary {
            continue;
        }
        let value_expr = cell_to_scalar_expr(&row.values[idx], table_schema, idx)?;
        let this_col_expr =
            Expr::Column(Column::new_unqualified(column_schema.name.clone())).eq(value_expr);
        pk_expr = Some(match pk_expr {
            None => this_col_expr,
            Some(acc) => acc.and(this_col_expr),
        });
    }

    pk_expr.ok_or_else(|| {
        etl_error!(
            ErrorKind::MissingTableSchema,
            "Table has no primary key columns",
            table_schema.name.to_string()
        )
    })
}

/// Materialize events into delete and upsert predicates
pub(crate) fn materialize_events<'a>(
    events: &'a [Event],
    table_schema: &TableSchema,
    is_append_only: bool,
) -> EtlResult<(Vec<Expr>, Vec<&'a TableRow>)> {
    let mut crdt_by_key: HashMap<Expr, LWWReg<RowOp, (PgLsn, PgLsn)>> = HashMap::new();

    for event in events.iter() {
        match event {
            Event::Insert(e) => {
                let marker = (e.commit_lsn, e.start_lsn);
                let pk_expr = build_pk_expr(table_schema, &e.table_row)?;
                let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                    val: RowOp::Upsert(&e.table_row),
                    marker,
                });
                entry.update(RowOp::Upsert(&e.table_row), marker);
            }
            Event::Update(e) => {
                if is_append_only {
                    warn!("Received update event for append-only table, ignoring",);
                    continue;
                }
                let marker = (e.commit_lsn, e.start_lsn);
                let pk_expr = build_pk_expr(table_schema, &e.table_row)?;
                let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                    val: RowOp::Upsert(&e.table_row),
                    marker,
                });
                entry.update(RowOp::Upsert(&e.table_row), marker);
            }
            Event::Delete(e) => {
                if is_append_only {
                    warn!("Received delete event for append-only table, ignoring",);
                    continue;
                }
                if let Some((_, ref old_row)) = e.old_table_row {
                    let marker = (e.commit_lsn, e.start_lsn);
                    let pk_expr = build_pk_expr(table_schema, old_row)?;
                    let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                        val: RowOp::Delete,
                        marker,
                    });
                    entry.update(RowOp::Delete, marker);
                } else {
                    warn!("Delete event missing old_table_row for table");
                }
            }
            Event::Truncate(_) => {
                // TODO(abhi): Implement truncate event handling
                warn!("Truncate event not implemented");
            }
            Event::Relation(_) | Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {
                // Skip non-row events
            }
        }
    }

    let mut delete_predicates: Vec<Expr> = Vec::new();
    let mut upsert_rows: Vec<&TableRow> = Vec::new();

    for (expr, reg) in crdt_by_key.into_iter() {
        match reg.val {
            RowOp::Delete => delete_predicates.push(expr),
            RowOp::Upsert(row) => upsert_rows.push(row),
        }
    }

    Ok((delete_predicates, upsert_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::{
        Cell, ColumnSchema, DeleteEvent, InsertEvent, PgLsn, TableId, TableName, TableRow,
        TableSchema, Type, UpdateEvent,
    };

    fn schema_single_pk(table_id: TableId) -> TableSchema {
        TableSchema::new(
            table_id,
            TableName::new("public".to_string(), "t".to_string()),
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    typ: Type::INT8,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    primary: false,
                    nullable: true,
                },
            ],
        )
    }

    fn row(id: i64, name: &str) -> TableRow {
        TableRow {
            values: vec![Cell::I64(id), Cell::String(name.to_string())],
        }
    }

    fn schema_composite_pk(table_id: TableId) -> TableSchema {
        TableSchema::new(
            table_id,
            TableName::new("public".to_string(), "t".to_string()),
            vec![
                ColumnSchema {
                    name: "tenant_id".to_string(),
                    typ: Type::INT4,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                ColumnSchema {
                    name: "user_id".to_string(),
                    typ: Type::INT8,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    primary: false,
                    nullable: true,
                },
            ],
        )
    }

    fn row_composite(tenant: i32, user: i64, name: &str) -> TableRow {
        TableRow {
            values: vec![
                Cell::I32(tenant),
                Cell::I64(user),
                Cell::String(name.to_string()),
            ],
        }
    }

    #[test]
    fn lww_reg_uses_commit_then_start_lsn() {
        let table_id = TableId(1);
        let schema = schema_single_pk(table_id);

        // Earlier commit/start pair
        let e1 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(10u64),
            commit_lsn: PgLsn::from(20u64),
            table_id,
            table_row: row(1, "a"),
        });
        // Later commit wins
        let e2 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(11u64),
            commit_lsn: PgLsn::from(21u64),
            table_id,
            table_row: row(1, "b"),
        });

        let events = vec![e1, e2];

        let (deletes, upserts) = materialize_events(&events, &schema, false).unwrap();
        assert!(deletes.is_empty());
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[1], Cell::String("b".to_string()));
    }

    #[test]
    fn delete_overrides_prior_upsert_for_same_pk() {
        let table_id = TableId(1);
        let schema = schema_single_pk(table_id);

        let ins = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1u64),
            commit_lsn: PgLsn::from(2u64),
            table_id,
            table_row: row(1, "a"),
        });
        let del = Event::Delete(DeleteEvent {
            start_lsn: PgLsn::from(3u64),
            commit_lsn: PgLsn::from(4u64),
            table_id,
            old_table_row: Some((false, row(1, "a"))),
        });

        let events = vec![ins, del];

        let (deletes, upserts) = materialize_events(&events, &schema, false).unwrap();
        assert!(upserts.is_empty());
        assert_eq!(deletes.len(), 1);
    }

    #[test]
    fn update_on_append_only_is_ignored() {
        let table_id = TableId(1);
        let schema = schema_single_pk(table_id);
        let ins = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1u64),
            commit_lsn: PgLsn::from(2u64),
            table_id,
            table_row: row(1, "a"),
        });
        let upd = Event::Update(UpdateEvent {
            start_lsn: PgLsn::from(3u64),
            commit_lsn: PgLsn::from(4u64),
            table_id,
            table_row: row(1, "b"),
            old_table_row: Some((false, row(1, "a"))),
        });

        let events = vec![ins, upd];

        // append_only = true, so update ignored, last write stays as insert
        let (_deletes, upserts) = materialize_events(&events, &schema, true).unwrap();
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[1], Cell::String("a".to_string()));
    }

    #[test]
    fn composite_pk_predicate_and_lww() {
        let table_id = TableId(42);
        let schema = schema_composite_pk(table_id);

        // Inserts for two different composite PKs
        let ins1 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1u64),
            commit_lsn: PgLsn::from(2u64),
            table_id,
            table_row: row_composite(10, 100, "a"),
        });
        let ins2 = Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1u64),
            commit_lsn: PgLsn::from(2u64),
            table_id,
            table_row: row_composite(10, 101, "b"),
        });

        // Update to the first composite key with later commit/start
        let upd1 = Event::Update(UpdateEvent {
            start_lsn: PgLsn::from(3u64),
            commit_lsn: PgLsn::from(4u64),
            table_id,
            table_row: row_composite(10, 100, "a2"),
            old_table_row: Some((false, row_composite(10, 100, "a"))),
        });

        // Delete the second composite key with even later lsn
        let del2 = Event::Delete(DeleteEvent {
            start_lsn: PgLsn::from(5u64),
            commit_lsn: PgLsn::from(6u64),
            table_id,
            old_table_row: Some((false, row_composite(10, 101, "b"))),
        });

        let events = vec![ins1, ins2, upd1, del2];

        let (deletes, upserts) = materialize_events(&events, &schema, false).unwrap();

        // We expect one delete predicate (for tenant_id=10 AND user_id=101)
        // and one upsert (tenant_id=10 AND user_id=100 with name=a2)
        assert_eq!(deletes.len(), 1);
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[2], Cell::String("a2".to_string()));
    }
}
