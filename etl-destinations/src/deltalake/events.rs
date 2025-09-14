use crate::deltalake::util::LWWReg;
use deltalake::datafusion::{common::HashMap, prelude::Expr};
use etl::{
    error::EtlResult,
    types::{Event, PgLsn, TableRow as PgTableRow, TableSchema as PgTableSchema},
};
use tracing::warn;

use crate::deltalake::expr::build_pk_expr;

#[derive(Debug, Clone, PartialEq)]
enum RowOp<'a> {
    Upsert(&'a PgTableRow),
    Delete,
}

pub fn materialize_events_append_only<'a>(
    events: &'a [Event],
    table_schema: &PgTableSchema,
) -> EtlResult<Vec<&'a PgTableRow>> {
    let mut crdt_by_key: HashMap<Expr, LWWReg<RowOp, (PgLsn, PgLsn)>> = HashMap::new();

    for event in events.iter() {
        match event {
            Event::Insert(e) => {
                let marker = (e.commit_lsn, e.start_lsn);
                let pk_expr = build_pk_expr(table_schema, &e.table_row);
                let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                    val: RowOp::Upsert(&e.table_row),
                    marker,
                });
                entry.update(RowOp::Upsert(&e.table_row), marker);
            }
            Event::Update(_) => {
                warn!("Received update event for append-only table, ignoring");
            }
            Event::Delete(_) => {
                warn!("Received delete event for append-only table, ignoring");
            }
            Event::Relation(_)
            | Event::Begin(_)
            | Event::Commit(_)
            | Event::Truncate(_)
            | Event::Unsupported => {
                // Skip non-row events
            }
        }
    }

    let mut upsert_rows: Vec<&PgTableRow> = Vec::new();
    for (_, reg) in crdt_by_key.into_iter() {
        if let RowOp::Upsert(row) = reg.val {
            upsert_rows.push(row)
        }
    }

    Ok(upsert_rows)
}

/// Materialize events into delete and upsert predicates
pub fn materialize_events<'a>(
    events: &'a [Event],
    table_schema: &PgTableSchema,
) -> EtlResult<(Vec<Expr>, Vec<&'a PgTableRow>)> {
    let mut crdt_by_key: HashMap<Expr, LWWReg<RowOp, (PgLsn, PgLsn)>> = HashMap::new();

    for event in events.iter() {
        match event {
            Event::Insert(e) => {
                let marker = (e.commit_lsn, e.start_lsn);
                let pk_expr = build_pk_expr(table_schema, &e.table_row);
                let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                    val: RowOp::Upsert(&e.table_row),
                    marker,
                });
                entry.update(RowOp::Upsert(&e.table_row), marker);
            }
            Event::Update(e) => {
                let marker = (e.commit_lsn, e.start_lsn);
                let pk_expr = build_pk_expr(table_schema, &e.table_row);
                let entry = crdt_by_key.entry(pk_expr).or_insert_with(|| LWWReg {
                    val: RowOp::Upsert(&e.table_row),
                    marker,
                });
                entry.update(RowOp::Upsert(&e.table_row), marker);
            }
            Event::Delete(e) => {
                if let Some((_, ref old_row)) = e.old_table_row {
                    let marker = (e.commit_lsn, e.start_lsn);
                    let pk_expr = build_pk_expr(table_schema, old_row);
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
    let mut upsert_rows: Vec<&PgTableRow> = Vec::new();

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
        Cell as PgCell, ColumnSchema as PgColumnSchema, DeleteEvent, InsertEvent, TableId,
        TableName, Type as PgType, UpdateEvent,
    };

    fn schema_single_pk(table_id: TableId) -> PgTableSchema {
        PgTableSchema::new(
            table_id,
            TableName::new("public".to_string(), "t".to_string()),
            vec![
                PgColumnSchema {
                    name: "id".to_string(),
                    typ: PgType::INT8,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                PgColumnSchema {
                    name: "name".to_string(),
                    typ: PgType::TEXT,
                    modifier: -1,
                    primary: false,
                    nullable: true,
                },
            ],
        )
    }

    fn row(id: i64, name: &str) -> PgTableRow {
        PgTableRow {
            values: vec![PgCell::I64(id), PgCell::String(name.to_string())],
        }
    }

    fn schema_composite_pk(table_id: TableId) -> PgTableSchema {
        PgTableSchema::new(
            table_id,
            TableName::new("public".to_string(), "t".to_string()),
            vec![
                PgColumnSchema {
                    name: "tenant_id".to_string(),
                    typ: PgType::INT4,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                PgColumnSchema {
                    name: "user_id".to_string(),
                    typ: PgType::INT8,
                    modifier: -1,
                    primary: true,
                    nullable: false,
                },
                PgColumnSchema {
                    name: "name".to_string(),
                    typ: PgType::TEXT,
                    modifier: -1,
                    primary: false,
                    nullable: true,
                },
            ],
        )
    }

    fn row_composite(tenant: i32, user: i64, name: &str) -> PgTableRow {
        PgTableRow {
            values: vec![
                PgCell::I32(tenant),
                PgCell::I64(user),
                PgCell::String(name.to_string()),
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

        let (deletes, upserts) = materialize_events(&events, &schema).unwrap();
        assert!(deletes.is_empty());
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[1], PgCell::String("b".to_string()));
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

        let (deletes, upserts) = materialize_events(&events, &schema).unwrap();
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

        let upserts = materialize_events_append_only(&events, &schema).unwrap();
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[1], PgCell::String("a".to_string()));
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

        let (deletes, upserts) = materialize_events(&events, &schema).unwrap();

        // We expect one delete predicate (for tenant_id=10 AND user_id=101)
        // and one upsert (tenant_id=10 AND user_id=100 with name=a2)
        assert_eq!(deletes.len(), 1);
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].values[2], PgCell::String("a2".to_string()));
    }
}
