// Utilities related to constructing DataFusion expressions

use deltalake::datafusion::prelude::Expr;
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::datafusion::{common::Column, prelude::lit};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{
        Cell as PgCell, ColumnSchema as PgColumnSchema, TableId, TableName, TableRow as PgTableRow,
        TableSchema as PgTableSchema,
    },
};

use crate::{arrow::rows_to_record_batch, deltalake::schema::postgres_to_arrow_schema};

/// Build a DataFusion predicate `Expr` representing equality over all primary key columns
/// for the provided `row` according to `table_schema`.
pub fn build_pk_expr(table_schema: &PgTableSchema, row: &PgTableRow) -> EtlResult<Expr> {
    let mut pk_expr: Option<Expr> = None;
    for (idx, column_schema) in table_schema.column_schemas.iter().enumerate() {
        if !column_schema.primary {
            continue;
        }
        let value_expr = cell_to_scalar_expr(&row.values[idx], column_schema)?;
        let this_col_expr =
            Expr::Column(Column::new_unqualified(column_schema.name.clone())).eq(value_expr);
        pk_expr = Some(match pk_expr {
            None => this_col_expr,
            Some(acc) => acc.and(this_col_expr),
        });
    }

    // In practice, this should never happen as the tables we're replicating are guaranteed to have primary keys
    pk_expr.ok_or(etl_error!(
        ErrorKind::ConversionError,
        "Table has no primary key columns"
    ))
}

/// Convert a Postgres [`PgCell`] into a DataFusion [`Expr`] literal.
fn cell_to_scalar_expr(cell: &PgCell, column_schema: &PgColumnSchema) -> EtlResult<Expr> {
    let single_col_schema = PgTableSchema {
        id: TableId::new(0),
        name: TableName::new("foo".to_string(), "bar".to_string()),
        column_schemas: vec![column_schema.clone()],
    };

    let arrow_schema = postgres_to_arrow_schema(&single_col_schema).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to convert table schema to Arrow schema",
            e
        )
    })?;
    let temp_row = vec![PgTableRow::new(vec![cell.clone()])];
    let array = rows_to_record_batch(&temp_row, arrow_schema).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to convert row to Arrow array",
            e
        )
    })?;
    let array = array.column(0);
    let scalar_value = ScalarValue::try_from_array(array, 0).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to convert cell to scalar expression",
            e
        )
    })?;
    Ok(lit(scalar_value))
}

/// Turns a set of primary key column expressions into qualified equality expressions
/// matching merge target/source.
///
/// Takes column expressions and creates qualified equality comparisons between
/// source and target aliases for merge operations.
///
/// # Examples
/// - `col("id")` becomes `source.id = target.id`
/// - `[col("tenant_id"), col("user_id")]` becomes `source.tenant_id = target.tenant_id AND source.user_id = target.user_id`
pub fn qualify_primary_keys(
    primary_keys: Vec<Expr>,
    source_alias: &str,
    target_alias: &str,
) -> Option<Expr> {
    primary_keys
        .into_iter()
        .filter_map(|key_expr| {
            // Extract column name from the expression
            let column_name = match key_expr {
                Expr::Column(column) => column.name,
                _ => return None,
            };

            let source_col = Expr::Column(Column::new(Some(source_alias), &column_name));
            let target_col = Expr::Column(Column::new(Some(target_alias), &column_name));

            Some(source_col.eq(target_col))
        })
        .fold(None, |acc: Option<Expr>, eq_expr| match acc {
            None => Some(eq_expr),
            Some(acc) => Some(acc.and(eq_expr)),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use deltalake::datafusion::logical_expr::{col, lit};
    use etl::types::{ColumnSchema as PgColumnSchema, TableName, Type as PgType};
    use insta::assert_debug_snapshot;

    /// Create a test table schema with various column types.
    fn create_test_schema() -> PgTableSchema {
        PgTableSchema {
            id: etl::types::TableId(1),
            name: TableName::new("public".to_string(), "test_table".to_string()),
            column_schemas: vec![
                PgColumnSchema::new("id".to_string(), PgType::INT8, -1, false, true), // Primary key
                PgColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
                PgColumnSchema::new("age".to_string(), PgType::INT4, -1, true, false),
                PgColumnSchema::new("is_active".to_string(), PgType::BOOL, -1, true, false),
                PgColumnSchema::new("created_at".to_string(), PgType::TIMESTAMP, -1, true, false),
            ],
        }
    }

    /// Create a test table schema with multiple primary key columns.
    fn create_composite_pk_schema() -> PgTableSchema {
        PgTableSchema {
            id: etl::types::TableId(2),
            name: TableName::new("public".to_string(), "composite_pk_table".to_string()),
            column_schemas: vec![
                PgColumnSchema::new("tenant_id".to_string(), PgType::INT4, -1, false, true), // Primary key 1
                PgColumnSchema::new("user_id".to_string(), PgType::INT8, -1, false, true), // Primary key 2
                PgColumnSchema::new("data".to_string(), PgType::TEXT, -1, true, false),
            ],
        }
    }

    /// Create a test row matching the test schema.
    fn create_test_row() -> PgTableRow {
        let timestamp = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
        );

        PgTableRow::new(vec![
            PgCell::I64(12345),
            PgCell::String("John Doe".to_string()),
            PgCell::I32(30),
            PgCell::Bool(true),
            PgCell::Timestamp(timestamp),
        ])
    }

    /// Create a test row for composite primary key schema.
    fn create_composite_pk_row() -> PgTableRow {
        PgTableRow::new(vec![
            PgCell::I32(1),                          // tenant_id
            PgCell::I64(42),                         // user_id
            PgCell::String("test data".to_string()), // data
        ])
    }

    #[test]
    fn test_build_pk_expr_single_primary_key() {
        let schema = create_test_schema();
        let row = create_test_row();

        let pk_expr = build_pk_expr(&schema, &row).unwrap();

        assert_debug_snapshot!(pk_expr, @r#"
        BinaryExpr(
            BinaryExpr {
                left: Column(
                    Column {
                        relation: None,
                        name: "id",
                    },
                ),
                op: Eq,
                right: Literal(
                    Int64(12345),
                    None,
                ),
            },
        )
        "#);
    }

    #[test]
    fn test_build_pk_expr_composite_primary_key() {
        let schema = create_composite_pk_schema();
        let row = create_composite_pk_row();

        let pk_expr = build_pk_expr(&schema, &row).unwrap();

        assert_debug_snapshot!(pk_expr, @r#"
        BinaryExpr(
            BinaryExpr {
                left: BinaryExpr(
                    BinaryExpr {
                        left: Column(
                            Column {
                                relation: None,
                                name: "tenant_id",
                            },
                        ),
                        op: Eq,
                        right: Literal(
                            Int32(1),
                            None,
                        ),
                    },
                ),
                op: And,
                right: BinaryExpr(
                    BinaryExpr {
                        left: Column(
                            Column {
                                relation: None,
                                name: "user_id",
                            },
                        ),
                        op: Eq,
                        right: Literal(
                            Int64(42),
                            None,
                        ),
                    },
                ),
            },
        )
        "#);
    }

    #[test]
    fn test_build_pk_expr_no_primary_keys() {
        // Create schema with no primary key columns
        let schema = PgTableSchema {
            id: etl::types::TableId(4),
            name: TableName::new("public".to_string(), "no_pk_table".to_string()),
            column_schemas: vec![
                PgColumnSchema::new("col1".to_string(), PgType::TEXT, -1, true, false),
                PgColumnSchema::new("col2".to_string(), PgType::INT4, -1, true, false),
            ],
        };
        let row = PgTableRow::new(vec![PgCell::String("test".to_string()), PgCell::I32(42)]);

        let result = build_pk_expr(&schema, &row);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_pk_expr_with_nulls_in_primary_key() {
        let schema = create_test_schema();
        let row_with_null_pk = PgTableRow::new(vec![
            PgCell::Null, // NULL in primary key column
            PgCell::String("John Doe".to_string()),
            PgCell::I32(30),
            PgCell::Bool(true),
            PgCell::Timestamp(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
                NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
            )),
        ]);

        let pk_expr = build_pk_expr(&schema, &row_with_null_pk);
        assert!(pk_expr.is_err());
    }

    #[test]
    fn test_build_pk_expr_expression_structure() {
        let schema = create_composite_pk_schema();
        let row = create_composite_pk_row();

        let pk_expr = build_pk_expr(&schema, &row).unwrap();

        assert_debug_snapshot!(pk_expr, @r#"
        BinaryExpr(
            BinaryExpr {
                left: BinaryExpr(
                    BinaryExpr {
                        left: Column(
                            Column {
                                relation: None,
                                name: "tenant_id",
                            },
                        ),
                        op: Eq,
                        right: Literal(
                            Int32(1),
                            None,
                        ),
                    },
                ),
                op: And,
                right: BinaryExpr(
                    BinaryExpr {
                        left: Column(
                            Column {
                                relation: None,
                                name: "user_id",
                            },
                        ),
                        op: Eq,
                        right: Literal(
                            Int64(42),
                            None,
                        ),
                    },
                ),
            },
        )
        "#);
    }

    #[test]
    fn test_qualify_primary_keys_single_column() {
        use deltalake::datafusion::prelude::col;

        let primary_keys = vec![col("id")];
        let result = qualify_primary_keys(primary_keys, "source", "target");

        assert_debug_snapshot!(result, @r#"
        Some(
            BinaryExpr(
                BinaryExpr {
                    left: Column(
                        Column {
                            relation: Some(
                                Bare {
                                    table: "source",
                                },
                            ),
                            name: "id",
                        },
                    ),
                    op: Eq,
                    right: Column(
                        Column {
                            relation: Some(
                                Bare {
                                    table: "target",
                                },
                            ),
                            name: "id",
                        },
                    ),
                },
            ),
        )
        "#);
    }

    #[test]
    fn test_qualify_primary_keys_composite_columns() {
        let primary_keys = vec![col("tenant_id"), col("user_id")];
        let result = qualify_primary_keys(primary_keys, "src", "tgt");

        assert_debug_snapshot!(result, @r#"
        Some(
            BinaryExpr(
                BinaryExpr {
                    left: BinaryExpr(
                        BinaryExpr {
                            left: Column(
                                Column {
                                    relation: Some(
                                        Bare {
                                            table: "src",
                                        },
                                    ),
                                    name: "tenant_id",
                                },
                            ),
                            op: Eq,
                            right: Column(
                                Column {
                                    relation: Some(
                                        Bare {
                                            table: "tgt",
                                        },
                                    ),
                                    name: "tenant_id",
                                },
                            ),
                        },
                    ),
                    op: And,
                    right: BinaryExpr(
                        BinaryExpr {
                            left: Column(
                                Column {
                                    relation: Some(
                                        Bare {
                                            table: "src",
                                        },
                                    ),
                                    name: "user_id",
                                },
                            ),
                            op: Eq,
                            right: Column(
                                Column {
                                    relation: Some(
                                        Bare {
                                            table: "tgt",
                                        },
                                    ),
                                    name: "user_id",
                                },
                            ),
                        },
                    ),
                },
            ),
        )
        "#);
    }

    #[test]
    fn test_qualify_primary_keys_multiple_columns() {
        let primary_keys = vec![col("a"), col("b"), col("c")];
        let result = qualify_primary_keys(primary_keys, "s", "t").unwrap();

        assert_debug_snapshot!(result, @r#"
        BinaryExpr(
            BinaryExpr {
                left: BinaryExpr(
                    BinaryExpr {
                        left: BinaryExpr(
                            BinaryExpr {
                                left: Column(
                                    Column {
                                        relation: Some(
                                            Bare {
                                                table: "s",
                                            },
                                        ),
                                        name: "a",
                                    },
                                ),
                                op: Eq,
                                right: Column(
                                    Column {
                                        relation: Some(
                                            Bare {
                                                table: "t",
                                            },
                                        ),
                                        name: "a",
                                    },
                                ),
                            },
                        ),
                        op: And,
                        right: BinaryExpr(
                            BinaryExpr {
                                left: Column(
                                    Column {
                                        relation: Some(
                                            Bare {
                                                table: "s",
                                            },
                                        ),
                                        name: "b",
                                    },
                                ),
                                op: Eq,
                                right: Column(
                                    Column {
                                        relation: Some(
                                            Bare {
                                                table: "t",
                                            },
                                        ),
                                        name: "b",
                                    },
                                ),
                            },
                        ),
                    },
                ),
                op: And,
                right: BinaryExpr(
                    BinaryExpr {
                        left: Column(
                            Column {
                                relation: Some(
                                    Bare {
                                        table: "s",
                                    },
                                ),
                                name: "c",
                            },
                        ),
                        op: Eq,
                        right: Column(
                            Column {
                                relation: Some(
                                    Bare {
                                        table: "t",
                                    },
                                ),
                                name: "c",
                            },
                        ),
                    },
                ),
            },
        )
        "#);
    }

    #[test]
    fn test_qualify_primary_keys_empty_list() {
        let primary_keys: Vec<Expr> = vec![];

        let res = qualify_primary_keys(primary_keys, "source", "target");
        assert!(res.is_none());
    }

    #[test]
    fn test_qualify_primary_keys_invalid_expression() {
        // Pass a literal instead of a column expression
        let primary_keys = vec![lit(42)];
        let res = qualify_primary_keys(primary_keys, "source", "target");
        assert!(res.is_none());
    }
}
