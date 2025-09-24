// Utilities related to constructing DataFusion expressions

use crate::deltalake::schema::TableRowEncoder;
use crate::deltalake::schema::cell_to_scalar_value_for_arrow;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::prelude::{Expr, lit};
use etl::error::EtlResult;
use etl::types::{Cell as PgCell, TableRow as PgTableRow, TableSchema as PgTableSchema};

/// Convert `Cell` to DataFusion `ScalarValue` wrapped as a literal `Expr`.
pub fn cell_to_scalar_expr(
    cell: &PgCell,
    schema: &PgTableSchema,
    col_idx: usize,
) -> EtlResult<Expr> {
    let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(
        &schema.column_schemas[col_idx].typ,
        schema.column_schemas[col_idx].modifier,
    );
    let sv = cell_to_scalar_value_for_arrow(cell, &arrow_type)?;
    Ok(lit(sv))
}

/// Build a DataFusion predicate `Expr` representing equality over all primary key columns
/// for the provided `row` according to `table_schema`.
pub fn build_pk_expr(table_schema: &PgTableSchema, row: &PgTableRow) -> Expr {
    let mut pk_expr: Option<Expr> = None;
    for (idx, column_schema) in table_schema.column_schemas.iter().enumerate() {
        if !column_schema.primary {
            continue;
        }
        let value_expr = cell_to_scalar_expr(&row.values[idx], table_schema, idx)
            .expect("Failed to convert cell to scalar expression");
        let this_col_expr =
            Expr::Column(Column::new_unqualified(column_schema.name.clone())).eq(value_expr);
        pk_expr = Some(match pk_expr {
            None => this_col_expr,
            Some(acc) => acc.and(this_col_expr),
        });
    }

    // In practice, this should never happen as the tables we're replicating are guaranteed to have primary keys
    pk_expr.expect("Table has no primary key columns")
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
    use deltalake::datafusion::logical_expr::Operator::{And, Eq};
    use deltalake::datafusion::logical_expr::{col, lit};
    use etl::types::{ColumnSchema as PgColumnSchema, TableName, Type as PgType};
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

        let pk_expr = build_pk_expr(&schema, &row);

        // The expression should be an equality comparison
        match pk_expr {
            Expr::BinaryExpr(binary_expr) => {
                assert!(matches!(binary_expr.op, Eq));

                // Left side should be a column reference
                match &*binary_expr.left {
                    Expr::Column(column) => {
                        assert_eq!(column.name, "id");
                    }
                    _ => panic!("Expected column reference on left side"),
                }

                // Right side should be a literal
                match &*binary_expr.right {
                    Expr::Literal(_, _) => {}
                    _ => panic!("Expected literal on right side"),
                }
            }
            _ => panic!("Expected binary expression for single primary key"),
        }
    }

    #[test]
    fn test_build_pk_expr_composite_primary_key() {
        let schema = create_composite_pk_schema();
        let row = create_composite_pk_row();

        let pk_expr = build_pk_expr(&schema, &row);

        // The expression should be an AND of two equality comparisons
        match pk_expr {
            Expr::BinaryExpr(binary_expr) => {
                assert!(matches!(binary_expr.op, And));

                // Both sides should be equality expressions
                match (&*binary_expr.left, &*binary_expr.right) {
                    (Expr::BinaryExpr(left_eq), Expr::BinaryExpr(right_eq)) => {
                        assert!(matches!(left_eq.op, Eq));
                        assert!(matches!(right_eq.op, Eq));
                    }
                    _ => panic!("Expected equality expressions on both sides of AND"),
                }
            }
            _ => panic!("Expected AND expression for composite primary key"),
        }
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

        // This should panic as stated in the function documentation
        let result = std::panic::catch_unwind(|| build_pk_expr(&schema, &row));
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

        // This should still work - the conversion should handle null values
        let pk_expr = build_pk_expr(&schema, &row_with_null_pk);

        // Verify it's still an equality expression
        match pk_expr {
            Expr::BinaryExpr(binary_expr) => {
                assert!(matches!(binary_expr.op, Eq));
            }
            _ => panic!("Expected binary expression even with null primary key"),
        }
    }

    #[test]
    fn test_build_pk_expr_expression_structure() {
        let schema = create_composite_pk_schema();
        let row = create_composite_pk_row();

        let pk_expr = build_pk_expr(&schema, &row);

        // Helper function to verify expression structure recursively
        fn verify_pk_expression(expr: &Expr, expected_columns: &[&str]) -> bool {
            match expr {
                Expr::BinaryExpr(binary_expr) => {
                    match binary_expr.op {
                        Eq => {
                            // This should be a leaf equality expression
                            if let Expr::Column(column) = &*binary_expr.left {
                                expected_columns.contains(&column.name.as_str())
                            } else {
                                false
                            }
                        }
                        And => {
                            // This should be an AND of other expressions
                            verify_pk_expression(&binary_expr.left, expected_columns)
                                && verify_pk_expression(&binary_expr.right, expected_columns)
                        }
                        _ => false,
                    }
                }
                _ => false,
            }
        }

        assert!(verify_pk_expression(&pk_expr, &["tenant_id", "user_id"]));
    }

    #[test]
    fn test_qualify_primary_keys_single_column() {
        use deltalake::datafusion::prelude::col;

        let primary_keys = vec![col("id")];
        let result = qualify_primary_keys(primary_keys, "source", "target");

        // Should create: source.id = target.id
        match result {
            Some(Expr::BinaryExpr(binary_expr)) => {
                assert!(matches!(binary_expr.op, Eq));

                // Left side should be source.id
                match &*binary_expr.left {
                    Expr::Column(column) => {
                        assert_eq!(column.relation, Some("source".into()));
                        assert_eq!(column.name, "id");
                    }
                    _ => panic!("Expected qualified source column on left side"),
                }

                // Right side should be target.id
                match &*binary_expr.right {
                    Expr::Column(column) => {
                        assert_eq!(column.relation, Some("target".into()));
                        assert_eq!(column.name, "id");
                    }
                    _ => panic!("Expected qualified target column on right side"),
                }
            }
            _ => panic!("Expected binary expression for single primary key"),
        }
    }

    #[test]
    fn test_qualify_primary_keys_composite_columns() {
        let primary_keys = vec![col("tenant_id"), col("user_id")];
        let result = qualify_primary_keys(primary_keys, "src", "tgt");

        // Should create: src.tenant_id = tgt.tenant_id AND src.user_id = tgt.user_id
        match result {
            Some(Expr::BinaryExpr(binary_expr)) => {
                assert!(matches!(binary_expr.op, And));

                // Both sides should be equality expressions
                match (&*binary_expr.left, &*binary_expr.right) {
                    (Expr::BinaryExpr(left_eq), Expr::BinaryExpr(right_eq)) => {
                        assert!(matches!(left_eq.op, Eq));
                        assert!(matches!(right_eq.op, Eq));

                        // Verify left equality (first primary key)
                        match (&*left_eq.left, &*left_eq.right) {
                            (Expr::Column(src_col), Expr::Column(tgt_col)) => {
                                assert_eq!(src_col.relation, Some("src".into()));
                                assert_eq!(src_col.name, "tenant_id");
                                assert_eq!(tgt_col.relation, Some("tgt".into()));
                                assert_eq!(tgt_col.name, "tenant_id");
                            }
                            _ => panic!("Expected qualified columns in first equality"),
                        }

                        // Verify right equality (second primary key)
                        match (&*right_eq.left, &*right_eq.right) {
                            (Expr::Column(src_col), Expr::Column(tgt_col)) => {
                                assert_eq!(src_col.relation, Some("src".into()));
                                assert_eq!(src_col.name, "user_id");
                                assert_eq!(tgt_col.relation, Some("tgt".into()));
                                assert_eq!(tgt_col.name, "user_id");
                            }
                            _ => panic!("Expected qualified columns in second equality"),
                        }
                    }
                    _ => panic!("Expected equality expressions on both sides of AND"),
                }
            }
            _ => panic!("Expected AND expression for composite primary key"),
        }
    }

    #[test]
    fn test_qualify_primary_keys_multiple_columns() {
        let primary_keys = vec![col("a"), col("b"), col("c")];
        let result = qualify_primary_keys(primary_keys, "s", "t");

        fn verify_qualified_expression(
            expr: &Expr,
            expected_columns: &[&str],
            source: &str,
            target: &str,
        ) -> bool {
            match expr {
                Expr::BinaryExpr(binary_expr) => {
                    match binary_expr.op {
                        Eq => {
                            // This should be a leaf equality expression
                            match (&*binary_expr.left, &*binary_expr.right) {
                                (Expr::Column(src_col), Expr::Column(tgt_col)) => {
                                    src_col.relation == Some(source.into())
                                        && tgt_col.relation == Some(target.into())
                                        && src_col.name == tgt_col.name
                                        && expected_columns.contains(&src_col.name.as_str())
                                }
                                _ => false,
                            }
                        }
                        And => {
                            // This should be an AND of other expressions
                            verify_qualified_expression(
                                &binary_expr.left,
                                expected_columns,
                                source,
                                target,
                            ) && verify_qualified_expression(
                                &binary_expr.right,
                                expected_columns,
                                source,
                                target,
                            )
                        }
                        _ => false,
                    }
                }
                _ => false,
            }
        }

        assert!(verify_qualified_expression(
            &result.unwrap(),
            &["a", "b", "c"],
            "s",
            "t"
        ));
    }

    #[test]
    fn test_qualify_primary_keys_empty_list() {
        let primary_keys: Vec<Expr> = vec![];

        let res = qualify_primary_keys(primary_keys, "source", "target");
        assert!(res.is_none());
    }

    #[test]
    fn test_qualify_primary_keys_different_aliases() {
        let primary_keys = vec![col("key")];
        let result = qualify_primary_keys(primary_keys, "new_records", "existing_table");

        match result.unwrap() {
            Expr::BinaryExpr(binary_expr) => match (&*binary_expr.left, &*binary_expr.right) {
                (Expr::Column(src_col), Expr::Column(tgt_col)) => {
                    assert_eq!(src_col.relation, Some("new_records".into()));
                    assert_eq!(src_col.name, "key");
                    assert_eq!(tgt_col.relation, Some("existing_table".into()));
                    assert_eq!(tgt_col.name, "key");
                }
                _ => panic!("Expected qualified columns"),
            },
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_qualify_primary_keys_invalid_expression() {
        // Pass a literal instead of a column expression
        let primary_keys = vec![lit(42)];
        let res = qualify_primary_keys(primary_keys, "source", "target");
        assert!(res.is_none());
    }
}
