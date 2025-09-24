use std::sync::Arc;

use arrow::datatypes::Field;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::{DataType as DeltaDataType, StructField as DeltaStructField};
use deltalake::{DeltaResult, Schema as DeltaSchema};

use deltalake::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::scalar::ScalarValue;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{
    Cell as PGCell, TableRow as PgTableRow, TableSchema as PgTableSchema, Type as PgType,
};

/// Map Postgres types to appropriate Arrow types
pub fn postgres_type_to_arrow_type(pg_type: &PgType, modifier: i32) -> ArrowDataType {
    match *pg_type {
        // Boolean types
        PgType::BOOL => ArrowDataType::Boolean,
        PgType::BOOL_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Boolean, true))),

        // String types
        PgType::CHAR
        | PgType::BPCHAR
        | PgType::VARCHAR
        | PgType::NAME
        | PgType::TEXT
        | PgType::UUID
        | PgType::JSON
        | PgType::JSONB => ArrowDataType::Utf8,
        PgType::CHAR_ARRAY
        | PgType::BPCHAR_ARRAY
        | PgType::VARCHAR_ARRAY
        | PgType::NAME_ARRAY
        | PgType::TEXT_ARRAY
        | PgType::UUID_ARRAY
        | PgType::JSON_ARRAY
        | PgType::JSONB_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true))),

        // Integer types
        PgType::INT2 => ArrowDataType::Int16,
        PgType::INT2_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int16, true))),
        PgType::INT4 | PgType::OID => ArrowDataType::Int32,
        PgType::INT4_ARRAY | PgType::OID_ARRAY => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true)))
        }
        PgType::INT8 => ArrowDataType::Int64,
        PgType::INT8_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),

        // Float types
        PgType::FLOAT4 => ArrowDataType::Float32,
        PgType::FLOAT4_ARRAY => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float32, true)))
        }
        PgType::FLOAT8 => ArrowDataType::Float64,
        PgType::FLOAT8_ARRAY => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, true)))
        }
        PgType::NUMERIC => {
            let precision = extract_numeric_precision(modifier);
            let scale = extract_numeric_scale(modifier);
            ArrowDataType::Decimal128(precision, scale)
        }
        PgType::NUMERIC_ARRAY => {
            let precision = extract_numeric_precision(modifier);
            let scale = extract_numeric_scale(modifier);
            ArrowDataType::List(Arc::new(Field::new(
                "item",
                ArrowDataType::Decimal128(precision, scale),
                true,
            )))
        }
        // Date/Time types
        PgType::DATE => ArrowDataType::Date32,
        PgType::DATE_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Date32, true))),
        // Note: Delta Lake doesn't support standalone TIME, so we map to TIMESTAMP_NTZ
        PgType::TIME => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        PgType::TIME_ARRAY => ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ))),
        PgType::TIMESTAMP => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        PgType::TIMESTAMP_ARRAY => ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ))),
        PgType::TIMESTAMPTZ => ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        PgType::TIMESTAMPTZ_ARRAY => ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ))),

        // Binary types
        PgType::BYTEA => ArrowDataType::Binary,
        PgType::BYTEA_ARRAY => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Binary, true))),

        // Default fallback for unsupported types
        _ => ArrowDataType::Utf8,
    }
}

/// Convert Postgres PgTableSchema to Arrow Schema with proper type mapping
pub fn postgres_schema_to_arrow_schema(table_schema: &PgTableSchema) -> Result<Schema, ArrowError> {
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col_schema| {
            let data_type = postgres_type_to_arrow_type(&col_schema.typ, col_schema.modifier);
            Field::new(&col_schema.name, data_type, col_schema.nullable)
        })
        .collect();

    Ok(Schema::new(fields))
}

/// Convert a batch of TableRows to Arrow RecordBatch using PostgreSQL schema
pub fn encode_table_rows(
    table_schema: &PgTableSchema,
    table_rows: &[TableRow],
) -> Result<RecordBatch, ArrowError> {
    let arrow_schema = postgres_schema_to_arrow_schema(table_schema)?;

    if table_rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(arrow_schema)));
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
        let array = build_array_for_field(table_rows, field_idx, field.data_type());
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), arrays)?;
    Ok(batch)
}

/// Converts TableRows to Arrow RecordBatch for Delta Lake writes
pub struct TableRowEncoder;

impl TableRowEncoder {
    /// Convert a batch of TableRows to Arrow RecordBatch
    pub fn encode_table_rows(
        table_schema: &PgTableSchema,
        table_rows: Vec<&PgTableRow>,
    ) -> Result<RecordBatch, ArrowError> {
        // Convert to the format expected by the encoding function
        let rows: Vec<PgTableRow> = table_rows.into_iter().cloned().collect();
        encode_table_rows(table_schema, &rows)
    }

    /// Convert Postgres PgTableSchema to Arrow Schema with proper type mapping
    pub(crate) fn postgres_schema_to_arrow_schema(
        table_schema: &PgTableSchema,
    ) -> Result<ArrowSchema, ArrowError> {
        postgres_schema_to_arrow_schema(table_schema)
    }

    /// Map Postgres types to appropriate Arrow types
    pub(crate) fn postgres_type_to_arrow_type(pg_type: &PgType, modifier: i32) -> ArrowDataType {
        postgres_type_to_arrow_type(pg_type, modifier)
    }
}

/// Convert a single PGCell to a DataFusion ScalarValue according to the provided Arrow ArrowDataType.
///
/// This is a simplified implementation that delegates to the encoding module for type conversion.
pub(crate) fn cell_to_scalar_value_for_arrow(
    cell: &PGCell,
    _expected_type: &ArrowDataType,
) -> EtlResult<ScalarValue> {
    // Create a temporary single-element array and extract the scalar value
    let temp_row = PgTableRow::new(vec![cell.clone()]);
    let temp_schema = PgTableSchema {
        id: etl::types::TableId(0),
        name: etl::types::TableName::new("temp".to_string(), "temp".to_string()),
        column_schemas: vec![etl::types::ColumnSchema::new(
            "temp".to_string(),
            PgType::TEXT, // This will be overridden by the expected_type
            -1,
            true,
            false,
        )],
    };

    // Use encoding functions to create a batch and extract scalar value
    let batch = encode_table_rows(&temp_schema, &[temp_row]).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed converting Cell to Arrow array for ScalarValue",
            e
        )
    })?;

    let array = batch.column(0);
    ScalarValue::try_from_array(array, 0).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed converting Arrow array to ScalarValue",
            e
        )
    })
}

/// Convert a Postgres type to Delta ArrowDataType using delta-kernel's conversion traits
#[allow(dead_code)]
pub(crate) fn postgres_type_to_delta(typ: &PgType) -> Result<DeltaDataType, ArrowError> {
    let arrow_type = postgres_type_to_arrow_type(typ, -1);
    DeltaDataType::try_from_arrow(&arrow_type)
}

/// Convert a Postgres `PgTableSchema` to a Delta `Schema`
pub(crate) fn postgres_to_delta_schema(schema: &PgTableSchema) -> DeltaResult<DeltaSchema> {
    let fields: Vec<DeltaStructField> = schema
        .column_schemas
        .iter()
        .map(|col| {
            let arrow_type = postgres_type_to_arrow_type(&col.typ, col.modifier);
            let delta_data_type = DeltaDataType::try_from_arrow(&arrow_type)
                .map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?;
            Ok(DeltaStructField::new(
                &col.name,
                delta_data_type,
                col.nullable,
            ))
        })
        .collect::<Result<Vec<_>, deltalake::DeltaTableError>>()?;

    let delta_schema = DeltaSchema::try_new(fields)?;
    Ok(delta_schema)
}
