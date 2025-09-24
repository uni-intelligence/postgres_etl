use deltalake::kernel::{DataType as DeltaDataType, StructField as DeltaStructField};
use deltalake::{ArrayType, DecimalType, DeltaResult, Schema as DeltaSchema};

use deltalake::PrimitiveType;
use etl::types::{TableSchema as PgTableSchema, Type as PgType, is_array_type};

use crate::arrow::{extract_numeric_precision, extract_numeric_scale};

/// Map Postgres types to appropriate Arrow types
pub fn postgres_array_type_to_delta_type(typ: &PgType) -> DeltaDataType {
    match typ {
        &PgType::BOOL_ARRAY => create_delta_list_type(PrimitiveType::Boolean),
        &PgType::CHAR_ARRAY
        | &PgType::BPCHAR_ARRAY
        | &PgType::VARCHAR_ARRAY
        | &PgType::NAME_ARRAY
        | &PgType::TEXT_ARRAY => create_delta_list_type(PrimitiveType::String),
        &PgType::INT2_ARRAY | &PgType::INT4_ARRAY => create_delta_list_type(PrimitiveType::Integer),
        &PgType::INT8_ARRAY => create_delta_list_type(PrimitiveType::Long),
        &PgType::FLOAT4_ARRAY => create_delta_list_type(PrimitiveType::Float),
        &PgType::FLOAT8_ARRAY => create_delta_list_type(PrimitiveType::Double),
        &PgType::NUMERIC_ARRAY => create_delta_list_type(PrimitiveType::String),
        &PgType::DATE_ARRAY => create_delta_list_type(PrimitiveType::Date),
        &PgType::TIME_ARRAY => create_delta_list_type(PrimitiveType::Timestamp),
        &PgType::TIMESTAMP_ARRAY => create_delta_list_type(PrimitiveType::Timestamp),
        &PgType::TIMESTAMPTZ_ARRAY => create_delta_list_type(PrimitiveType::TimestampNtz),
        &PgType::UUID_ARRAY => create_delta_list_type(PrimitiveType::String),
        &PgType::JSON_ARRAY | &PgType::JSONB_ARRAY => create_delta_list_type(PrimitiveType::String),
        &PgType::OID_ARRAY => create_delta_list_type(PrimitiveType::Long),
        &PgType::BYTEA_ARRAY => create_delta_list_type(PrimitiveType::Binary),
        _ => create_delta_list_type(PrimitiveType::String),
    }
}

/// Converts a Postgres scalar type to equivalent Delta type
pub fn postgres_scalar_type_to_delta_type(typ: &PgType, modifier: i32) -> DeltaDataType {
    match typ {
        &PgType::BOOL => DeltaDataType::Primitive(PrimitiveType::Boolean),
        &PgType::CHAR | &PgType::BPCHAR | &PgType::VARCHAR | &PgType::NAME | &PgType::TEXT => {
            DeltaDataType::Primitive(PrimitiveType::String)
        }
        &PgType::INT2 | &PgType::INT4 => DeltaDataType::Primitive(PrimitiveType::Integer),
        &PgType::INT8 => DeltaDataType::Primitive(PrimitiveType::Long),
        &PgType::FLOAT4 => DeltaDataType::Primitive(PrimitiveType::Float),
        &PgType::FLOAT8 => DeltaDataType::Primitive(PrimitiveType::Double),
        &PgType::NUMERIC => {
            let precision = extract_numeric_precision(modifier);
            let scale = extract_numeric_scale(modifier);
            let decimal_type = DecimalType::try_new(precision, scale)
                .map(|e| PrimitiveType::Decimal(e))
                .unwrap_or(PrimitiveType::String);
            DeltaDataType::Primitive(decimal_type)
        }
        &PgType::DATE => DeltaDataType::Primitive(PrimitiveType::Date),
        &PgType::TIME => DeltaDataType::Primitive(PrimitiveType::Timestamp),
        &PgType::TIMESTAMP => DeltaDataType::Primitive(PrimitiveType::Timestamp),
        &PgType::TIMESTAMPTZ => DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
        &PgType::UUID => DeltaDataType::Primitive(PrimitiveType::String),
        &PgType::JSON | &PgType::JSONB => DeltaDataType::Primitive(PrimitiveType::String),
        &PgType::OID => DeltaDataType::Primitive(PrimitiveType::Long),
        &PgType::BYTEA => DeltaDataType::Primitive(PrimitiveType::Binary),
        _ => DeltaDataType::Primitive(PrimitiveType::String),
    }
}

fn create_delta_list_type(element_type: PrimitiveType) -> DeltaDataType {
    let array_type = Box::new(ArrayType::new(element_type.into(), true));

    DeltaDataType::Array(array_type)
}

/// Convert a Postgres `PgTableSchema` to a Delta `Schema`
pub fn postgres_to_delta_schema(schema: &PgTableSchema) -> DeltaResult<DeltaSchema> {
    let fields: Vec<DeltaStructField> = schema
        .column_schemas
        .iter()
        .map(|col| {
            let field_type = if is_array_type(&col.typ) {
                postgres_array_type_to_delta_type(&col.typ)
            } else {
                postgres_scalar_type_to_delta_type(&col.typ, col.modifier)
            };
            Ok(DeltaStructField::new(&col.name, field_type, col.nullable))
        })
        .collect::<Result<Vec<_>, deltalake::DeltaTableError>>()?;

    let delta_schema = DeltaSchema::try_new(fields)?;
    Ok(delta_schema)
}
