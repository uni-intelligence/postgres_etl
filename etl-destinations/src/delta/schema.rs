use std::sync::Arc;

use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use etl::types::{TableSchema, Type};
use etl_postgres::types::is_array_type;

/// Convert a Postgres scalar type to an equivalent Arrow DataType
fn postgres_scalar_type_to_arrow(typ: &Type) -> DataType {
    match typ {
        &Type::BOOL => DataType::Boolean,
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
            DataType::Utf8
        }
        &Type::INT2 => DataType::Int16,
        &Type::INT4 => DataType::Int32,
        &Type::INT8 => DataType::Int64,
        &Type::FLOAT4 => DataType::Float32,
        &Type::FLOAT8 => DataType::Float64,
        // Without precision/scale information, map NUMERIC to Utf8 for now
        &Type::NUMERIC => DataType::Utf8,
        &Type::DATE => DataType::Date32,
        &Type::TIME => DataType::Time64(TimeUnit::Microsecond),
        &Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        &Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // Arrow has no native UUID type; represent as string
        &Type::UUID => DataType::Utf8,
        // Represent JSON as string
        &Type::JSON | &Type::JSONB => DataType::Utf8,
        // OID is 32-bit unsigned in Postgres
        &Type::OID => DataType::UInt32,
        &Type::BYTEA => DataType::Binary,
        _ => DataType::Utf8,
    }
}

/// Convert a Postgres array type to an Arrow List type
fn postgres_array_type_to_arrow(typ: &Type) -> DataType {
    let element_type = match typ {
        &Type::BOOL_ARRAY => DataType::Boolean,
        &Type::CHAR_ARRAY | &Type::BPCHAR_ARRAY | &Type::VARCHAR_ARRAY | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => DataType::Utf8,
        &Type::INT2_ARRAY => DataType::Int16,
        &Type::INT4_ARRAY => DataType::Int32,
        &Type::INT8_ARRAY => DataType::Int64,
        &Type::FLOAT4_ARRAY => DataType::Float32,
        &Type::FLOAT8_ARRAY => DataType::Float64,
        // Map NUMERIC arrays to string arrays until precision/scale available
        &Type::NUMERIC_ARRAY => DataType::Utf8,
        &Type::DATE_ARRAY => DataType::Date32,
        &Type::TIME_ARRAY => DataType::Time64(TimeUnit::Microsecond),
        &Type::TIMESTAMP_ARRAY => DataType::Timestamp(TimeUnit::Microsecond, None),
        &Type::TIMESTAMPTZ_ARRAY => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        &Type::UUID_ARRAY => DataType::Utf8,
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => DataType::Utf8,
        &Type::OID_ARRAY => DataType::UInt32,
        &Type::BYTEA_ARRAY => DataType::Binary,
        _ => DataType::Utf8,
    };

    DataType::List(Arc::new(Field::new("item", element_type, true)))
}

/// Convert a Postgres `TableSchema` to an Arrow `Schema`
pub fn postgres_to_arrow_schema(schema: &TableSchema) -> Arc<Schema> {
    let fields: Vec<Field> = schema
        .column_schemas
        .iter()
        .map(|col| {
            let data_type = if is_array_type(&col.typ) {
                postgres_array_type_to_arrow(&col.typ)
            } else {
                postgres_scalar_type_to_arrow(&col.typ)
            };
            Field::new(&col.name, data_type, col.nullable)
        })
        .collect();

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_mappings() {
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::BOOL), DataType::Boolean));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::TEXT), DataType::Utf8));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::INT2), DataType::Int16));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::INT4), DataType::Int32));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::INT8), DataType::Int64));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::FLOAT4), DataType::Float32));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::FLOAT8), DataType::Float64));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::DATE), DataType::Date32));
        assert!(matches!(postgres_scalar_type_to_arrow(&Type::BYTEA), DataType::Binary));
    }

    #[test]
    fn test_array_mappings() {
        let dt = postgres_array_type_to_arrow(&Type::INT4_ARRAY);
        if let DataType::List(inner) = dt { assert_eq!(inner.name(), "item"); } else { panic!(); }
    }
}


