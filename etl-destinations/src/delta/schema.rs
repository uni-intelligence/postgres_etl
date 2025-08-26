use deltalake::kernel::{ArrayType, DataType, StructField};
use deltalake::{DeltaResult, Schema};
use etl::types::{TableSchema, Type};
use etl_postgres::types::is_array_type;

/// Convert a Postgres scalar type to an equivalent Delta DataType
fn postgres_scalar_type_to_delta(typ: &Type) -> DataType {
    match typ {
        &Type::BOOL => DataType::BOOLEAN,
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
            DataType::STRING
        }
        &Type::INT2 => DataType::SHORT,
        &Type::INT4 => DataType::INTEGER,
        &Type::INT8 => DataType::LONG,
        &Type::FLOAT4 => DataType::FLOAT,
        &Type::FLOAT8 => DataType::DOUBLE,
        // Without precision/scale information, map NUMERIC to STRING for now
        &Type::NUMERIC => DataType::STRING,
        &Type::DATE => DataType::DATE,
        // Delta Lake doesn't have a separate TIME type, use TIMESTAMP_NTZ
        &Type::TIME => DataType::TIMESTAMP_NTZ,
        &Type::TIMESTAMP => DataType::TIMESTAMP_NTZ,
        &Type::TIMESTAMPTZ => DataType::TIMESTAMP,
        // Delta Lake has no native UUID type; represent as string
        &Type::UUID => DataType::STRING,
        // Represent JSON as string
        &Type::JSON | &Type::JSONB => DataType::STRING,
        // OID is 32-bit unsigned in Postgres, map to INTEGER
        &Type::OID => DataType::INTEGER,
        &Type::BYTEA => DataType::BINARY,
        // Default fallback for unsupported types
        _ => DataType::STRING,
    }
}

/// Convert a Postgres array type to a Delta Array type
fn postgres_array_type_to_delta(typ: &Type) -> DataType {
    let element_type = match typ {
        &Type::BOOL_ARRAY => DataType::BOOLEAN,
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => DataType::STRING,
        &Type::INT2_ARRAY => DataType::SHORT,
        &Type::INT4_ARRAY => DataType::INTEGER,
        &Type::INT8_ARRAY => DataType::LONG,
        &Type::FLOAT4_ARRAY => DataType::FLOAT,
        &Type::FLOAT8_ARRAY => DataType::DOUBLE,
        // Map NUMERIC arrays to string arrays until precision/scale available
        &Type::NUMERIC_ARRAY => DataType::STRING,
        &Type::DATE_ARRAY => DataType::DATE,
        &Type::TIME_ARRAY => DataType::TIMESTAMP_NTZ,
        &Type::TIMESTAMP_ARRAY => DataType::TIMESTAMP_NTZ,
        &Type::TIMESTAMPTZ_ARRAY => DataType::TIMESTAMP,
        &Type::UUID_ARRAY => DataType::STRING,
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => DataType::STRING,
        &Type::OID_ARRAY => DataType::INTEGER,
        &Type::BYTEA_ARRAY => DataType::BINARY,
        _ => DataType::STRING,
    };

    ArrayType::new(element_type, true).into()
}

/// Convert a Postgres `TableSchema` to a Delta `Schema`
pub fn postgres_to_delta_schema(schema: &TableSchema) -> DeltaResult<Schema> {
    let fields: Vec<StructField> = schema
        .column_schemas
        .iter()
        .map(|col| {
            let data_type = if is_array_type(&col.typ) {
                postgres_array_type_to_delta(&col.typ)
            } else {
                postgres_scalar_type_to_delta(&col.typ)
            };
            StructField::new(&col.name, data_type, col.nullable)
        })
        .collect();

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_mappings() {
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::BOOL),
            DataType::BOOLEAN
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::TEXT),
            DataType::STRING
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::INT2),
            DataType::SHORT
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::INT4),
            DataType::INTEGER
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::INT8),
            DataType::LONG
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::FLOAT4),
            DataType::FLOAT
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::FLOAT8),
            DataType::DOUBLE
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::DATE),
            DataType::DATE
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::BYTEA),
            DataType::BINARY
        ));
    }

    #[test]
    fn test_array_mappings() {
        let dt = postgres_array_type_to_delta(&Type::INT4_ARRAY);
        if let DataType::Array(array_type) = dt {
            assert!(matches!(array_type.element_type(), &DataType::INTEGER));
            assert!(array_type.contains_null());
        } else {
            panic!("Expected Array type, got: {:?}", dt);
        }
    }

    #[test]
    fn test_timestamp_mappings() {
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::TIMESTAMP),
            DataType::TIMESTAMP_NTZ
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::TIMESTAMPTZ),
            DataType::TIMESTAMP
        ));
    }

    #[test]
    fn test_string_mappings() {
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::UUID),
            DataType::STRING
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::JSON),
            DataType::STRING
        ));
        assert!(matches!(
            postgres_scalar_type_to_delta(&Type::JSONB),
            DataType::STRING
        ));
    }
}
