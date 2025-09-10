use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::{DataType as DeltaDataType, StructField as DeltaStructField};
use deltalake::{DeltaResult, Schema as DeltaSchema};

use deltalake::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt32Array,
};
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::record_batch::RecordBatch;
use etl::types::{
    ArrayCell as PGArrayCell, Cell as PGCell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
    TIMESTAMPTZ_FORMAT_HH_MM, TableRow as PGTableRow, TableSchema as PGTableSchema, Type as PGType,
};
use std::sync::Arc;

/// Extract numeric precision from Postgres atttypmod
/// Based on: https://stackoverflow.com/questions/72725508/how-to-calculate-numeric-precision-and-other-vals-from-atttypmod
fn extract_numeric_precision(atttypmod: i32) -> u8 {
    if atttypmod == -1 {
        // No limit specified, use maximum precision
        38
    } else {
        let precision = ((atttypmod - 4) >> 16) & 65535;
        std::cmp::min(precision as u8, 38) // Cap at Arrow's max precision
    }
}

/// Extract numeric scale from Postgres atttypmod
/// Based on: https://stackoverflow.com/questions/72725508/how-to-calculate-numeric-precision-and-other-vals-from-atttypmod
fn extract_numeric_scale(atttypmod: i32) -> i8 {
    if atttypmod == -1 {
        // No limit specified, use reasonable default scale
        18
    } else {
        let scale = (atttypmod - 4) & 65535;
        std::cmp::min(scale as i8, 38) // Cap at reasonable scale
    }
}

/// Converts TableRows to Arrow RecordBatch for Delta Lake writes
pub struct TableRowEncoder;

impl TableRowEncoder {
    /// Convert a batch of TableRows to Arrow RecordBatch
    pub fn encode_table_rows(
        table_schema: &PGTableSchema,
        table_rows: Vec<PGTableRow>,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        if table_rows.is_empty() {
            return Ok(vec![]);
        }

        let record_batch = Self::table_rows_to_record_batch(table_schema, table_rows)?;
        Ok(vec![record_batch])
    }

    /// Convert TableRows to a single RecordBatch with schema-driven type conversion
    fn table_rows_to_record_batch(
        table_schema: &PGTableSchema,
        table_rows: Vec<PGTableRow>,
    ) -> Result<RecordBatch, ArrowError> {
        let arrow_schema = Self::postgres_schema_to_arrow_schema(table_schema)?;

        let arrays =
            Self::convert_columns_to_arrays_with_schema(table_schema, &table_rows, &arrow_schema)?;

        RecordBatch::try_new(Arc::new(arrow_schema), arrays)
    }

    /// Convert Postgres PGTableSchema to Arrow Schema with proper type mapping
    fn postgres_schema_to_arrow_schema(
        table_schema: &PGTableSchema,
    ) -> Result<ArrowSchema, ArrowError> {
        let fields: Vec<ArrowField> = table_schema
            .column_schemas
            .iter()
            .map(|col_schema| {
                let data_type =
                    Self::postgres_type_to_arrow_type(&col_schema.typ, col_schema.modifier);
                ArrowField::new(&col_schema.name, data_type, col_schema.nullable)
            })
            .collect();

        Ok(ArrowSchema::new(fields))
    }

    /// Map Postgres types to appropriate Arrow types
    pub(crate) fn postgres_type_to_arrow_type(pg_type: &PGType, modifier: i32) -> ArrowDataType {
        match *pg_type {
            // Boolean types
            PGType::BOOL => ArrowDataType::Boolean,
            PGType::BOOL_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Boolean,
                true,
            ))),

            // String types
            PGType::CHAR
            | PGType::BPCHAR
            | PGType::VARCHAR
            | PGType::NAME
            | PGType::TEXT
            | PGType::UUID
            | PGType::JSON
            | PGType::JSONB => ArrowDataType::Utf8,
            PGType::CHAR_ARRAY
            | PGType::BPCHAR_ARRAY
            | PGType::VARCHAR_ARRAY
            | PGType::NAME_ARRAY
            | PGType::TEXT_ARRAY
            | PGType::UUID_ARRAY
            | PGType::JSON_ARRAY
            | PGType::JSONB_ARRAY => {
                ArrowDataType::List(Arc::new(ArrowField::new("item", ArrowDataType::Utf8, true)))
            }

            // Integer types
            PGType::INT2 => ArrowDataType::Int16,
            PGType::INT2_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Int16,
                true,
            ))),
            PGType::INT4 | PGType::OID => ArrowDataType::Int32,
            PGType::INT4_ARRAY | PGType::OID_ARRAY => ArrowDataType::List(Arc::new(
                ArrowField::new("item", ArrowDataType::Int32, true),
            )),
            PGType::INT8 => ArrowDataType::Int64,
            PGType::INT8_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Int64,
                true,
            ))),

            // Unsigned integer types
            // Note: Postgres doesn't have native unsigned types, but we support U32 in PGCell
            // Map to closest signed type for now

            // Float types
            PGType::FLOAT4 => ArrowDataType::Float32,
            PGType::FLOAT4_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Float32,
                true,
            ))),
            PGType::FLOAT8 => ArrowDataType::Float64,
            PGType::FLOAT8_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Float64,
                true,
            ))),
            PGType::NUMERIC => {
                let precision = extract_numeric_precision(modifier);
                let scale = extract_numeric_scale(modifier);
                ArrowDataType::Decimal128(precision, scale)
            }
            PGType::NUMERIC_ARRAY => {
                let precision = extract_numeric_precision(modifier);
                let scale = extract_numeric_scale(modifier);
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Decimal128(precision, scale),
                    true,
                )))
            }
            // Date/Time types
            PGType::DATE => ArrowDataType::Date32,
            PGType::DATE_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Date32,
                true,
            ))),
            // Note: Delta Lake doesn't support standalone TIME, so we map to TIMESTAMP_NTZ
            PGType::TIME => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            PGType::TIME_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
            PGType::TIMESTAMP => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            PGType::TIMESTAMP_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
            PGType::TIMESTAMPTZ => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PGType::TIMESTAMPTZ_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))),

            // Binary types
            PGType::BYTEA => ArrowDataType::Binary,
            PGType::BYTEA_ARRAY => ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Binary,
                true,
            ))),

            // Default fallback for unsupported types
            _ => ArrowDataType::Utf8,
        }
    }

    /// Convert table columns to Arrow arrays using schema-driven conversion
    fn convert_columns_to_arrays_with_schema(
        table_schema: &PGTableSchema,
        table_rows: &[PGTableRow],
        arrow_schema: &ArrowSchema,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        let mut arrays = Vec::new();

        for (col_idx, _col_schema) in table_schema.column_schemas.iter().enumerate() {
            let column_data: Vec<&PGCell> =
                table_rows.iter().map(|row| &row.values[col_idx]).collect();

            let expected_type = &arrow_schema.field(col_idx).data_type();
            let array = Self::convert_cell_column_to_arrow_array(column_data, expected_type)?;
            arrays.push(array);
        }

        Ok(arrays)
    }

    /// Convert a column of Cells to an Arrow array with proper type mapping
    fn convert_cell_column_to_arrow_array(
        cells: Vec<&PGCell>,
        expected_type: &ArrowDataType,
    ) -> Result<ArrayRef, ArrowError> {
        if cells.is_empty() {
            return Self::create_empty_array(expected_type);
        }

        match expected_type {
            ArrowDataType::Boolean => Self::convert_to_boolean_array(cells),
            ArrowDataType::Int16 => Self::convert_to_int16_array(cells),
            ArrowDataType::Int32 => Self::convert_to_int32_array(cells),
            ArrowDataType::Int64 => Self::convert_to_int64_array(cells),
            ArrowDataType::UInt32 => Self::convert_to_uint32_array(cells),
            ArrowDataType::Float32 => Self::convert_to_float32_array(cells),
            ArrowDataType::Float64 => Self::convert_to_float64_array(cells),
            ArrowDataType::Utf8 => Self::convert_to_string_array(cells),
            ArrowDataType::Binary => Self::convert_to_binary_array(cells),
            ArrowDataType::Date32 => Self::convert_to_date32_array(cells),
            ArrowDataType::Time64(TimeUnit::Nanosecond) => Self::convert_to_time64_array(cells),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                if !cells.is_empty() && matches!(cells[0], PGCell::Time(_)) {
                    Self::convert_time_to_timestamp_array(cells)
                } else {
                    Self::convert_to_timestamp_array(cells)
                }
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                Self::convert_to_timestamptz_array(cells)
            }
            ArrowDataType::Decimal128(precision, scale) => {
                Self::convert_to_decimal128_array(cells, *precision, *scale)
            }
            ArrowDataType::List(field) => Self::convert_to_list_array(cells, field.data_type()),
            _ => {
                // Fallback to string representation for unsupported types
                Self::convert_to_string_array(cells)
            }
        }
    }

    /// Create an empty array of the specified type
    fn create_empty_array(data_type: &ArrowDataType) -> Result<ArrayRef, ArrowError> {
        match data_type {
            ArrowDataType::Boolean => Ok(Arc::new(BooleanArray::from(Vec::<Option<bool>>::new()))),
            ArrowDataType::Int16 => Ok(Arc::new(Int16Array::from(Vec::<Option<i16>>::new()))),
            ArrowDataType::Int32 => Ok(Arc::new(Int32Array::from(Vec::<Option<i32>>::new()))),
            ArrowDataType::Int64 => Ok(Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))),
            ArrowDataType::UInt32 => Ok(Arc::new(UInt32Array::from(Vec::<Option<u32>>::new()))),
            ArrowDataType::Float32 => Ok(Arc::new(Float32Array::from(Vec::<Option<f32>>::new()))),
            ArrowDataType::Float64 => Ok(Arc::new(Float64Array::from(Vec::<Option<f64>>::new()))),
            ArrowDataType::Utf8 => Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new()))),
            ArrowDataType::Binary => Ok(Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new()))),
            ArrowDataType::Date32 => Ok(Arc::new(Date32Array::from(Vec::<Option<i32>>::new()))),
            ArrowDataType::Time64(_) => Ok(Arc::new(Time64NanosecondArray::from(
                Vec::<Option<i64>>::new(),
            ))),
            ArrowDataType::Timestamp(_, _) => Ok(Arc::new(TimestampMicrosecondArray::from(Vec::<
                Option<i64>,
            >::new(
            )))),
            ArrowDataType::Decimal128(_, _) => {
                Ok(Arc::new(Decimal128Array::from(Vec::<Option<i128>>::new())))
            }
            _ => Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new()))),
        }
    }

    /// Convert cells to boolean array
    fn convert_to_boolean_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<bool>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Bool(b) => Some(*b),
                // String to bool conversion
                PGCell::String(s) => match s.to_lowercase().as_str() {
                    "t" | "true" | "y" | "yes" | "1" => Some(true),
                    "f" | "false" | "n" | "no" | "0" => Some(false),
                    _ => None,
                },
                // Numeric to bool conversion
                PGCell::I16(i) => Some(*i != 0),
                PGCell::I32(i) => Some(*i != 0),
                PGCell::I64(i) => Some(*i != 0),
                PGCell::U32(i) => Some(*i != 0),
                _ => None,
            })
            .collect();
        Ok(Arc::new(BooleanArray::from(values)))
    }

    /// Convert cells to int16 array
    fn convert_to_int16_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<i16>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::I16(i) => Some(*i),
                PGCell::I32(i) => Some(*i as i16), // Potential overflow
                PGCell::Bool(b) => Some(if *b { 1 } else { 0 }),
                PGCell::String(s) => s.parse::<i16>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(Int16Array::from(values)))
    }

    /// Convert cells to int32 array
    fn convert_to_int32_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<i32>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::I16(i) => Some(*i as i32),
                PGCell::I32(i) => Some(*i),
                PGCell::I64(i) => Some(*i as i32), // Potential overflow
                PGCell::U32(i) => Some(*i as i32), // Potential overflow
                PGCell::Bool(b) => Some(if *b { 1 } else { 0 }),
                PGCell::String(s) => s.parse::<i32>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(Int32Array::from(values)))
    }

    /// Convert cells to int64 array
    fn convert_to_int64_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<i64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::I16(i) => Some(*i as i64),
                PGCell::I32(i) => Some(*i as i64),
                PGCell::I64(i) => Some(*i),
                PGCell::U32(i) => Some(*i as i64),
                PGCell::Bool(b) => Some(if *b { 1 } else { 0 }),
                PGCell::String(s) => s.parse::<i64>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(Int64Array::from(values)))
    }

    /// Convert cells to uint32 array
    fn convert_to_uint32_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<u32>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::U32(i) => Some(*i),
                PGCell::I16(i) => {
                    if *i >= 0 {
                        Some(*i as u32)
                    } else {
                        None
                    }
                }
                PGCell::I32(i) => {
                    if *i >= 0 {
                        Some(*i as u32)
                    } else {
                        None
                    }
                }
                PGCell::I64(i) => {
                    if *i >= 0 && *i <= u32::MAX as i64 {
                        Some(*i as u32)
                    } else {
                        None
                    }
                }
                PGCell::Bool(b) => Some(if *b { 1 } else { 0 }),
                PGCell::String(s) => s.parse::<u32>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(UInt32Array::from(values)))
    }

    /// Convert cells to float32 array
    fn convert_to_float32_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<f32>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::F32(f) => Some(*f),
                PGCell::F64(f) => Some(*f as f32), // Potential precision loss
                PGCell::I16(i) => Some(*i as f32),
                PGCell::I32(i) => Some(*i as f32),
                PGCell::I64(i) => Some(*i as f32),
                PGCell::U32(i) => Some(*i as f32),
                PGCell::Numeric(n) => n.to_string().parse::<f32>().ok(),
                PGCell::String(s) => s.parse::<f32>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(Float32Array::from(values)))
    }

    /// Convert cells to float64 array
    fn convert_to_float64_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<f64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::F32(f) => Some(*f as f64),
                PGCell::F64(f) => Some(*f),
                PGCell::I16(i) => Some(*i as f64),
                PGCell::I32(i) => Some(*i as f64),
                PGCell::I64(i) => Some(*i as f64),
                PGCell::U32(i) => Some(*i as f64),
                PGCell::Numeric(n) => n.to_string().parse::<f64>().ok(),
                PGCell::String(s) => s.parse::<f64>().ok(),
                _ => None,
            })
            .collect();
        Ok(Arc::new(Float64Array::from(values)))
    }

    /// Convert cells to string array  
    fn convert_to_string_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<String>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Bool(b) => Some(b.to_string()),
                PGCell::String(s) => Some(s.clone()),
                PGCell::I16(i) => Some(i.to_string()),
                PGCell::I32(i) => Some(i.to_string()),
                PGCell::U32(i) => Some(i.to_string()),
                PGCell::I64(i) => Some(i.to_string()),
                PGCell::F32(f) => Some(f.to_string()),
                PGCell::F64(f) => Some(f.to_string()),
                PGCell::Numeric(n) => Some(n.to_string()),
                PGCell::Date(d) => Some(d.format(DATE_FORMAT).to_string()),
                PGCell::Time(t) => Some(t.format(TIME_FORMAT).to_string()),
                PGCell::Timestamp(ts) => Some(ts.format(TIMESTAMP_FORMAT).to_string()),
                PGCell::TimestampTz(ts) => Some(ts.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string()),
                PGCell::Uuid(u) => Some(u.to_string()),
                PGCell::Json(j) => Some(j.to_string()),
                PGCell::Bytes(b) => Some(format!("\\x{b:02x?}")),
                PGCell::Array(_) => Some("[ARRAY]".to_string()),
            })
            .collect();
        Ok(Arc::new(StringArray::from(values)))
    }

    /// Convert cells to binary array
    fn convert_to_binary_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<&[u8]>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Bytes(b) => Some(b.as_slice()),
                PGCell::String(s) => Some(s.as_bytes()),
                PGCell::Uuid(u) => Some(u.as_bytes().as_slice()),
                _ => None,
            })
            .collect();
        Ok(Arc::new(BinaryArray::from(values)))
    }

    /// Convert cells to date32 array (days since Unix epoch)
    fn convert_to_date32_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        use chrono::NaiveDate;

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let values: Vec<Option<i32>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Date(d) => {
                    let days = d.signed_duration_since(epoch).num_days();
                    Some(days as i32)
                }
                PGCell::Timestamp(ts) => {
                    let days = ts.date().signed_duration_since(epoch).num_days();
                    Some(days as i32)
                }
                PGCell::TimestampTz(ts) => {
                    let days = ts
                        .naive_utc()
                        .date()
                        .signed_duration_since(epoch)
                        .num_days();
                    Some(days as i32)
                }
                PGCell::String(s) => {
                    if let Ok(parsed_date) = chrono::NaiveDate::parse_from_str(s, DATE_FORMAT) {
                        let days = parsed_date.signed_duration_since(epoch).num_days();
                        Some(days as i32)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(Date32Array::from(values)))
    }

    /// Convert cells to time64 array (nanoseconds since midnight)
    fn convert_to_time64_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        use chrono::Timelike;

        let values: Vec<Option<i64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Time(t) => {
                    // Convert time to nanoseconds since midnight
                    let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                        + t.nanosecond() as i64;
                    Some(nanos)
                }
                PGCell::String(s) => {
                    if let Ok(parsed_time) = chrono::NaiveTime::parse_from_str(s, TIME_FORMAT) {
                        let nanos = parsed_time.num_seconds_from_midnight() as i64 * 1_000_000_000
                            + parsed_time.nanosecond() as i64;
                        Some(nanos)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(Time64NanosecondArray::from(values)))
    }

    /// Convert time cells to timestamp array (treating time as timestamp at epoch date)
    fn convert_time_to_timestamp_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        use chrono::NaiveDate;

        // Use epoch date (1970-01-01) as the base date for time values
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        let values: Vec<Option<i64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Time(t) => {
                    // Convert time to a timestamp at epoch date
                    let ts = epoch_date.and_time(*t);
                    Some(ts.and_utc().timestamp_micros())
                }
                PGCell::String(s) => {
                    if let Ok(parsed_time) = chrono::NaiveTime::parse_from_str(s, TIME_FORMAT) {
                        let ts = epoch_date.and_time(parsed_time);
                        Some(ts.and_utc().timestamp_micros())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(TimestampMicrosecondArray::from(values)))
    }

    /// Convert cells to timestamp array (microseconds since Unix epoch)
    fn convert_to_timestamp_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        use chrono::NaiveDateTime;

        let values: Vec<Option<i64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros()),
                PGCell::TimestampTz(ts) => Some(ts.naive_utc().and_utc().timestamp_micros()),
                PGCell::Date(d) => {
                    // Convert date to midnight timestamp
                    let ts = d.and_hms_opt(0, 0, 0).unwrap();
                    Some(ts.and_utc().timestamp_micros())
                }
                PGCell::String(s) => {
                    if let Ok(parsed_ts) = NaiveDateTime::parse_from_str(s, TIMESTAMP_FORMAT) {
                        Some(parsed_ts.and_utc().timestamp_micros())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(TimestampMicrosecondArray::from(values)))
    }

    /// Convert cells to timestamptz array (microseconds since Unix epoch with timezone)
    fn convert_to_timestamptz_array(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        use chrono::{DateTime, Utc};

        let values: Vec<Option<i64>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::TimestampTz(ts) => Some(ts.timestamp_micros()),
                PGCell::Timestamp(ts) => {
                    // Assume local timestamp is UTC for conversion
                    let utc_ts = DateTime::<Utc>::from_naive_utc_and_offset(*ts, Utc);
                    Some(utc_ts.timestamp_micros())
                }
                PGCell::String(_s) => {
                    // Simplified string parsing - convert to string representation
                    None // Skip complex parsing for now
                }
                _ => None,
            })
            .collect();
        // Create timezone-aware timestamp array
        let timestamp_type = ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        Ok(Arc::new(
            TimestampMicrosecondArray::from(values).with_data_type(timestamp_type),
        ))
    }

    /// Convert cells to decimal128 array
    fn convert_to_decimal128_array(
        cells: Vec<&PGCell>,
        precision: u8,
        scale: i8,
    ) -> Result<ArrayRef, ArrowError> {
        let values: Vec<Option<i128>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Numeric(n) => {
                    // This is a simplified conversion - ideally we'd preserve the exact decimal representation
                    if let Ok(string_val) = n.to_string().parse::<f64>() {
                        // Scale up by the scale factor and convert to i128
                        let scaled = (string_val * 10_f64.powi(scale as i32)) as i128;
                        Some(scaled)
                    } else {
                        None
                    }
                }
                PGCell::I16(i) => Some(*i as i128 * 10_i128.pow(scale as u32)),
                PGCell::I32(i) => Some(*i as i128 * 10_i128.pow(scale as u32)),
                PGCell::I64(i) => Some(*i as i128 * 10_i128.pow(scale as u32)),
                PGCell::U32(i) => Some(*i as i128 * 10_i128.pow(scale as u32)),
                PGCell::F32(f) => {
                    let scaled = (*f as f64 * 10_f64.powi(scale as i32)) as i128;
                    Some(scaled)
                }
                PGCell::F64(f) => {
                    let scaled = (f * 10_f64.powi(scale as i32)) as i128;
                    Some(scaled)
                }
                PGCell::String(s) => {
                    if let Ok(val) = s.parse::<f64>() {
                        let scaled = (val * 10_f64.powi(scale as i32)) as i128;
                        Some(scaled)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        let decimal_type = ArrowDataType::Decimal128(precision, scale);
        Ok(Arc::new(
            Decimal128Array::from(values).with_data_type(decimal_type),
        ))
    }

    /// Convert cells to list array for array types
    fn convert_to_list_array(
        cells: Vec<&PGCell>,
        _element_type: &ArrowDataType,
    ) -> Result<ArrayRef, ArrowError> {
        // Simplified implementation: convert all arrays to string lists
        Self::convert_array_to_string_list(cells)
    }

    /// Fallback method to convert any array to string list
    fn convert_array_to_string_list(cells: Vec<&PGCell>) -> Result<ArrayRef, ArrowError> {
        // Simplified implementation: convert all arrays to single string representation
        let values: Vec<Option<String>> = cells
            .iter()
            .map(|cell| match cell {
                PGCell::Null => None,
                PGCell::Array(array_cell) => match array_cell {
                    PGArrayCell::Null => None,
                    PGArrayCell::Bool(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::String(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::I16(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::I32(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::U32(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::I64(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::F32(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::F64(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Numeric(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Date(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Time(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Timestamp(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::TimestampTz(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Uuid(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Json(arr) => Some(format!("{arr:?}")),
                    PGArrayCell::Bytes(arr) => Some(format!("{arr:02x?}")),
                },
                _ => None, // Not an array
            })
            .collect();

        Ok(Arc::new(StringArray::from(values)))
    }
}

/// Convert a Postgres type to Delta DataType using delta-kernel's conversion traits
#[allow(dead_code)]
pub(crate) fn postgres_type_to_delta(typ: &PGType) -> Result<DeltaDataType, ArrowError> {
    let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(typ, -1);
    DeltaDataType::try_from_arrow(&arrow_type)
}

/// Convert a Postgres `PGTableSchema` to a Delta `Schema`
pub(crate) fn postgres_to_delta_schema(schema: &PGTableSchema) -> DeltaResult<DeltaSchema> {
    let fields: Vec<DeltaStructField> = schema
        .column_schemas
        .iter()
        .map(|col| {
            let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(&col.typ, col.modifier);
            let delta_data_type = DeltaDataType::try_from_arrow(&arrow_type)
                .map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?;
            Ok(DeltaStructField::new(
                &col.name,
                delta_data_type,
                col.nullable,
            ))
        })
        .collect::<Result<Vec<_>, deltalake::DeltaTableError>>()?;

    Ok(DeltaSchema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_mappings() {
        // Test unified mappings using delta-kernel types
        assert!(matches!(
            postgres_type_to_delta(&PGType::BOOL).unwrap(),
            DeltaDataType::BOOLEAN
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::TEXT).unwrap(),
            DeltaDataType::STRING
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::INT2).unwrap(),
            DeltaDataType::SHORT
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::INT4).unwrap(),
            DeltaDataType::INTEGER
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::INT8).unwrap(),
            DeltaDataType::LONG
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::FLOAT4).unwrap(),
            DeltaDataType::FLOAT
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::FLOAT8).unwrap(),
            DeltaDataType::DOUBLE
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::DATE).unwrap(),
            DeltaDataType::DATE
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::BYTEA).unwrap(),
            DeltaDataType::BINARY
        ));
        // TODO(abhi): https://github.com/delta-io/delta-rs/issues/3729
        // assert!(matches!(
        //     postgres_type_to_delta(&PGType::NUMERIC).unwrap(),
        //     DeltaDataType::Primitive(PrimitiveType::Decimal(DecimalType { .. }))
        // ));
    }

    #[test]
    fn test_array_mappings() {
        // Test unified array mapping using delta-kernel types
        let dt = postgres_type_to_delta(&PGType::INT4_ARRAY).unwrap();
        if let DeltaDataType::Array(array_type) = dt {
            assert!(matches!(array_type.element_type(), &DeltaDataType::INTEGER));
            assert!(array_type.contains_null());
        } else {
            panic!("Expected Array type, got: {dt:?}");
        }

        let numeric_array_dt = postgres_type_to_delta(&PGType::NUMERIC_ARRAY).unwrap();
        if let DeltaDataType::Array(array_type) = numeric_array_dt {
            println!(
                "NUMERIC array element type: {:?}",
                array_type.element_type()
            );
            assert!(array_type.contains_null());
        } else {
            panic!("Expected Array type for NUMERIC_ARRAY, got: {numeric_array_dt:?}");
        }
    }

    #[test]
    fn test_timestamp_mappings() {
        // Test unified timestamp mappings using delta-kernel types
        assert!(matches!(
            postgres_type_to_delta(&PGType::TIMESTAMP).unwrap(),
            DeltaDataType::TIMESTAMP_NTZ
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::TIMESTAMPTZ).unwrap(),
            DeltaDataType::TIMESTAMP
        ));
        // TIME maps to TIMESTAMP_NTZ in delta-kernel
        assert!(matches!(
            postgres_type_to_delta(&PGType::TIME).unwrap(),
            DeltaDataType::TIMESTAMP_NTZ
        ));
    }

    #[test]
    fn test_string_mappings() {
        // Test unified string mappings using delta-kernel types
        assert!(matches!(
            postgres_type_to_delta(&PGType::UUID).unwrap(),
            DeltaDataType::STRING
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::JSON).unwrap(),
            DeltaDataType::STRING
        ));
        assert!(matches!(
            postgres_type_to_delta(&PGType::JSONB).unwrap(),
            DeltaDataType::STRING
        ));
    }

    #[test]
    fn test_conversion_roundtrip() {
        // Test that our conversion through delta-kernel works correctly
        let test_types = vec![
            PGType::BOOL,
            PGType::INT2,
            PGType::INT4,
            PGType::INT8,
            PGType::FLOAT4,
            PGType::FLOAT8,
            PGType::TEXT,
            PGType::NUMERIC,
            PGType::DATE,
            PGType::TIME,
            PGType::TIMESTAMP,
            PGType::TIMESTAMPTZ,
            PGType::UUID,
            PGType::JSON,
            PGType::BYTEA,
            PGType::BOOL_ARRAY,
            PGType::INT4_ARRAY,
            PGType::TEXT_ARRAY,
            PGType::NUMERIC_ARRAY,
        ];

        for pg_type in test_types {
            // Test that conversion succeeds
            let delta_type = postgres_type_to_delta(&pg_type);
            assert!(
                delta_type.is_ok(),
                "Failed to convert {:?}: {:?}",
                pg_type,
                delta_type.err()
            );

            // Test that we can convert back to Arrow
            let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(&pg_type, -1);
            let roundtrip_delta = DeltaDataType::try_from_arrow(&arrow_type);
            assert!(
                roundtrip_delta.is_ok(),
                "Failed roundtrip conversion for {:?}: {:?}",
                pg_type,
                roundtrip_delta.err()
            );
        }
    }

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use etl::types::{ColumnSchema, TableName, TableSchema as PGTableSchema, Type as PGType};
    use uuid::Uuid;

    #[test]
    fn test_empty_table_rows() {
        let schema = create_test_schema();
        let result = TableRowEncoder::encode_table_rows(&schema, vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_comprehensive_type_conversion() {
        let schema = create_comprehensive_test_schema();
        let rows = vec![create_comprehensive_test_row()];

        let result = TableRowEncoder::encode_table_rows(&schema, rows);
        assert!(result.is_ok());

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12); // All test columns
    }

    #[test]
    fn test_decimal_precision_scale_extraction() {
        // Test specific atttypmod values from the Stack Overflow example
        // https://stackoverflow.com/questions/72725508/how-to-calculate-numeric-precision-and-other-vals-from-atttypmod

        // NUMERIC(5,2) -> atttypmod = 327686
        assert_eq!(extract_numeric_precision(327686), 5);
        assert_eq!(extract_numeric_scale(327686), 2);

        // NUMERIC(5,1) -> atttypmod = 327685
        assert_eq!(extract_numeric_precision(327685), 5);
        assert_eq!(extract_numeric_scale(327685), 1);

        // NUMERIC(6,3) -> atttypmod = 393223
        assert_eq!(extract_numeric_precision(393223), 6);
        assert_eq!(extract_numeric_scale(393223), 3);

        // NUMERIC(4,4) -> atttypmod = 262152
        assert_eq!(extract_numeric_precision(262152), 4);
        assert_eq!(extract_numeric_scale(262152), 4);

        // Test -1 (no limit)
        assert_eq!(extract_numeric_precision(-1), 38); // Max precision
        assert_eq!(extract_numeric_scale(-1), 18); // Default scale

        let arrow_type = TableRowEncoder::postgres_type_to_arrow_type(&PGType::NUMERIC, 327686);
        if let ArrowDataType::Decimal128(precision, scale) = arrow_type {
            assert_eq!(precision, 5);
            assert_eq!(scale, 2);
        } else {
            panic!("Expected Decimal128 type, got: {arrow_type:?}");
        }
    }

    #[test]
    fn test_postgres_type_to_arrow_type_mapping() {
        // Test basic types
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::BOOL, -1),
            ArrowDataType::Boolean
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::INT4, -1),
            ArrowDataType::Int32
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::INT8, -1),
            ArrowDataType::Int64
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::FLOAT8, -1),
            ArrowDataType::Float64
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::TEXT, -1),
            ArrowDataType::Utf8
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::DATE, -1),
            ArrowDataType::Date32
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::TIME, -1),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::BYTEA, -1),
            ArrowDataType::Binary
        );

        // Test array types
        if let ArrowDataType::List(field) =
            TableRowEncoder::postgres_type_to_arrow_type(&PGType::INT4_ARRAY, -1)
        {
            assert_eq!(*field.data_type(), ArrowDataType::Int32);
        } else {
            panic!("Expected List type for INT4_ARRAY");
        }
    }

    #[test]
    fn test_boolean_conversion() {
        let true_str = PGCell::String("true".to_string());
        let false_str = PGCell::String("false".to_string());
        let int_1 = PGCell::I32(1);
        let int_0 = PGCell::I32(0);

        let cells = vec![
            &PGCell::Bool(true),
            &PGCell::Bool(false),
            &PGCell::Null,
            &true_str,
            &false_str,
            &int_1,
            &int_0,
        ];

        let result = TableRowEncoder::convert_to_boolean_array(cells);
        assert!(result.is_ok());

        let array = result.unwrap();
        assert_eq!(array.len(), 7);
    }

    #[test]
    fn test_string_conversion() {
        let hello_str = PGCell::String("hello".to_string());
        let int_val = PGCell::I32(42);
        let uuid_val = PGCell::Uuid(Uuid::new_v4());

        let cells = vec![
            &hello_str,
            &int_val,
            &PGCell::Bool(true),
            &PGCell::Null,
            &uuid_val,
        ];

        let result = TableRowEncoder::convert_to_string_array(cells);
        assert!(result.is_ok());

        let array = result.unwrap();
        assert_eq!(array.len(), 5);
    }

    #[test]
    fn test_temporal_conversion() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let timestamp = NaiveDateTime::new(date, time);
        let timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc);

        let date_cell = PGCell::Date(date);
        let time_cell = PGCell::Time(time);
        let timestamp_cell = PGCell::Timestamp(timestamp);
        let timestamptz_cell = PGCell::TimestampTz(timestamptz);

        let date_cells = vec![&date_cell, &PGCell::Null];
        let result = TableRowEncoder::convert_to_date32_array(date_cells);
        assert!(result.is_ok());

        let time_cells = vec![&time_cell, &PGCell::Null];
        let result = TableRowEncoder::convert_time_to_timestamp_array(time_cells);
        assert!(result.is_ok());

        let timestamp_cells = vec![&timestamp_cell, &PGCell::Null];
        let result = TableRowEncoder::convert_to_timestamp_array(timestamp_cells);
        assert!(result.is_ok());

        let timestamptz_cells = vec![&timestamptz_cell, &PGCell::Null];
        let result = TableRowEncoder::convert_to_timestamptz_array(timestamptz_cells);
        assert!(result.is_ok());
    }

    #[test]
    fn test_array_conversion() {
        let bool_array = PGCell::Array(PGArrayCell::Bool(vec![Some(true), Some(false), None]));
        let string_array =
            PGCell::Array(PGArrayCell::String(vec![Some("hello".to_string()), None]));
        let int_array = PGCell::Array(PGArrayCell::I32(vec![Some(1), Some(2), Some(3)]));

        let cells = vec![&bool_array, &string_array, &int_array, &PGCell::Null];

        let result = TableRowEncoder::convert_array_to_string_list(cells);
        assert!(result.is_ok());

        let array = result.unwrap();
        assert_eq!(array.len(), 4);
    }

    #[test]
    fn test_schema_generation() {
        let table_schema = create_comprehensive_test_schema();
        let result = TableRowEncoder::postgres_schema_to_arrow_schema(&table_schema);
        assert!(result.is_ok());

        let arrow_schema = result.unwrap();
        assert_eq!(
            arrow_schema.fields().len(),
            table_schema.column_schemas.len()
        );
    }

    fn create_test_schema() -> PGTableSchema {
        PGTableSchema {
            id: etl::types::TableId(1),
            name: TableName::new("public".to_string(), "test_table".to_string()),
            column_schemas: vec![ColumnSchema::new(
                "id".to_string(),
                PGType::INT4,
                -1,
                false,
                true,
            )],
        }
    }

    fn create_comprehensive_test_schema() -> PGTableSchema {
        PGTableSchema {
            id: etl::types::TableId(1),
            name: TableName::new("public".to_string(), "comprehensive_test".to_string()),
            column_schemas: vec![
                ColumnSchema::new("bool_col".to_string(), PGType::BOOL, -1, true, false),
                ColumnSchema::new("int2_col".to_string(), PGType::INT2, -1, true, false),
                ColumnSchema::new("int4_col".to_string(), PGType::INT4, -1, true, false),
                ColumnSchema::new("int8_col".to_string(), PGType::INT8, -1, true, false),
                ColumnSchema::new("float4_col".to_string(), PGType::FLOAT4, -1, true, false),
                ColumnSchema::new("float8_col".to_string(), PGType::FLOAT8, -1, true, false),
                ColumnSchema::new("text_col".to_string(), PGType::TEXT, -1, true, false),
                ColumnSchema::new("date_col".to_string(), PGType::DATE, -1, true, false),
                ColumnSchema::new("time_col".to_string(), PGType::TIME, -1, true, false),
                ColumnSchema::new(
                    "timestamp_col".to_string(),
                    PGType::TIMESTAMP,
                    -1,
                    true,
                    false,
                ),
                ColumnSchema::new(
                    "timestamptz_col".to_string(),
                    PGType::TIMESTAMPTZ,
                    -1,
                    true,
                    false,
                ),
                ColumnSchema::new("bytea_col".to_string(), PGType::BYTEA, -1, true, false),
            ],
        }
    }

    fn create_comprehensive_test_row() -> PGTableRow {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let timestamp = NaiveDateTime::new(date, time);
        let timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc);

        PGTableRow::new(vec![
            PGCell::Bool(true),
            PGCell::I16(12345),
            PGCell::I32(1234567),
            PGCell::I64(123456789012345),
            PGCell::F64(std::f64::consts::PI),
            PGCell::F64(std::f64::consts::E),
            PGCell::String("hello world".to_string()),
            PGCell::Date(date),
            PGCell::Time(time),
            PGCell::Timestamp(timestamp),
            PGCell::TimestampTz(timestamptz),
            PGCell::Bytes(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]),
        ])
    }
}
