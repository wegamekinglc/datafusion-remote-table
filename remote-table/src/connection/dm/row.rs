use crate::DFResult;
use crate::connection::projections_contains;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, StringBuilder, Time32MillisecondBuilder,
    Time32SecondBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use odbc_api::{CursorRow, decimal_text_to_i128};
use std::str::FromStr;

macro_rules! read_text {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let mut value = Vec::new();
        let is_not_null = $row.get_text($col_idx, &mut value).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        if is_not_null {
            let value = String::from_utf8(value).map_err(|e| {
                DataFusionError::Execution(format!("Failed to convert value to string: {e:?}"))
            })?;
            builder.append_value($convert(value)?);
        } else {
            builder.append_null();
        }
    }};
}

pub(crate) fn row_to_batch(
    mut row: CursorRow,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), 1);
        array_builders.push(builder);
    }

    for (idx, field) in table_schema.fields().iter().enumerate() {
        if !projections_contains(projection, idx) {
            continue;
        }
        let builder = &mut array_builders[idx];
        let odbc_col_idx = (idx + 1) as u16;
        match field.data_type() {
            DataType::Boolean => {
                read_text!(
                    builder,
                    field,
                    BooleanBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        if v == "1" {
                            Ok(true)
                        } else if v == "0" {
                            Ok(false)
                        } else {
                            Err(DataFusionError::Execution(format!(
                                "Failed to convert value to boolean: {v:?}"
                            )))
                        }
                    }
                );
            }
            DataType::Int8 => {
                read_text!(
                    builder,
                    field,
                    Int8Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Int16 => {
                read_text!(
                    builder,
                    field,
                    Int16Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Int32 => {
                read_text!(
                    builder,
                    field,
                    Int32Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Int64 => {
                read_text!(
                    builder,
                    field,
                    Int64Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Float32 => {
                read_text!(
                    builder,
                    field,
                    Float32Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Float64 => {
                read_text!(
                    builder,
                    field,
                    Float64Builder,
                    row,
                    odbc_col_idx,
                    parse_primitive
                );
            }
            DataType::Decimal128(_precision, scale) => {
                read_text!(
                    builder,
                    field,
                    Decimal128Builder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        Ok::<_, DataFusionError>(decimal_text_to_i128(
                            v.as_bytes(),
                            *scale as usize,
                        ))
                    }
                );
            }
            DataType::Utf8 => {
                read_text!(
                    builder,
                    field,
                    StringBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                let mut value = Vec::new();
                let is_not_null = row.get_binary(odbc_col_idx, &mut value).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&value)?;
                } else {
                    builder.append_null();
                }
            }
            DataType::Binary => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                    });
                let mut value = Vec::new();
                let is_not_null = row.get_binary(odbc_col_idx, &mut value).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&value);
                } else {
                    builder.append_null();
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                read_text!(
                    builder,
                    field,
                    TimestampSecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let ndt = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse timestamp: {e:?}"
                                ))
                            })?;
                        Ok::<_, DataFusionError>(ndt.and_utc().timestamp())
                    }
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                read_text!(
                    builder,
                    field,
                    TimestampMillisecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let ndt = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse timestamp: {e:?}"
                                ))
                            })?;
                        Ok::<_, DataFusionError>(ndt.and_utc().timestamp_millis())
                    }
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                read_text!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let ndt = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse timestamp: {e:?}"
                                ))
                            })?;
                        Ok::<_, DataFusionError>(ndt.and_utc().timestamp_micros())
                    }
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                read_text!(
                    builder,
                    field,
                    TimestampNanosecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let ndt = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse timestamp: {e:?}"
                                ))
                            })?;
                        let nanos = ndt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                            DataFusionError::Execution(format!("Timestamp out of range: {ndt}"))
                        })?;
                        Ok::<_, DataFusionError>(nanos)
                    }
                );
            }
            DataType::Time32(TimeUnit::Second) => {
                read_text!(
                    builder,
                    field,
                    Time32SecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(nt.num_seconds_from_midnight() as i32)
                    }
                );
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                read_text!(
                    builder,
                    field,
                    Time32MillisecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i32 * 1000
                                + (nt.nanosecond() / 1000_000) as i32,
                        )
                    }
                );
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                read_text!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i64 * 1000_1000
                                + (nt.nanosecond() / 1000) as i64,
                        )
                    }
                );
            }
            DataType::Date32 => {
                read_text!(
                    builder,
                    field,
                    Date32Builder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let date = NaiveDate::parse_from_str(&v, "%Y-%m-%d").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse date: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(Date32Type::from_naive_date(date))
                    }
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type: {field:?}",
                )));
            }
        }
    }

    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}

fn parse_primitive<T: FromStr>(s: String) -> DFResult<T>
where
    <T as FromStr>::Err: std::fmt::Debug,
{
    s.parse::<T>()
        .map_err(|e| DataFusionError::Execution(format!("Failed to parse value: {e:?}")))
}
