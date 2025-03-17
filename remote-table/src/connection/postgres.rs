use crate::connection::{big_decimal_to_i128, projections_contains};
use crate::transform::transform_batch;
use crate::{
    project_remote_schema, Connection, DFResult, Pool, PostgresType, RemoteField, RemoteSchema,
    RemoteType, Transform,
};
use bb8_postgres::tokio_postgres::types::{FromSql, Type};
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bb8_postgres::PostgresConnectionManager;
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::Timelike;
use datafusion::arrow::array::{
    make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder,
    Decimal128Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    IntervalMonthDayNanoBuilder, LargeStringBuilder, ListBuilder, RecordBatch, StringBuilder,
    Time64NanosecondBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{Date32Type, IntervalMonthDayNanoType, SchemaRef};
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use num_bigint::{BigInt, Sign};
use std::string::ToString;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, derive_with::With)]
pub struct PostgresConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
}

impl PostgresConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            database: None,
        }
    }
}

#[derive(Debug)]
pub struct PostgresPool {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Pool for PostgresPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get postgres connection due to {e:?}"))
        })?;
        Ok(Arc::new(PostgresConnection { conn }))
    }
}

pub(crate) async fn connect_postgres(
    options: &PostgresConnectionOptions,
) -> DFResult<PostgresPool> {
    let mut config = bb8_postgres::tokio_postgres::config::Config::new();
    config
        .host(&options.host)
        .port(options.port)
        .user(&options.username)
        .password(&options.password);
    if let Some(database) = &options.database {
        config.dbname(database);
    }
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = bb8::Pool::builder()
        .max_size(5)
        .build(manager)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create postgres connection pool due to {e}",
            ))
        })?;

    Ok(PostgresPool { pool })
}

#[derive(Debug)]
pub(crate) struct PostgresConnection {
    conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
        let mut stream = self
            .conn
            .query_raw(sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on postgres due to {e}",
                ))
            })?
            .chunks(1)
            .boxed();

        let Some(first_chunk) = stream.next().await else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let first_chunk: Vec<Row> = first_chunk
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to collect rows from postgres due to {e}",
                ))
            })?;
        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let remote_schema = build_remote_schema(first_row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        if let Some(transform) = transform {
            let batch = rows_to_batch(
                std::slice::from_ref(first_row),
                &remote_schema,
                arrow_schema,
                None,
            )?;
            let transformed_batch = transform_batch(batch, transform.as_ref(), &remote_schema)?;
            Ok((remote_schema, transformed_batch.schema()))
        } else {
            Ok((remote_schema, arrow_schema))
        }
    }

    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, RemoteSchema)> {
        let mut stream = self
            .conn
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on postgres due to {e}",
                ))
            })?
            .chunks(2048)
            .boxed();

        let Some(first_chunk) = stream.next().await else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let first_chunk: Vec<Row> = first_chunk
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to collect rows from postgres due to {e}",
                ))
            })?;
        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let remote_schema = build_remote_schema(first_row)?;
        let projected_remote_schema = project_remote_schema(&remote_schema, projection.as_ref());
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let first_chunk = rows_to_batch(
            first_chunk.as_slice(),
            &remote_schema,
            arrow_schema.clone(),
            projection.as_ref(),
        )?;
        let schema = first_chunk.schema();

        let mut stream = stream.map(move |rows| {
            let rows: Vec<Row> = rows
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to collect rows from postgres due to {e}",
                    ))
                })?;
            let batch = rows_to_batch(
                rows.as_slice(),
                &remote_schema,
                arrow_schema.clone(),
                projection.as_ref(),
            )?;
            Ok::<RecordBatch, DataFusionError>(batch)
        });

        let output_stream = async_stream::stream! {
           yield Ok(first_chunk);
           while let Some(batch) = stream.next().await {
                yield batch
           }
        };

        Ok((
            Box::pin(RecordBatchStreamAdapter::new(schema, output_stream)),
            projected_remote_schema,
        ))
    }
}

fn pg_type_to_remote_type(pg_type: &Type, row: &Row, idx: usize) -> DFResult<RemoteType> {
    match pg_type {
        &Type::INT2 => Ok(RemoteType::Postgres(PostgresType::Int2)),
        &Type::INT4 => Ok(RemoteType::Postgres(PostgresType::Int4)),
        &Type::INT8 => Ok(RemoteType::Postgres(PostgresType::Int8)),
        &Type::FLOAT4 => Ok(RemoteType::Postgres(PostgresType::Float4)),
        &Type::FLOAT8 => Ok(RemoteType::Postgres(PostgresType::Float8)),
        &Type::NUMERIC => {
            let v: Option<BigDecimalFromSql> = row.try_get(idx).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get BigDecimal value: {e:?}"))
            })?;
            let scale = match v {
                Some(v) => v.scale,
                None => 0,
            };
            assert!((scale as u32) <= (i8::MAX as u32));
            Ok(RemoteType::Postgres(PostgresType::Numeric(
                scale.try_into().unwrap_or_default(),
            )))
        }
        &Type::VARCHAR => Ok(RemoteType::Postgres(PostgresType::Varchar)),
        &Type::BPCHAR => Ok(RemoteType::Postgres(PostgresType::Bpchar)),
        &Type::TEXT => Ok(RemoteType::Postgres(PostgresType::Text)),
        &Type::BYTEA => Ok(RemoteType::Postgres(PostgresType::Bytea)),
        &Type::DATE => Ok(RemoteType::Postgres(PostgresType::Date)),
        &Type::TIMESTAMP => Ok(RemoteType::Postgres(PostgresType::Timestamp)),
        &Type::TIMESTAMPTZ => Ok(RemoteType::Postgres(PostgresType::TimestampTz)),
        &Type::TIME => Ok(RemoteType::Postgres(PostgresType::Time)),
        &Type::INTERVAL => Ok(RemoteType::Postgres(PostgresType::Interval)),
        &Type::BOOL => Ok(RemoteType::Postgres(PostgresType::Bool)),
        &Type::JSON => Ok(RemoteType::Postgres(PostgresType::Json)),
        &Type::JSONB => Ok(RemoteType::Postgres(PostgresType::Jsonb)),
        &Type::INT2_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int2Array)),
        &Type::INT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int4Array)),
        &Type::INT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int8Array)),
        &Type::FLOAT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float4Array)),
        &Type::FLOAT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float8Array)),
        &Type::VARCHAR_ARRAY => Ok(RemoteType::Postgres(PostgresType::VarcharArray)),
        &Type::BPCHAR_ARRAY => Ok(RemoteType::Postgres(PostgresType::BpcharArray)),
        &Type::TEXT_ARRAY => Ok(RemoteType::Postgres(PostgresType::TextArray)),
        &Type::BYTEA_ARRAY => Ok(RemoteType::Postgres(PostgresType::ByteaArray)),
        &Type::BOOL_ARRAY => Ok(RemoteType::Postgres(PostgresType::BoolArray)),
        other if other.name().eq_ignore_ascii_case("geometry") => {
            Ok(RemoteType::Postgres(PostgresType::PostGisGeometry))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported postgres type {pg_type:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for (idx, col) in row.columns().iter().enumerate() {
        remote_fields.push(RemoteField::new(
            col.name(),
            pg_type_to_remote_type(col.type_(), row, idx)?,
            true,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    concat!(
                        "Failed to downcast builder to ",
                        stringify!($builder_ty),
                        " for {:?}"
                    ),
                    $field
                )
            });
        let v: Option<$value_ty> = $row.try_get($index).unwrap_or_else(|e| {
            panic!(
                concat!(
                    "Failed to get ",
                    stringify!($value_ty),
                    " value for {:?}: {:?}"
                ),
                $field, e
            )
        });

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

macro_rules! handle_primitive_array_type {
    ($builder:expr, $field:expr, $values_builder_ty:ty, $primitive_value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for {:?}",
                    $field
                )
            });
        let values_builder = builder
            .values()
            .as_any_mut()
            .downcast_mut::<$values_builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    concat!(
                        "Failed to downcast values builder to ",
                        stringify!($builder_ty),
                        " for {:?}"
                    ),
                    $field
                )
            });
        let v: Option<Vec<$primitive_value_ty>> = $row.try_get($index).unwrap_or_else(|e| {
            panic!(
                concat!(
                    "Failed to get ",
                    stringify!($value_ty),
                    " array value for {:?}: {:?}"
                ),
                $field, e
            )
        });

        match v {
            Some(v) => {
                let v = v.into_iter().map(Some);
                values_builder.extend(v);
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }};
}

#[derive(Debug)]
struct BigDecimalFromSql {
    inner: BigDecimal,
    scale: u16,
}

impl BigDecimalFromSql {
    fn to_decimal_128(&self) -> Option<i128> {
        big_decimal_to_i128(&self.inner, Some(self.scale as u32))
    }
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
impl<'a> FromSql<'a> for BigDecimalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw_u16: Vec<u16> = raw
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    u16::from_be_bytes([chunk[0], chunk[1]])
                } else {
                    u16::from_be_bytes([chunk[0], 0])
                }
            })
            .collect();

        let base_10_000_digit_count = raw_u16[0];
        let weight = raw_u16[1] as i16;
        let sign = raw_u16[2];
        let scale = raw_u16[3];

        let mut base_10_000_digits = Vec::new();
        for i in 4..4 + base_10_000_digit_count {
            base_10_000_digits.push(raw_u16[i as usize]);
        }

        let mut u8_digits = Vec::new();
        for &base_10_000_digit in base_10_000_digits.iter().rev() {
            let mut base_10_000_digit = base_10_000_digit;
            let mut temp_result = Vec::new();
            while base_10_000_digit > 0 {
                temp_result.push((base_10_000_digit % 10) as u8);
                base_10_000_digit /= 10;
            }
            while temp_result.len() < 4 {
                temp_result.push(0);
            }
            u8_digits.extend(temp_result);
        }
        u8_digits.reverse();

        let value_scale = 4 * (i64::from(base_10_000_digit_count) - i64::from(weight) - 1);
        let size = i64::try_from(u8_digits.len())? + i64::from(scale) - value_scale;
        u8_digits.resize(size as usize, 0);

        let sign = match sign {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            _ => {
                return Err(Box::new(DataFusionError::Execution(
                    "Failed to parse big decimal from postgres numeric value".to_string(),
                )));
            }
        };

        let Some(digits) = BigInt::from_radix_be(sign, u8_digits.as_slice(), 10) else {
            return Err(Box::new(DataFusionError::Execution(
                "Failed to parse big decimal from postgres numeric value".to_string(),
            )));
        };
        Ok(BigDecimalFromSql {
            inner: BigDecimal::new(digits, i64::from(scale)),
            scale,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

// interval_send - Postgres C (https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c#L1032)
// interval values are internally stored as three integral fields: months, days, and microseconds
#[derive(Debug)]
struct IntervalFromSql {
    time: i64,
    day: i32,
    month: i32,
}

impl<'a> FromSql<'a> for IntervalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut cursor = std::io::Cursor::new(raw);

        let time = cursor.read_i64::<BigEndian>()?;
        let day = cursor.read_i32::<BigEndian>()?;
        let month = cursor.read_i32::<BigEndian>()?;

        Ok(IntervalFromSql { time, day, month })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

pub struct GeometryFromSql<'a> {
    wkb: &'a [u8],
}

impl<'a> FromSql<'a> for GeometryFromSql<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GeometryFromSql { wkb: raw })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "geometry")
    }
}

fn rows_to_batch(
    rows: &[Row],
    remote_schema: &RemoteSchema,
    arrow_schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(&arrow_schema, projection)?;
    let mut array_builders = vec![];
    for field in arrow_schema.fields() {
        let builder = make_builder(field.data_type(), rows.len());
        array_builders.push(builder);
    }
    for row in rows {
        for (idx, field) in remote_schema.fields.iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let builder = &mut array_builders[idx];
            match field.remote_type {
                RemoteType::Postgres(PostgresType::Int2) => {
                    handle_primitive_type!(builder, field, Int16Builder, i16, row, idx);
                }
                RemoteType::Postgres(PostgresType::Int4) => {
                    handle_primitive_type!(builder, field, Int32Builder, i32, row, idx);
                }
                RemoteType::Postgres(PostgresType::Int8) => {
                    handle_primitive_type!(builder, field, Int64Builder, i64, row, idx);
                }
                RemoteType::Postgres(PostgresType::Float4) => {
                    handle_primitive_type!(builder, field, Float32Builder, f32, row, idx);
                }
                RemoteType::Postgres(PostgresType::Float8) => {
                    handle_primitive_type!(builder, field, Float64Builder, f64, row, idx);
                }
                RemoteType::Postgres(PostgresType::Numeric(_scale)) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal128Builder for {field:?}")
                        });
                    let v: Option<BigDecimalFromSql> = row.try_get(idx).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to get BigDecimal value: {e:?}"))
                    })?;

                    match v {
                        Some(v) => {
                            let Some(v) = v.to_decimal_128() else {
                                return Err(DataFusionError::Execution(format!(
                                    "Failed to convert BigDecimal {:?} to i128",
                                    v
                                )));
                            };
                            builder.append_value(v)
                        }
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Varchar)
                | RemoteType::Postgres(PostgresType::Text) => {
                    handle_primitive_type!(builder, field, StringBuilder, &str, row, idx);
                }
                RemoteType::Postgres(PostgresType::Bpchar) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to StringBuilder for {field:?}")
                        });
                    let v: Option<&str> = row.try_get(idx).unwrap_or_else(|e| {
                        panic!("Failed to get &str value for {field:?}: {e:?}")
                    });

                    match v {
                        Some(v) => builder.append_value(v.trim_end()),
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Bytea) => {
                    handle_primitive_type!(builder, field, BinaryBuilder, Vec<u8>, row, idx);
                }
                RemoteType::Postgres(PostgresType::Timestamp) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .unwrap_or_else(|| panic!("Failed to downcast builder to TimestampNanosecondBuilder for {field:?}"));
                    let v: Option<SystemTime> = row.try_get(idx).unwrap_or_else(|e| {
                        panic!("Failed to get SystemTime value for {field:?}: {e:?}")
                    });

                    match v {
                        Some(v) => {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_nanos()
                                    .try_into()
                                    .expect("Failed to convert SystemTime to i64");
                                builder.append_value(timestamp);
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::TimestampTz) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to TimestampTz for {field:?}")
                        });
                    let v: Option<chrono::DateTime<chrono::Utc>> = row.try_get(idx).unwrap_or_else(|e| panic!("Failed to get chrono::DateTime<chrono::Utc> value for {field:?}: {e:?}"));

                    match v {
                        Some(v) => {
                            let timestamp: i64 = v.timestamp_nanos_opt().unwrap_or_else(|| panic!("Failed to get timestamp in nanoseconds from {v} for Type::TIMESTAMP"));
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Time) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                        .expect(
                            "Failed to downcast builder to Time64NanosecondBuilder for Type::TIME",
                        );
                    let v: Option<chrono::NaiveTime> = row
                        .try_get(idx)
                        .expect("Failed to get chrono::NaiveTime value for column Type::TIME");

                    match v {
                        Some(v) => {
                            let timestamp: i64 = i64::from(v.num_seconds_from_midnight())
                                * 1_000_000_000
                                + i64::from(v.nanosecond());
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Date) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .expect("Failed to downcast builder to Date32Builder for Type::DATE");
                    let v: Option<chrono::NaiveDate> = row
                        .try_get(idx)
                        .expect("Failed to get chrono::NaiveDate value for column Type::DATE");

                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Interval) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<IntervalMonthDayNanoBuilder>()
                        .expect("Failed to downcast builder to IntervalMonthDayNanoBuilder for Type::INTERVAL");

                    let v: Option<IntervalFromSql> = row
                        .try_get(idx)
                        .expect("Failed to get IntervalFromSql value for column Type::INTERVAL");

                    match v {
                        Some(v) => {
                            let interval_month_day_nano = IntervalMonthDayNanoType::make_value(
                                v.month,
                                v.day,
                                v.time * 1_000,
                            );
                            builder.append_value(interval_month_day_nano);
                        }
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Bool) => {
                    handle_primitive_type!(builder, field, BooleanBuilder, bool, row, idx);
                }
                RemoteType::Postgres(PostgresType::Json)
                | RemoteType::Postgres(PostgresType::Jsonb) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<LargeStringBuilder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to LargeStringBuilder for {field:?}")
                        });
                    let v: Option<serde_json::value::Value> =
                        row.try_get(idx).unwrap_or_else(|e| {
                            panic!(
                                "Failed to get serde_json::value::Value value for {field:?}: {e:?}"
                            )
                        });

                    match v {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
                RemoteType::Postgres(PostgresType::Int2Array) => {
                    handle_primitive_array_type!(builder, field, Int16Builder, i16, row, idx);
                }
                RemoteType::Postgres(PostgresType::Int4Array) => {
                    handle_primitive_array_type!(builder, field, Int32Builder, i32, row, idx);
                }
                RemoteType::Postgres(PostgresType::Int8Array) => {
                    handle_primitive_array_type!(builder, field, Int64Builder, i64, row, idx);
                }
                RemoteType::Postgres(PostgresType::Float4Array) => {
                    handle_primitive_array_type!(builder, field, Float32Builder, f32, row, idx);
                }
                RemoteType::Postgres(PostgresType::Float8Array) => {
                    handle_primitive_array_type!(builder, field, Float64Builder, f64, row, idx);
                }
                RemoteType::Postgres(PostgresType::VarcharArray)
                | RemoteType::Postgres(PostgresType::BpcharArray)
                | RemoteType::Postgres(PostgresType::TextArray) => {
                    handle_primitive_array_type!(builder, field, StringBuilder, &str, row, idx);
                }
                RemoteType::Postgres(PostgresType::ByteaArray) => {
                    handle_primitive_array_type!(builder, field, BinaryBuilder, Vec<u8>, row, idx);
                }
                RemoteType::Postgres(PostgresType::BoolArray) => {
                    handle_primitive_array_type!(builder, field, BooleanBuilder, bool, row, idx);
                }

                RemoteType::Postgres(PostgresType::PostGisGeometry) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .expect("Failed to downcast builder to BinaryBuilder for Type::geometry");
                    let v: Option<GeometryFromSql> = row
                        .try_get(idx)
                        .expect("Failed to get GeometryFromSql value for column Type::geometry");

                    match v {
                        Some(v) => builder.append_value(v.wkb),
                        None => builder.append_null(),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported postgres type {field:?}",
                    )));
                }
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
