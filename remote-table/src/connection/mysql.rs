use crate::connection::{big_decimal_to_i128, projections_contains};
use crate::transform::transform_batch;
use crate::{
    Connection, DFResult, MysqlType, Pool, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteType,
    Transform,
};
use async_stream::stream;
use bigdecimal::num_bigint;
use chrono::Timelike;
use datafusion::arrow::array::{
    make_builder, ArrayRef, BinaryBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    LargeBinaryBuilder, LargeStringBuilder, RecordBatch, StringBuilder, Time32SecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use datafusion::arrow::datatypes::{i256, DataType, Date32Type, SchemaRef, TimeUnit};
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::lock::Mutex;
use futures::StreamExt;
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Row};
use std::sync::Arc;

#[derive(Debug, Clone, derive_with::With)]
pub struct MysqlConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
}

impl MysqlConnectionOptions {
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
pub struct MysqlPool {
    pool: mysql_async::Pool,
}

pub(crate) fn connect_mysql(options: &MysqlConnectionOptions) -> DFResult<MysqlPool> {
    let opts_builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(options.host.clone())
        .tcp_port(options.port)
        .user(Some(options.username.clone()))
        .pass(Some(options.password.clone()))
        .db_name(options.database.clone());
    let pool = mysql_async::Pool::new(opts_builder);
    Ok(MysqlPool { pool })
}

#[async_trait::async_trait]
impl Pool for MysqlPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_conn().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get mysql connection from pool: {:?}", e))
        })?;
        Ok(Arc::new(MysqlConnection {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }
}

#[derive(Debug)]
pub struct MysqlConnection {
    conn: Arc<Mutex<mysql_async::Conn>>,
}

#[async_trait::async_trait]
impl Connection for MysqlConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchemaRef, SchemaRef)> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let row: Option<Row> = conn.query_first(sql).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute query on mysql: {e:?}",))
        })?;
        let Some(row) = row else {
            return Err(DataFusionError::Execution(
                "No rows returned to infer schema".to_string(),
            ));
        };
        let remote_schema = Arc::new(build_remote_schema(&row)?);
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        if let Some(transform) = transform {
            let batch = rows_to_batch(&[row], &arrow_schema, None)?;
            let transformed_batch = transform_batch(
                batch,
                transform.as_ref(),
                &arrow_schema,
                None,
                Some(&remote_schema),
            )?;
            Ok((remote_schema, transformed_batch.schema()))
        } else {
            Ok((remote_schema, arrow_schema))
        }
    }

    async fn query(
        &self,
        sql: String,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection.as_ref())?;
        let conn = Arc::clone(&self.conn);
        let stream = Box::pin(stream! {
            let mut conn = conn.lock().await;
            let mut query_iter = conn
                .query_iter(sql)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to execute query on mysql: {e:?}"))
                })?;

            let Some(stream) = query_iter.stream::<Row>().await.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to get stream from mysql: {e:?}"))
                })? else {
                yield Err(DataFusionError::Execution("Get none stream from mysql".to_string()));
                return;
            };

            let mut chunked_stream = stream.chunks(4_000).boxed();

            while let Some(chunk) = chunked_stream.next().await {
                let rows = chunk
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to collect rows from mysql due to {e}",
                        ))
                    })?;

                yield Ok::<_, DataFusionError>(rows)
            }
        });

        let stream = stream.map(move |rows| {
            let rows = rows?;
            rows_to_batch(rows.as_slice(), &table_schema, projection.as_ref())
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

fn mysql_type_to_remote_type(mysql_col: &Column) -> DFResult<RemoteType> {
    let is_binary = mysql_col.flags().contains(ColumnFlags::BINARY_FLAG);
    let is_blob = mysql_col.flags().contains(ColumnFlags::BLOB_FLAG);
    let is_unsigned = mysql_col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let col_length = mysql_col.column_length();
    match mysql_col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => {
            if is_unsigned {
                Ok(RemoteType::Mysql(MysqlType::TinyIntUnsigned))
            } else {
                Ok(RemoteType::Mysql(MysqlType::TinyInt))
            }
        }
        ColumnType::MYSQL_TYPE_SHORT => {
            if is_unsigned {
                Ok(RemoteType::Mysql(MysqlType::SmallIntUnsigned))
            } else {
                Ok(RemoteType::Mysql(MysqlType::SmallInt))
            }
        }
        ColumnType::MYSQL_TYPE_INT24 => {
            if is_unsigned {
                Ok(RemoteType::Mysql(MysqlType::MediumIntUnsigned))
            } else {
                Ok(RemoteType::Mysql(MysqlType::MediumInt))
            }
        }
        ColumnType::MYSQL_TYPE_LONG => {
            if is_unsigned {
                Ok(RemoteType::Mysql(MysqlType::IntegerUnsigned))
            } else {
                Ok(RemoteType::Mysql(MysqlType::Integer))
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if is_unsigned {
                Ok(RemoteType::Mysql(MysqlType::BigIntUnsigned))
            } else {
                Ok(RemoteType::Mysql(MysqlType::BigInt))
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT => Ok(RemoteType::Mysql(MysqlType::Float)),
        ColumnType::MYSQL_TYPE_DOUBLE => Ok(RemoteType::Mysql(MysqlType::Double)),
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let precision = (mysql_col.column_length() - 2) as u8;
            let scale = mysql_col.decimals();
            Ok(RemoteType::Mysql(MysqlType::Decimal(precision, scale)))
        }
        ColumnType::MYSQL_TYPE_DATE => Ok(RemoteType::Mysql(MysqlType::Date)),
        ColumnType::MYSQL_TYPE_DATETIME => Ok(RemoteType::Mysql(MysqlType::Datetime)),
        ColumnType::MYSQL_TYPE_TIME => Ok(RemoteType::Mysql(MysqlType::Time)),
        ColumnType::MYSQL_TYPE_TIMESTAMP => Ok(RemoteType::Mysql(MysqlType::Timestamp)),
        ColumnType::MYSQL_TYPE_YEAR => Ok(RemoteType::Mysql(MysqlType::Year)),
        ColumnType::MYSQL_TYPE_STRING if !is_binary => Ok(RemoteType::Mysql(MysqlType::Char)),
        ColumnType::MYSQL_TYPE_STRING if is_binary => Ok(RemoteType::Mysql(MysqlType::Binary)),
        ColumnType::MYSQL_TYPE_VAR_STRING if !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Varchar))
        }
        ColumnType::MYSQL_TYPE_VAR_STRING if is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Varbinary))
        }
        ColumnType::MYSQL_TYPE_VARCHAR => Ok(RemoteType::Mysql(MysqlType::Varchar)),
        ColumnType::MYSQL_TYPE_BLOB if is_blob && !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Text(col_length)))
        }
        ColumnType::MYSQL_TYPE_BLOB if is_blob && is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Blob(col_length)))
        }
        ColumnType::MYSQL_TYPE_JSON => Ok(RemoteType::Mysql(MysqlType::Json)),
        ColumnType::MYSQL_TYPE_GEOMETRY => Ok(RemoteType::Mysql(MysqlType::Geometry)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported mysql type: {mysql_col:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in row.columns_ref() {
        remote_fields.push(RemoteField::new(
            col.name_str().to_string(),
            mysql_type_to_remote_type(col)?,
            true,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

macro_rules! handle_primitive_type {
    ($builder:expr, $mysql_col:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
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
                    $mysql_col
                )
            });
        let v = $row.get::<Option<$value_ty>, usize>($index);

        match v {
            Some(Some(v)) => builder.append_value(v),
            _ => builder.append_null(),
        }
    }};
}

fn rows_to_batch(
    rows: &[Row],
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), rows.len());
        array_builders.push(builder);
    }

    for row in rows {
        for (idx, field) in table_schema.fields.iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let builder = &mut array_builders[idx];
            let col = row.columns_ref().get(idx);
            match field.data_type() {
                DataType::Int8 => {
                    handle_primitive_type!(builder, col, Int8Builder, i8, row, idx);
                }
                DataType::Int16 => {
                    handle_primitive_type!(builder, col, Int16Builder, i16, row, idx);
                }
                DataType::Int32 => {
                    handle_primitive_type!(builder, col, Int32Builder, i32, row, idx);
                }
                DataType::Int64 => {
                    handle_primitive_type!(builder, col, Int64Builder, i64, row, idx);
                }
                DataType::UInt8 => {
                    handle_primitive_type!(builder, col, UInt8Builder, u8, row, idx);
                }
                DataType::UInt16 => {
                    handle_primitive_type!(builder, col, UInt16Builder, u16, row, idx);
                }
                DataType::UInt32 => {
                    handle_primitive_type!(builder, col, UInt32Builder, u32, row, idx);
                }
                DataType::UInt64 => {
                    handle_primitive_type!(builder, col, UInt64Builder, u64, row, idx);
                }
                DataType::Float32 => {
                    handle_primitive_type!(builder, col, Float32Builder, f32, row, idx);
                }
                DataType::Float64 => {
                    handle_primitive_type!(builder, col, Float64Builder, f64, row, idx);
                }
                DataType::Decimal128(_precision, scale) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal128Builder for {field:?}")
                        });
                    let v = row.get::<Option<bigdecimal::BigDecimal>, usize>(idx);

                    match v {
                        Some(Some(v)) => {
                            let Some(v) = big_decimal_to_i128(&v, Some(*scale as u32)) else {
                                return Err(DataFusionError::Execution(format!(
                                    "Failed to convert BigDecimal {v:?} to i128"
                                )));
                            };
                            builder.append_value(v)
                        }
                        _ => builder.append_null(),
                    }
                }
                DataType::Decimal256(_precision, _scale) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal256Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal256Builder for {field:?}")
                        });
                    let v = row.get::<Option<bigdecimal::BigDecimal>, usize>(idx);

                    match v {
                        Some(Some(v)) => builder.append_value(to_decimal_256(&v)),
                        _ => builder.append_null(),
                    }
                }
                DataType::Date32 => {
                    // TODO this could also be simplified by macro
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Date32Builder for {field:?}")
                        });
                    let v = row.get::<Option<chrono::NaiveDate>, usize>(idx);

                    match v {
                        Some(Some(v)) => builder.append_value(Date32Type::from_naive_date(v)),
                        _ => builder.append_null(),
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let builder = builder
                                .as_any_mut()
                                .downcast_mut::<TimestampMicrosecondBuilder>()
                                .unwrap_or_else(|| {
                                    panic!("Failed to downcast builder to TimestampMicrosecondBuilder for {field:?}")
                                });
                    let v = row.get::<Option<time::PrimitiveDateTime>, usize>(idx);

                    match v {
                        Some(Some(v)) => {
                            let timestamp_micros =
                                (v.assume_utc().unix_timestamp_nanos() / 1_000) as i64;
                            builder.append_value(timestamp_micros)
                        }
                        _ => builder.append_null(),
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let builder = builder
                                .as_any_mut()
                                .downcast_mut::<Time64NanosecondBuilder>()
                                .unwrap_or_else(|| {
                                    panic!("Failed to downcast builder to Time64NanosecondBuilder for {field:?}")
                                });
                    let v = row.get::<Option<chrono::NaiveTime>, usize>(idx);

                    match v {
                        Some(Some(v)) => {
                            builder.append_value(
                                i64::from(v.num_seconds_from_midnight()) * 1_000_000_000
                                    + i64::from(v.nanosecond()),
                            );
                        }
                        _ => builder.append_null(),
                    }
                }
                DataType::Time32(TimeUnit::Second) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Time32SecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to Time32SecondBuilder for {field:?}"
                            )
                        });
                    let v = row.get::<Option<chrono::NaiveTime>, usize>(idx);

                    match v {
                        Some(Some(v)) => {
                            builder.append_value(v.num_seconds_from_midnight() as i32);
                        }
                        _ => builder.append_null(),
                    }
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Time64NanosecondBuilder for {field:?}")
                        });
                    let v = row.get::<Option<chrono::NaiveTime>, usize>(idx);

                    match v {
                        Some(Some(v)) => {
                            builder.append_value(
                                i64::from(v.num_seconds_from_midnight()) * 1_000_000_000
                                    + i64::from(v.nanosecond()),
                            );
                        }
                        _ => builder.append_null(),
                    }
                }
                DataType::Utf8 => {
                    handle_primitive_type!(builder, col, StringBuilder, String, row, idx);
                }
                DataType::LargeUtf8 => {
                    handle_primitive_type!(builder, col, LargeStringBuilder, String, row, idx);
                }
                DataType::Binary => {
                    handle_primitive_type!(builder, col, BinaryBuilder, Vec<u8>, row, idx);
                }
                DataType::LargeBinary => {
                    handle_primitive_type!(builder, col, LargeBinaryBuilder, Vec<u8>, row, idx);
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {:?} for col: {:?}",
                        field.data_type(),
                        col
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

fn to_decimal_256(decimal: &bigdecimal::BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}
