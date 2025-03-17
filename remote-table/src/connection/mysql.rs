use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    project_remote_schema, Connection, DFResult, MysqlType, Pool, RemoteField, RemoteSchema,
    RemoteType, Transform,
};
use async_stream::stream;
use bigdecimal::{num_bigint, ToPrimitive};
use chrono::Timelike;
use datafusion::arrow::array::{
    make_builder, ArrayRef, BinaryBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    LargeBinaryBuilder, LargeStringBuilder, RecordBatch, StringBuilder, Time64NanosecondBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{i256, Date32Type, SchemaRef};
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

pub fn connect_mysql(options: &MysqlConnectionOptions) -> DFResult<MysqlPool> {
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
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
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
        let remote_schema = build_remote_schema(&row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        if let Some(transform) = transform {
            let batch = rows_to_batch(&[row], &remote_schema, arrow_schema.clone(), None)?;
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
        let conn = Arc::clone(&self.conn);
        let mut stream = Box::pin(stream! {
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

        let Some(first_chunk) = stream.next().await else {
            return Err(DataFusionError::Execution(
                "No data returned from mysql".to_string(),
            ));
        };
        let first_chunk = first_chunk?;

        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from mysql".to_string(),
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
            let rows = rows?;
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

fn mysql_type_to_remote_type(mysql_col: &Column) -> DFResult<RemoteType> {
    let empty_flags = mysql_col.flags().is_empty();
    let is_binary = mysql_col.flags().contains(ColumnFlags::BINARY_FLAG);
    let is_blob = mysql_col.flags().contains(ColumnFlags::BLOB_FLAG);
    let col_length = mysql_col.column_length();
    match mysql_col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => Ok(RemoteType::Mysql(MysqlType::TinyInt)),
        ColumnType::MYSQL_TYPE_SHORT => Ok(RemoteType::Mysql(MysqlType::SmallInt)),
        ColumnType::MYSQL_TYPE_INT24 => Ok(RemoteType::Mysql(MysqlType::MediumInt)),
        ColumnType::MYSQL_TYPE_LONG => Ok(RemoteType::Mysql(MysqlType::Integer)),
        ColumnType::MYSQL_TYPE_LONGLONG => Ok(RemoteType::Mysql(MysqlType::BigInt)),
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
        ColumnType::MYSQL_TYPE_STRING if empty_flags => Ok(RemoteType::Mysql(MysqlType::Char)),
        ColumnType::MYSQL_TYPE_STRING if is_binary => Ok(RemoteType::Mysql(MysqlType::Binary)),
        ColumnType::MYSQL_TYPE_VAR_STRING if empty_flags => {
            Ok(RemoteType::Mysql(MysqlType::Varchar))
        }
        ColumnType::MYSQL_TYPE_VAR_STRING if is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Varbinary))
        }
        ColumnType::MYSQL_TYPE_VARCHAR => Ok(RemoteType::Mysql(MysqlType::Varchar)),
        ColumnType::MYSQL_TYPE_BLOB if col_length == 1020 && is_blob && !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::TinyText))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 262140 && is_blob && !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Text))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 67108860 && is_blob && !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::MediumText))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 4294967295 && is_blob && !is_binary => {
            Ok(RemoteType::Mysql(MysqlType::LongText))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 255 && is_blob && is_binary => {
            Ok(RemoteType::Mysql(MysqlType::TinyBlob))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 65535 && is_blob && is_binary => {
            Ok(RemoteType::Mysql(MysqlType::Blob))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 16777215 && is_blob && is_binary => {
            Ok(RemoteType::Mysql(MysqlType::MediumBlob))
        }
        ColumnType::MYSQL_TYPE_BLOB if col_length == 4294967295 && is_blob && is_binary => {
            Ok(RemoteType::Mysql(MysqlType::LongBlob))
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
        for (idx, remote_field) in remote_schema.fields.iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let builder = &mut array_builders[idx];
            match remote_field.remote_type {
                RemoteType::Mysql(MysqlType::TinyInt) => {
                    handle_primitive_type!(builder, remote_field, Int8Builder, i8, row, idx);
                }
                RemoteType::Mysql(MysqlType::SmallInt) | RemoteType::Mysql(MysqlType::Year) => {
                    handle_primitive_type!(builder, remote_field, Int16Builder, i16, row, idx);
                }
                RemoteType::Mysql(MysqlType::MediumInt) | RemoteType::Mysql(MysqlType::Integer) => {
                    handle_primitive_type!(builder, remote_field, Int32Builder, i32, row, idx);
                }
                RemoteType::Mysql(MysqlType::BigInt) => {
                    handle_primitive_type!(builder, remote_field, Int64Builder, i64, row, idx);
                }
                RemoteType::Mysql(MysqlType::Float) => {
                    handle_primitive_type!(builder, remote_field, Float32Builder, f32, row, idx);
                }
                RemoteType::Mysql(MysqlType::Double) => {
                    handle_primitive_type!(builder, remote_field, Float64Builder, f64, row, idx);
                }
                RemoteType::Mysql(MysqlType::Decimal(precision, _scale)) => {
                    if precision > 38 {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<Decimal256Builder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to Decimal256Builder for {:?}",
                                    remote_field
                                )
                            });
                        let v = row.get::<Option<bigdecimal::BigDecimal>, usize>(idx);

                        match v {
                            Some(Some(v)) => builder.append_value(to_decimal_256(&v)),
                            _ => builder.append_null(),
                        }
                    } else {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<Decimal128Builder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to Decimal128Builder for {:?}",
                                    remote_field
                                )
                            });
                        let v = row.get::<Option<bigdecimal::BigDecimal>, usize>(idx);

                        match v {
                            Some(Some(v)) => {
                                let Some(v) = to_decimal_128(&v) else {
                                    return Err(DataFusionError::Execution(format!(
                                        "Failed to convert BigDecimal {:?} to i128",
                                        v
                                    )));
                                };
                                builder.append_value(v)
                            }
                            _ => builder.append_null(),
                        }
                    }
                }
                RemoteType::Mysql(MysqlType::Date) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to Date32Builder for {:?}",
                                remote_field
                            )
                        });
                    let v = row.get::<Option<chrono::NaiveDate>, usize>(idx);

                    match v {
                        Some(Some(v)) => builder.append_value(Date32Type::from_naive_date(v)),
                        _ => builder.append_null(),
                    }
                }
                RemoteType::Mysql(MysqlType::Datetime)
                | RemoteType::Mysql(MysqlType::Timestamp) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                            "Failed to downcast builder to TimestampMicrosecondBuilder for {:?}",
                            remote_field
                        )
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
                RemoteType::Mysql(MysqlType::Time) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to Time64NanosecondBuilder for {:?}",
                                remote_field
                            )
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
                RemoteType::Mysql(MysqlType::Char)
                | RemoteType::Mysql(MysqlType::Varchar)
                | RemoteType::Mysql(MysqlType::TinyText)
                | RemoteType::Mysql(MysqlType::Text)
                | RemoteType::Mysql(MysqlType::MediumText) => {
                    handle_primitive_type!(builder, remote_field, StringBuilder, String, row, idx);
                }
                RemoteType::Mysql(MysqlType::LongText) | RemoteType::Mysql(MysqlType::Json) => {
                    handle_primitive_type!(
                        builder,
                        remote_field,
                        LargeStringBuilder,
                        String,
                        row,
                        idx
                    );
                }
                RemoteType::Mysql(MysqlType::Binary)
                | RemoteType::Mysql(MysqlType::Varbinary)
                | RemoteType::Mysql(MysqlType::TinyBlob)
                | RemoteType::Mysql(MysqlType::Blob)
                | RemoteType::Mysql(MysqlType::MediumBlob) => {
                    handle_primitive_type!(builder, remote_field, BinaryBuilder, Vec<u8>, row, idx);
                }
                RemoteType::Mysql(MysqlType::LongBlob) | RemoteType::Mysql(MysqlType::Geometry) => {
                    handle_primitive_type!(
                        builder,
                        remote_field,
                        LargeBinaryBuilder,
                        Vec<u8>,
                        row,
                        idx
                    );
                }
                _ => panic!("Invalid mysql type: {:?}", remote_field.remote_type),
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

fn to_decimal_128(decimal: &bigdecimal::BigDecimal) -> Option<i128> {
    (decimal
        * 10i128.pow(
            decimal
                .fractional_digit_count()
                .try_into()
                .unwrap_or_default(),
        ))
    .to_i128()
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
