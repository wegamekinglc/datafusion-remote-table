use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    Connection, DFResult, MysqlType, Pool, RemoteField, RemoteSchema, RemoteType, Transform,
};
use async_stream::stream;
use datafusion::arrow::array::{
    make_builder, ArrayRef, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, RecordBatch,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::lock::Mutex;
use futures::StreamExt;
use mysql_async::consts::ColumnType;
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
        let host = host.into();
        let username = username.into();
        let password = password.into();
        Self {
            host,
            port,
            username,
            password,
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
        transform: Option<&dyn Transform>,
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
        let batch = rows_to_batch(&[row], arrow_schema.clone(), None)?;
        if let Some(transform) = transform {
            let transformed_batch = transform_batch(batch, transform, &remote_schema)?;
            Ok((remote_schema, transformed_batch.schema()))
        } else {
            Ok((remote_schema, batch.schema()))
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
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let first_chunk = rows_to_batch(
            first_chunk.as_slice(),
            arrow_schema.clone(),
            projection.as_ref(),
        )?;
        let schema = first_chunk.schema();

        let mut stream = stream.map(move |rows| {
            let rows = rows?;
            let batch = rows_to_batch(rows.as_slice(), arrow_schema.clone(), projection.as_ref())?;
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
            remote_schema,
        ))
    }
}

fn mysql_type_to_remote_type(mysql_col: &Column) -> DFResult<RemoteType> {
    match mysql_col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => Ok(RemoteType::Mysql(MysqlType::TinyInt)),
        ColumnType::MYSQL_TYPE_SHORT => Ok(RemoteType::Mysql(MysqlType::SmallInt)),
        ColumnType::MYSQL_TYPE_LONG => Ok(RemoteType::Mysql(MysqlType::Integer)),
        ColumnType::MYSQL_TYPE_LONGLONG => Ok(RemoteType::Mysql(MysqlType::BigInt)),
        ColumnType::MYSQL_TYPE_FLOAT => Ok(RemoteType::Mysql(MysqlType::Float)),
        ColumnType::MYSQL_TYPE_DOUBLE => Ok(RemoteType::Mysql(MysqlType::Double)),
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
            .expect(concat!(
                "Failed to downcast builder to ",
                stringify!($builder_ty),
                " for ",
                stringify!($mysql_col)
            ));
        let v = $row.get::<$value_ty, usize>($index);

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

fn rows_to_batch(
    rows: &[Row],
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
        for (idx, col) in row.columns_ref().iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let builder = &mut array_builders[idx];
            match col.column_type() {
                ColumnType::MYSQL_TYPE_TINY => {
                    handle_primitive_type!(builder, col, Int8Builder, i8, row, idx);
                }
                ColumnType::MYSQL_TYPE_SHORT => {
                    handle_primitive_type!(builder, col, Int16Builder, i16, row, idx);
                }
                ColumnType::MYSQL_TYPE_LONG => {
                    handle_primitive_type!(builder, col, Int32Builder, i32, row, idx);
                }
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    handle_primitive_type!(builder, col, Int64Builder, i64, row, idx);
                }
                ColumnType::MYSQL_TYPE_FLOAT => {
                    handle_primitive_type!(builder, col, Float32Builder, f32, row, idx);
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    handle_primitive_type!(builder, col, Float64Builder, f64, row, idx);
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported mysql type: {col:?}",
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
