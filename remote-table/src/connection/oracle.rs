use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    project_remote_schema, Connection, DFResult, OracleType, Pool, RemoteField, RemoteSchema,
    RemoteType, Transform,
};
use bb8_oracle::OracleConnectionManager;
use datafusion::arrow::array::{make_builder, ArrayRef, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use oracle::sql_type::OracleType as ColumnType;
use oracle::{Connector, Row};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OracleConnectionOptions {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub service_name: String,
}

impl OracleConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            service_name: service_name.into(),
        }
    }
}

#[derive(Debug)]
pub struct OraclePool {
    pool: bb8::Pool<OracleConnectionManager>,
}

pub async fn connect_oracle(options: &OracleConnectionOptions) -> DFResult<OraclePool> {
    let connect_string = format!(
        "//{}:{}/{}",
        options.host, options.port, options.service_name
    );
    let connector = Connector::new(
        options.username.clone(),
        options.password.clone(),
        connect_string,
    );
    let _ = connector
        .connect()
        .map_err(|e| DataFusionError::Internal(format!("Failed to connect to oracle: {e:?}")))?;
    let manager = OracleConnectionManager::from_connector(connector);
    let pool = bb8::Pool::builder()
        .build(manager)
        .await
        .map_err(|e| DataFusionError::Internal(format!("Failed to create oracle pool: {:?}", e)))?;
    Ok(OraclePool { pool })
}

#[async_trait::async_trait]
impl Pool for OraclePool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get oracle connection due to {e:?}"))
        })?;
        Ok(Arc::new(OracleConnection { conn }))
    }
}

#[derive(Debug)]
pub struct OracleConnection {
    conn: bb8::PooledConnection<'static, OracleConnectionManager>,
}

#[async_trait::async_trait]
impl Connection for OracleConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
        let row = self.conn.query_row(sql, &[]).map_err(|e| {
            DataFusionError::Execution(format!("Failed to query one row to infer schema: {e:?}"))
        })?;
        let remote_schema = build_remote_schema(&row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        if let Some(transform) = transform {
            let batch = rows_to_batch(&[row], arrow_schema, None)?;
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
        let result_set = self.conn.query(&sql, &[]).unwrap();
        let mut stream = futures::stream::iter(result_set).chunks(2000).boxed();

        let Some(first_chunk) = stream.next().await else {
            return Err(DataFusionError::Execution(
                "No data returned from oracle".to_string(),
            ));
        };
        let first_chunk: Vec<Row> = first_chunk
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::Execution(
                    format!("Failed to collect rows from oracle due to {e}",),
                )
            })?;
        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from oracle".to_string(),
            ));
        };

        let remote_schema = build_remote_schema(first_row)?;
        let projected_remote_schema = project_remote_schema(&remote_schema, projection.as_ref());
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let first_chunk = rows_to_batch(
            first_chunk.as_slice(),
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
                        "Failed to collect rows from oracle due to {e}",
                    ))
                })?;
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
            projected_remote_schema,
        ))
    }
}

fn oracle_type_to_remote_type(oracle_type: &ColumnType) -> DFResult<RemoteType> {
    match oracle_type {
        ColumnType::Varchar2(size) => Ok(RemoteType::Oracle(OracleType::Varchar2(*size))),
        ColumnType::Char(size) => Ok(RemoteType::Oracle(OracleType::Char(*size))),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported oracle type: {oracle_type:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in row.column_info() {
        let remote_type = oracle_type_to_remote_type(col.oracle_type())?;
        remote_fields.push(RemoteField::new(col.name(), remote_type, col.nullable()));
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
        let v = $row
            .get::<usize, Option<$value_ty>>($index)
            .unwrap_or_else(|e| {
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
        for (i, col) in row.column_info().iter().enumerate() {
            let builder = &mut array_builders[i];
            match col.oracle_type() {
                ColumnType::Varchar2(_size) | ColumnType::Char(_size) => {
                    handle_primitive_type!(builder, col, StringBuilder, String, row, i);
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported oracle type: {col:?}",
                    )))
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
