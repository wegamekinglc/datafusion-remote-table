use crate::connection::{ODBC_ENV, project_batch};
use crate::{
    Connection, ConnectionOptions, DFResult, DmType, Pool, RemoteDbType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use arrow_odbc::OdbcReaderBuilder;
use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::lock::Mutex;
use odbc_api::handles::StatementImpl;
use odbc_api::{CursorImpl, Environment, ResultSetMetadata};
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone, With, Getters)]
pub struct DmConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) schema: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl DmConnectionOptions {
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
            schema: None,
            pool_max_size: 10,
            stream_chunk_size: 2048,
        }
    }
}

#[derive(Debug)]
pub struct DmPool {
    options: DmConnectionOptions,
}

pub(crate) fn connect_dm(options: &DmConnectionOptions) -> DFResult<DmPool> {
    Ok(DmPool {
        options: options.clone(),
    })
}

#[async_trait::async_trait]
impl Pool for DmPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let env = ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
        let mut connection_str = format!(
            "Driver={};Server={};Port={};UID={};PWD={}",
            "{DM8 ODBC DRIVER}",
            self.options.host,
            self.options.port,
            self.options.username,
            self.options.password,
        );
        if let Some(schema) = &self.options.schema {
            connection_str.push_str(&format!(";SCHEMA={schema}"));
        }
        let connection = env
            .connect_with_connection_string(&connection_str, odbc_api::ConnectionOptions::default())
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create odbc connection: {e:?}"))
            })?;
        Ok(Arc::new(DmConnection {
            conn: Arc::new(Mutex::new(connection)),
        }))
    }
}

#[derive(Debug)]
pub struct DmConnection {
    conn: Arc<Mutex<odbc_api::Connection<'static>>>,
}

#[async_trait::async_trait]
impl Connection for DmConnection {
    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Dm.query_limit_1(sql)?;
        let conn = self.conn.lock().await;
        let cursor_opt = conn
            .execute(&sql, (), None)
            .map_err(|e| DataFusionError::Execution(format!("Failed to infer schema: {e:?}")))?;
        match cursor_opt {
            None => Err(DataFusionError::Execution(
                "No rows returned to infer schema".to_string(),
            )),
            Some(cursor) => {
                let remote_schema = Arc::new(build_remote_schema(cursor)?);
                Ok(remote_schema)
            }
        }
    }

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;
        let sql = RemoteDbType::Dm.try_rewrite_query(sql, filters, limit)?;
        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let projection = projection.cloned();
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            let cursor_opt = conn.execute(&sql, (), None).map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute query: {e:?}"))
            })?;

            match cursor_opt {
                None => {}
                Some(cursor) => {
                    let reader = OdbcReaderBuilder::new()
                        .with_schema(table_schema)
                        .with_max_num_rows_per_batch(chunk_size)
                        .with_max_bytes_per_batch(256 * 1024 * 1024)
                        .build(cursor)
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Failed to build reader: {e:?}"))
                        })?;
                    for batch in reader {
                        let projected_batch = project_batch(batch?, projection.as_ref())?;
                        batch_tx.blocking_send(projected_batch).map_err(|e| {
                            DataFusionError::Execution(format!("Failed to send batch: {e:?}"))
                        })?;
                    }
                }
            }

            Ok::<_, DataFusionError>(())
        });

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            if let Err(e) = join_handle.await {
                yield Err(DataFusionError::Execution(format!(
                    "Failed to execute ODBC query: {e}"
                )))
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            output_stream,
        )))
    }
}

fn build_remote_schema(mut cursor: CursorImpl<StatementImpl>) -> DFResult<RemoteSchema> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        let col_name = cursor
            .col_name(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let col_type = cursor
            .col_data_type(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let remote_type = RemoteType::Dm(dm_type_to_remote_type(col_type)?);
        let col_nullable = cursor
            .col_nullability(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .could_be_nullable();

        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

fn dm_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<DmType> {
    match data_type {
        odbc_api::DataType::TinyInt => Ok(DmType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(DmType::SmallInt),
        odbc_api::DataType::Integer => Ok(DmType::Integer),
        odbc_api::DataType::BigInt => Ok(DmType::BigInt),
        odbc_api::DataType::Real => Ok(DmType::Real),
        odbc_api::DataType::Double => Ok(DmType::Double),
        odbc_api::DataType::Numeric { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Numeric(precision as u8, scale as i8))
        }
        odbc_api::DataType::Decimal { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Decimal(precision as u8, scale as i8))
        }
        odbc_api::DataType::Char { length } => Ok(DmType::Char(length.map(|l| l.get() as u16))),
        odbc_api::DataType::Varchar { length } => {
            Ok(DmType::Varchar(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Date => Ok(DmType::Date),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported DM type: {data_type:?}"
        ))),
    }
}
