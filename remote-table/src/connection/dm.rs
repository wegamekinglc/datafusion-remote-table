use crate::connection::ODBC_ENV;
use crate::{
    Connection, ConnectionOptions, DFResult, DmType, Pool, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use arrow_odbc::OdbcReaderBuilder;
use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::lock::Mutex;
use odbc_api::buffers::{ColumnarAnyBuffer, RowVec, TextRowSet};
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, Environment, ResultSetMetadata};
use oracle::Row;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone, With, Getters)]
pub struct DmConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
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
            database: None,
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
        if let Some(database) = &self.options.database {
            connection_str.push_str(&format!(";Database={}", database));
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
        let conn = self.conn.lock().await;
        let cursor_opt = conn
            .execute(sql, (), None)
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;
        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let create_stream = async || -> DFResult<SendableRecordBatchStream> {
            let table_schema_captured = table_schema.clone();
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
                            .with_schema(table_schema_captured)
                            .build(cursor)
                            .map_err(|e| {
                                DataFusionError::Execution(format!("Failed to build reader: {e:?}"))
                            })?;
                        for batch in reader {
                            batch_tx.blocking_send(batch?).map_err(|e| {
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

            let result: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(table_schema, output_stream));
            Ok(result)
        };
        run_async_with_tokio(create_stream).await
    }
}

pub async fn run_async_with_tokio<F, Fut, T, E>(f: F) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match Handle::try_current() {
        Ok(_) => f().await,
        Err(_) => execute_in_tokio(f),
    }
}

pub fn execute_in_tokio<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    get_tokio_runtime().0.block_on(f())
}

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
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
        odbc_api::DataType::LongVarchar { length: _ } => Ok(DmType::Text),
        odbc_api::DataType::Integer => Ok(DmType::Integer),
        odbc_api::DataType::Char { length } => Ok(DmType::Char(length.map(|l| l.get() as u16))),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported DM type: {data_type:?}"
        ))),
    }
}
