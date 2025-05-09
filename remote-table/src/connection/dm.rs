use crate::connection::ODBC_ENV;
use crate::{
    Connection, ConnectionOptions, DFResult, DmType, Pool, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use derive_getters::Getters;
use derive_with::With;
use odbc_api::handles::StatementImpl;
use odbc_api::{CursorImpl, Environment, ResultSetMetadata};
use std::sync::{Arc, Mutex};

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
            connection: Mutex::new(connection),
        }))
    }
}

#[derive(Debug)]
pub struct DmConnection {
    connection: Mutex<odbc_api::Connection<'static>>,
}

#[async_trait::async_trait]
impl Connection for DmConnection {
    async fn infer_schema(&self, sql: &str) -> DFResult<(RemoteSchemaRef, SchemaRef)> {
        let conn = self.connection.lock().unwrap();
        let cursor_opt = conn
            .execute(sql, (), None)
            .map_err(|e| DataFusionError::Execution(format!("Failed to infer schema: {e:?}")))?;
        match cursor_opt {
            None => Err(DataFusionError::Execution(
                "No rows returned to infer schema".to_string(),
            )),
            Some(cursor) => {
                let remote_schema = Arc::new(build_remote_schema(cursor)?);
                let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
                Ok((remote_schema, arrow_schema))
            }
        }
    }

    async fn query(
        &self,
        _conn_options: &ConnectionOptions,
        _sql: &str,
        _table_schema: SchemaRef,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        todo!()
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
        odbc_api::DataType::LongVarbinary { length: _ } => Ok(DmType::Text),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported DM type: {data_type:?}"
        ))),
    }
}
