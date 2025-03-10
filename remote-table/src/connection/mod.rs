mod mysql;
mod oracle;
mod postgres;
mod sqlite;

pub use mysql::*;
pub use oracle::*;
pub use postgres::*;

use crate::{DFResult, RemoteSchema, Transform};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use std::fmt::Debug;
use std::path::PathBuf;

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<&dyn Transform>,
    ) -> DFResult<(RemoteSchema, SchemaRef)>;

    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, RemoteSchema)>;
}

pub async fn connect(options: &ConnectionOptions) -> DFResult<Box<dyn Connection>> {
    match options {
        ConnectionOptions::Postgres(options) => {
            let conn = connect_postgres(options).await?;
            Ok(Box::new(conn))
        }
        ConnectionOptions::Oracle(_options) => {
            todo!()
        }
        ConnectionOptions::Mysql(_options) => {
            todo!()
        }
        ConnectionOptions::Sqlite(_) => {
            todo!()
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionOptions {
    Postgres(PostgresConnectionOptions),
    Oracle(OracleConnectionOptions),
    Mysql(MysqlConnectionOptions),
    Sqlite(PathBuf),
}

pub(crate) fn projections_contains(projection: Option<&Vec<usize>>, col_idx: usize) -> bool {
    match projection {
        Some(p) => p.contains(&col_idx),
        None => true,
    }
}
