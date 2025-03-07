mod postgres;

use crate::connection::postgres::connect_postgres;
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

pub async fn connect(args: &ConnectionArgs) -> DFResult<Box<dyn Connection>> {
    match args {
        ConnectionArgs::Postgresql {
            host,
            port,
            username,
            password,
            database,
        } => {
            let conn =
                connect_postgres(host, *port, username, password, database.as_deref()).await?;
            Ok(Box::new(conn))
        }
        ConnectionArgs::Oracle { .. } => {
            todo!()
        }
        ConnectionArgs::Mysql { .. } => {
            todo!()
        }
        ConnectionArgs::Sqlite(_) => {
            todo!()
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionArgs {
    Postgresql {
        host: String,
        port: u16,
        username: String,
        password: String,
        database: Option<String>,
    },
    Oracle {
        host: String,
        port: u16,
        username: String,
        password: String,
        database: Option<String>,
    },
    Mysql {
        host: String,
        port: u16,
        username: String,
        password: String,
        database: Option<String>,
    },
    Sqlite(PathBuf),
}

pub(crate) fn projections_contains(projection: Option<&Vec<usize>>, col_idx: usize) -> bool {
    match projection {
        Some(p) => p.contains(&col_idx),
        None => true,
    }
}
