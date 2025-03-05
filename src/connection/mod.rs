mod postgres;

use crate::connection::postgres::connect_postgres;
use crate::{DFResult, RemoteDataType, Transform};
use datafusion::execution::SendableRecordBatchStream;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, Vec<RemoteDataType>)>;
}

pub async fn connect(args: &ConnectionArgs) -> DFResult<Arc<dyn Connection>> {
    match args {
        ConnectionArgs::Postgresql {
            host,
            port,
            username,
            password,
            database,
        } => {
            let conn = connect_postgres(host, *port, username, password, database.as_deref()).await?;
            Ok(Arc::new(conn))
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
