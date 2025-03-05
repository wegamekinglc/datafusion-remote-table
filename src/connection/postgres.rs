use crate::{Connection, DFResult, RemoteDataType};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use std::time::Duration;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;

#[derive(Debug)]
pub struct PostgresConnection {
    conn: PoolConnection<Postgres>,
}

pub(crate) async fn connect_postgres(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database: Option<&str>,
) -> DFResult<PostgresConnection> {
    let mut opts = sqlx::postgres::PgConnectOptions::new()
        .host(host)
        .port(port)
        .username(username)
        .password(password);
    if let Some(database) = database {
        opts = opts.database(database);
    }
    let mut pool_option = sqlx::pool::PoolOptions::new();
    pool_option = pool_option
        .acquire_timeout(Duration::from_secs(60))
        .test_before_acquire(true)
        .max_connections(10)
        .min_connections(1);
    let pool = pool_option.connect_with(opts).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to connect to postgres host={}, port={}, username={}, due to {}",
            host,
            port,
            username,
            e.to_string()
        ))
    })?;
    let conn = pool.acquire().await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to acquire connection from postgres pool host={}, port={}, username={}, due to {}",
            host,
            port,
            username,
            e.to_string()
        ))
    })?;
    Ok(PostgresConnection { conn })
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, Vec<RemoteDataType>)> {

        todo!()
    }
}
