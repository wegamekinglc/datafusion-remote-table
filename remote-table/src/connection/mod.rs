mod mysql;
mod oracle;
mod postgres;
mod sqlite;

pub use mysql::*;
pub use oracle::*;
pub use postgres::*;

use crate::connection::sqlite::connect_sqlite;
use crate::{DFResult, RemoteSchemaRef, Transform};
use bigdecimal::{FromPrimitive, ToPrimitive};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait Pool: Debug + Send + Sync {
    async fn get(&self) -> DFResult<Arc<dyn Connection>>;
}

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchemaRef, SchemaRef)>;

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<SendableRecordBatchStream>;
}

pub async fn connect(options: &ConnectionOptions) -> DFResult<Arc<dyn Pool>> {
    match options {
        ConnectionOptions::Postgres(options) => {
            let pool = connect_postgres(options).await?;
            Ok(Arc::new(pool))
        }
        ConnectionOptions::Mysql(options) => {
            let pool = connect_mysql(options)?;
            Ok(Arc::new(pool))
        }
        ConnectionOptions::Oracle(options) => {
            let pool = connect_oracle(options).await?;
            Ok(Arc::new(pool))
        }
        ConnectionOptions::Sqlite(path) => {
            let pool = connect_sqlite(path).await?;
            Ok(Arc::new(pool))
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

impl ConnectionOptions {
    pub fn chunk_size(&self) -> Option<usize> {
        match self {
            ConnectionOptions::Postgres(options) => options.chunk_size,
            ConnectionOptions::Oracle(options) => options.chunk_size,
            ConnectionOptions::Mysql(options) => options.chunk_size,
            ConnectionOptions::Sqlite(_) => None,
        }
    }
}

pub(crate) fn projections_contains(projection: Option<&Vec<usize>>, col_idx: usize) -> bool {
    match projection {
        Some(p) => p.contains(&col_idx),
        None => true,
    }
}

fn big_decimal_to_i128(decimal: &bigdecimal::BigDecimal, scale: Option<i32>) -> Option<i128> {
    let scale = scale.unwrap_or_else(|| {
        decimal
            .fractional_digit_count()
            .try_into()
            .unwrap_or_default()
    });
    let scale_decimal = bigdecimal::BigDecimal::from_f32(10f32.powi(scale))?;
    (decimal * scale_decimal).to_i128()
}
