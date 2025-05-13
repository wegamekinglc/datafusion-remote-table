#[cfg(feature = "dm")]
mod dm;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "dm")]
pub use dm::*;
#[cfg(feature = "mysql")]
pub use mysql::*;
#[cfg(feature = "oracle")]
pub use oracle::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

use crate::{DFResult, RemoteSchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect, SqliteDialect};
use std::fmt::Debug;
use std::sync::Arc;

#[cfg(feature = "dm")]
pub(crate) static ODBC_ENV: std::sync::OnceLock<odbc_api::Environment> = std::sync::OnceLock::new();

#[async_trait::async_trait]
pub trait Pool: Debug + Send + Sync {
    async fn get(&self) -> DFResult<Arc<dyn Connection>>;
}

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef>;

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream>;
}

pub async fn connect(options: &ConnectionOptions) -> DFResult<Arc<dyn Pool>> {
    match options {
        #[cfg(feature = "postgres")]
        ConnectionOptions::Postgres(options) => {
            let pool = connect_postgres(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "mysql")]
        ConnectionOptions::Mysql(options) => {
            let pool = connect_mysql(options)?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "oracle")]
        ConnectionOptions::Oracle(options) => {
            let pool = connect_oracle(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "sqlite")]
        ConnectionOptions::Sqlite(options) => {
            let pool = connect_sqlite(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "dm")]
        ConnectionOptions::Dm(options) => {
            let pool = connect_dm(options)?;
            Ok(Arc::new(pool))
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionOptions {
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnectionOptions),
    #[cfg(feature = "oracle")]
    Oracle(OracleConnectionOptions),
    #[cfg(feature = "mysql")]
    Mysql(MysqlConnectionOptions),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnectionOptions),
    #[cfg(feature = "dm")]
    Dm(DmConnectionOptions),
}

impl ConnectionOptions {
    pub(crate) fn stream_chunk_size(&self) -> usize {
        match self {
            #[cfg(feature = "postgres")]
            ConnectionOptions::Postgres(options) => options.stream_chunk_size,
            #[cfg(feature = "oracle")]
            ConnectionOptions::Oracle(options) => options.stream_chunk_size,
            #[cfg(feature = "mysql")]
            ConnectionOptions::Mysql(options) => options.stream_chunk_size,
            #[cfg(feature = "sqlite")]
            ConnectionOptions::Sqlite(options) => options.stream_chunk_size,
            #[cfg(feature = "dm")]
            ConnectionOptions::Dm(options) => options.stream_chunk_size,
        }
    }

    pub(crate) fn db_type(&self) -> RemoteDbType {
        match self {
            #[cfg(feature = "postgres")]
            ConnectionOptions::Postgres(_) => RemoteDbType::Postgres,
            #[cfg(feature = "oracle")]
            ConnectionOptions::Oracle(_) => RemoteDbType::Oracle,
            #[cfg(feature = "mysql")]
            ConnectionOptions::Mysql(_) => RemoteDbType::Mysql,
            #[cfg(feature = "sqlite")]
            ConnectionOptions::Sqlite(_) => RemoteDbType::Sqlite,
            #[cfg(feature = "dm")]
            ConnectionOptions::Dm(_) => RemoteDbType::Dm,
        }
    }
}

pub enum RemoteDbType {
    Postgres,
    Mysql,
    Oracle,
    Sqlite,
    Dm,
}

impl RemoteDbType {
    pub(crate) fn support_rewrite_with_filters_limit(&self, sql: &str) -> bool {
        sql.trim()[0..6].eq_ignore_ascii_case("select")
    }

    pub(crate) fn create_unparser(&self) -> DFResult<Unparser> {
        match self {
            RemoteDbType::Postgres => Ok(Unparser::new(&PostgreSqlDialect {})),
            RemoteDbType::Mysql => Ok(Unparser::new(&MySqlDialect {})),
            RemoteDbType::Sqlite => Ok(Unparser::new(&SqliteDialect {})),
            RemoteDbType::Oracle => Err(DataFusionError::NotImplemented(
                "Oracle unparser not implemented".to_string(),
            )),
            RemoteDbType::Dm => Err(DataFusionError::NotImplemented(
                "Dm unparser not implemented".to_string(),
            )),
        }
    }

    pub(crate) fn try_rewrite_query(
        &self,
        sql: &str,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Option<String> {
        if !self.support_rewrite_with_filters_limit(sql) {
            return None;
        }
        match self {
            RemoteDbType::Postgres | RemoteDbType::Mysql | RemoteDbType::Sqlite => {
                let where_clause = if filters.is_empty() {
                    "".to_string()
                } else {
                    let unparser = self.create_unparser().ok()?;
                    let filters_ast = filters
                        .iter()
                        .map(|f| unparser.expr_to_sql(f).expect("checked already"))
                        .collect::<Vec<_>>();
                    format!(
                        " WHERE {}",
                        filters_ast
                            .iter()
                            .map(|f| format!("{f}"))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )
                };
                let limit_clause = if let Some(limit) = limit {
                    format!(" LIMIT {limit}")
                } else {
                    "".to_string()
                };

                if where_clause.is_empty() && limit_clause.is_empty() {
                    None
                } else {
                    Some(format!(
                        "SELECT * FROM ({sql}) as __subquery{where_clause}{limit_clause}"
                    ))
                }
            }
            RemoteDbType::Oracle => {
                let limit = limit?;
                Some(format!("SELECT * FROM ({sql}) WHERE ROWNUM <= {limit}"))
            }
            RemoteDbType::Dm => {
                let limit = limit?;
                Some(format!("SELECT * FROM ({sql}) LIMIT {limit}"))
            }
        }
    }
}

pub(crate) fn projections_contains(projection: Option<&Vec<usize>>, col_idx: usize) -> bool {
    match projection {
        Some(p) => p.contains(&col_idx),
        None => true,
    }
}

pub(crate) fn project_batch(
    batch: RecordBatch,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    if let Some(projection) = projection {
        Ok(batch.project(projection)?)
    } else {
        Ok(batch)
    }
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "oracle"))]
fn big_decimal_to_i128(decimal: &bigdecimal::BigDecimal, scale: Option<i32>) -> Option<i128> {
    use bigdecimal::{FromPrimitive, ToPrimitive};
    let scale = scale.unwrap_or_else(|| {
        decimal
            .fractional_digit_count()
            .try_into()
            .unwrap_or_default()
    });
    let scale_decimal = bigdecimal::BigDecimal::from_f32(10f32.powi(scale))?;
    (decimal * scale_decimal).to_i128()
}
