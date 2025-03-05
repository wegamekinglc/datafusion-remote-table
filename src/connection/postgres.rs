use crate::{Connection, DFResult, RemoteDataType, RemoteField, RemoteSchema};
use bb8_postgres::tokio_postgres::types::Type;
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bb8_postgres::PostgresConnectionManager;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt, TryStreamExt};
use std::sync::Arc;

#[derive(Debug)]
pub struct PostgresConnection {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

pub(crate) async fn connect_postgres(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database: Option<&str>,
) -> DFResult<PostgresConnection> {
    let mut config = bb8_postgres::tokio_postgres::config::Config::new();
    config
        .host(host)
        .port(port)
        .user(username)
        .password(password);
    if let Some(database) = database {
        config.dbname(database);
    }
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = bb8::Pool::builder()
        .max_size(5)
        .build(manager)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create postgres connection pool due to {}",
                e.to_string()
            ))
        })?;

    Ok(PostgresConnection { pool })
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, RemoteSchema)> {
        let conn = self.pool.get().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get connection from postgres connection pool due to {}",
                e.to_string()
            ))
        })?;
        let mut stream = conn
            .query_raw(&sql, &[""])
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query on postgres due to {}",
                    e.to_string()
                ))
            })?
            .chunks(2048)
            .boxed();

        let Some(first_chunk) = stream.next().await else {
            return Ok((
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    stream::empty(),
                )),
                RemoteSchema::empty(),
            ));
        };
        let first_chunk: Vec<Row> = first_chunk
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to collect rows from postgres due to {}",
                    e.to_string()
                ))
            })?;
        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let remote_schema = build_remote_schema(first_row)?;
        let first_chunk = rows_to_batch(first_chunk.as_slice(), projection.as_ref())?;
        let schema = first_chunk.schema();

        let mut stream = stream.map(move |rows| {
            let rows: Vec<Row> = rows
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to collect rows from postgres due to {}",
                        e.to_string()
                    ))
                })?;
            let batch = rows_to_batch(rows.as_slice(), projection.as_ref())?;
            Ok::<RecordBatch, DataFusionError>(batch)
        });

        let output_stream = async_stream::stream! {
           yield Ok(first_chunk);
           while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        yield Ok(batch); // we can yield the batch as-is because we've already converted to Arrow in the chunk map
                    }
                    Err(e) => {
                        yield Err(DataFusionError::Execution(format!("Failed to fetch batch: {e}")));
                    }
                }
           }
        };

        Ok((
            Box::pin(RecordBatchStreamAdapter::new(schema, output_stream)),
            remote_schema,
        ))
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in row.columns() {
        let col_type = col.type_();
        if *col_type == Type::BOOL {
            remote_fields.push(RemoteField::new(
                col.name().to_string(),
                RemoteDataType::Boolean,
            ));
        } else {
            return Err(DataFusionError::Execution(format!(
                "Unsupported type {:?} in postgres",
                col_type
            )));
        }
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn rows_to_batch(rows: &[Row], projection: Option<&Vec<usize>>) -> DFResult<RecordBatch> {
    todo!()
}
