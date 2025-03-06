use crate::connection::projections_contains;
use crate::{Connection, DFResult, RemoteDataType, RemoteField, RemoteSchema, Transform};
use bb8_postgres::tokio_postgres::types::Type;
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bb8_postgres::PostgresConnectionManager;
use datafusion::arrow::array::{make_builder, ArrayBuilder, ArrayRef, BooleanBuilder, RecordBatch};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
use std::sync::Arc;
use crate::transform::transform_batch;

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
    async fn infer_schema(&self, sql: &str, transform: Option<&dyn Transform>) -> DFResult<(RemoteSchema, SchemaRef)> {
        let conn = self.pool.get().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get connection from postgres connection pool due to {}",
                e.to_string()
            ))
        })?;
        let mut stream = conn
            .query_raw(sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {} on postgres due to {}",
                    sql,
                    e.to_string()
                ))
            })?
            .chunks(1)
            .boxed();

        let Some(first_chunk) = stream.next().await else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
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
        let (remote_schema, pg_types) = build_remote_schema(first_row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let batch = rows_to_batch(std::slice::from_ref(first_row), &pg_types, arrow_schema, None)?;
        if let Some(transform) = transform {
            let transformed_batch = transform_batch(batch, transform, &remote_schema)?;
            Ok((remote_schema, transformed_batch.schema()))
        } else {
            Ok((remote_schema, batch.schema()))
        }
    }

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
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {} on postgres due to {}",
                    sql,
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
        let (remote_schema, pg_types) = build_remote_schema(first_row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let first_chunk = rows_to_batch(
            first_chunk.as_slice(),
            &pg_types,
            arrow_schema.clone(),
            projection.as_ref(),
        )?;
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
            let batch = rows_to_batch(
                rows.as_slice(),
                &pg_types,
                arrow_schema.clone(),
                projection.as_ref(),
            )?;
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

fn build_remote_schema(row: &Row) -> DFResult<(RemoteSchema, Vec<Type>)> {
    let mut remote_fields = vec![];
    let mut pg_types = vec![];
    for col in row.columns() {
        let col_type = col.type_();
        pg_types.push(col_type.clone());
        if *col_type == Type::BOOL {
            remote_fields.push(RemoteField::new(
                col.name().to_string(),
                RemoteDataType::Boolean,
                true,
            ));
        } else {
            return Err(DataFusionError::Execution(format!(
                "Unsupported type {:?} in postgres",
                col_type
            )));
        }
    }
    Ok((RemoteSchema::new(remote_fields), pg_types))
}

fn rows_to_batch(
    rows: &[Row],
    pg_types: &Vec<Type>,
    arrow_schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(&arrow_schema, projection)?;
    let mut array_builders = vec![];
    for field in arrow_schema.fields() {
        let builder = make_builder(&field.data_type(), rows.len());
        array_builders.push(builder);
    }
    for row in rows {
        for (idx, pg_type) in pg_types.iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let array_builder = &mut array_builders[idx];
            match *pg_type {
                Type::BOOL => {
                    let value: Option<bool> = row.try_get(idx).expect(&format!(
                        "Failed to get bool value for column {} from row: {:?}",
                        idx, row
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .expect(&format!(
                            "Failed to downcast builder to BooleanBuilder for column {}",
                            idx
                        ));
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported type {:?} in postgres",
                        pg_type
                    )));
                }
            }
        }
    }
    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}
