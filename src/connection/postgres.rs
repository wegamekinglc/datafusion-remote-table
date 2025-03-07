use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{Connection, DFResult, PostgresType, RemoteField, RemoteSchema, RemoteType, Transform};
use bb8_postgres::tokio_postgres::types::Type;
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bb8_postgres::PostgresConnectionManager;
use datafusion::arrow::array::{
    make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder,
    RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
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
                "Failed to create postgres connection pool due to {e}",
            ))
        })?;

    Ok(PostgresConnection { pool })
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<&dyn Transform>,
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
        let conn = self.pool.get().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get connection from postgres connection pool due to {e}",
            ))
        })?;
        let mut stream = conn
            .query_raw(sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on postgres due to {e}",
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
                    "Failed to collect rows from postgres due to {e}",
                ))
            })?;
        let Some(first_row) = first_chunk.first() else {
            return Err(DataFusionError::Execution(
                "No data returned from postgres".to_string(),
            ));
        };
        let (remote_schema, pg_types) = build_remote_schema(first_row)?;
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        let batch = rows_to_batch(
            std::slice::from_ref(first_row),
            &pg_types,
            arrow_schema,
            None,
        )?;
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
                "Failed to get connection from postgres connection pool due to {e}",
            ))
        })?;
        let mut stream = conn
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on postgres due to {e}",
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
                    "Failed to collect rows from postgres due to {e}",
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
                        "Failed to collect rows from postgres due to {e}",
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

fn pg_type_to_remote_type(pg_type: &Type) -> DFResult<RemoteType> {
    match *pg_type {
        Type::BOOL => Ok(RemoteType::Postgres(PostgresType::Bool)),
        Type::CHAR => Ok(RemoteType::Postgres(PostgresType::Char)),
        Type::INT2 => Ok(RemoteType::Postgres(PostgresType::Int2)),
        Type::INT4 => Ok(RemoteType::Postgres(PostgresType::Int4)),
        Type::INT8 => Ok(RemoteType::Postgres(PostgresType::Int8)),
        Type::FLOAT4 => Ok(RemoteType::Postgres(PostgresType::Float4)),
        Type::FLOAT8 => Ok(RemoteType::Postgres(PostgresType::Float8)),
        Type::TEXT => Ok(RemoteType::Postgres(PostgresType::Text)),
        Type::VARCHAR => Ok(RemoteType::Postgres(PostgresType::Varchar)),
        Type::BYTEA => Ok(RemoteType::Postgres(PostgresType::Bytea)),
        Type::INT2_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int2Array)),
        Type::INT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int4Array)),
        Type::INT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int8Array)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported postgres type {pg_type:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<(RemoteSchema, Vec<Type>)> {
    let mut remote_fields = vec![];
    let mut pg_types = vec![];
    for col in row.columns() {
        let col_type = col.type_();
        pg_types.push(col_type.clone());
        remote_fields.push(RemoteField::new(
            col.name(),
            pg_type_to_remote_type(col_type)?,
            true,
        ));
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
                // TODO use macro to simplify
                Type::BOOL => {
                    let value: Option<bool> = row.try_get(idx).expect(&format!(
                        "Failed to get bool value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .expect("Failed to downcast builder to BooleanBuilder for BOOL");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::CHAR => {
                    let value: Option<i8> = row.try_get(idx).expect(&format!(
                        "Failed to get i8 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Int8Builder>()
                        .expect("Failed to downcast builder to Int8Builder for CHAR");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::INT2 => {
                    let value: Option<i16> = row.try_get(idx).expect(&format!(
                        "Failed to get i16 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Int16Builder>()
                        .expect("Failed to downcast builder to Int16Builder for INT2");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::INT4 => {
                    let value: Option<i32> = row.try_get(idx).expect(&format!(
                        "Failed to get i32 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .expect("Failed to downcast builder to Int32Builder for INT4");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::INT8 => {
                    let value: Option<i64> = row.try_get(idx).expect(&format!(
                        "Failed to get i64 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .expect("Failed to downcast builder to Int64Builder for INT8");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::FLOAT4 => {
                    let value: Option<f32> = row.try_get(idx).expect(&format!(
                        "Failed to get f32 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .expect("Failed to downcast builder to Float32Builder for FLOAT4");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::FLOAT8 => {
                    let value: Option<f64> = row.try_get(idx).expect(&format!(
                        "Failed to get f64 value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .expect("Failed to downcast builder to Float64Builder for FLOAT8");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::TEXT => {
                    let value: Option<&str> = row.try_get(idx).expect(&format!(
                        "Failed to get &str value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Failed to downcast builder to StringBuilder for TEXT");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(&v),
                    }
                }
                Type::VARCHAR => {
                    let value: Option<&str> = row.try_get(idx).expect(&format!(
                        "Failed to get &str value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Failed to downcast builder to StringBuilder for VARCHAR");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::BYTEA => {
                    let value: Option<&[u8]> = row.try_get(idx).expect(&format!(
                        "Failed to get &[u8] value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .expect("Failed to downcast builder to BinaryBuilder for BYTEA");
                    match value {
                        None => builder.append_null(),
                        Some(v) => builder.append_value(v),
                    }
                }
                Type::INT2_ARRAY => {
                    let value: Option<Vec<i16>> = row.try_get(idx).expect(&format!(
                        "Failed to get i16 array value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                        .expect("Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for INT2_ARRAY");
                    let values_builder = builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int16Builder>()
                        .expect("Failed to downcast values builder to Int16Builder for INT2_ARRAY");
                    match value {
                        None => builder.append_null(),
                        Some(v) => {
                            let v = v.into_iter().map(Some);
                            values_builder.extend(v);
                            builder.append(true);
                        }
                    }
                }
                Type::INT4_ARRAY => {
                    let value: Option<Vec<i32>> = row.try_get(idx).expect(&format!(
                        "Failed to get i32 array value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                        .expect("Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for INT4_ARRAY");
                    let values_builder = builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .expect("Failed to downcast values builder to Int32Builder for INT4_ARRAY");
                    match value {
                        None => builder.append_null(),
                        Some(v) => {
                            let v = v.into_iter().map(Some);
                            values_builder.extend(v);
                            builder.append(true);
                        }
                    }
                }
                Type::INT8_ARRAY => {
                    let value: Option<Vec<i64>> = row.try_get(idx).expect(&format!(
                        "Failed to get i64 array value for column {idx} from row: {row:?}",
                    ));
                    let builder = array_builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                        .expect("Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for INT8_ARRAY");
                    let values_builder = builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .expect("Failed to downcast values builder to Int64Builder for INT8_ARRAY");
                    match value {
                        None => builder.append_null(),
                        Some(v) => {
                            let v = v.into_iter().map(Some);
                            values_builder.extend(v);
                            builder.append(true);
                        }
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported postgres type {pg_type:?}",
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
