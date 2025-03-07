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
        Type::FLOAT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float4Array)),
        Type::FLOAT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float8Array)),
        Type::TEXT_ARRAY => Ok(RemoteType::Postgres(PostgresType::TextArray)),
        Type::VARCHAR_ARRAY => Ok(RemoteType::Postgres(PostgresType::VarcharArray)),
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

macro_rules! handle_primitive_type {
    ($builder:expr, $pg_type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .expect(concat!(
                "Failed to downcast builder to ",
                stringify!($builder_ty),
                " for ",
                stringify!($pg_type)
            ));
        let v: Option<$value_ty> = $row.try_get($index).expect(concat!(
            "Failed to get ",
            stringify!($value_ty),
            " value for column ",
            stringify!($pg_type)
        ));

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

macro_rules! handle_primitive_array_type {
    ($builder:expr, $pg_type:expr, $values_builder_ty:ty, $primitive_value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .expect(concat!(
                "Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for ",
                stringify!($pg_type)
            ));
        let values_builder = builder
            .values()
            .as_any_mut()
            .downcast_mut::<$values_builder_ty>()
            .expect(concat!(
                "Failed to downcast values builder to ",
                stringify!($values_builder_ty),
                " for ",
                stringify!($pg_type)
            ));
        let v: Option<Vec<$primitive_value_ty>> = $row.try_get($index).expect(concat!(
            "Failed to get ",
            stringify!($primitive_value_ty),
            " array value for column ",
            stringify!($pg_type)
        ));

        match v {
            Some(v) => {
                let v = v.into_iter().map(Some);
                values_builder.extend(v);
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }};
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
            let builder = &mut array_builders[idx];
            match *pg_type {
                Type::BOOL => {
                    handle_primitive_type!(builder, Type::BOOL, BooleanBuilder, bool, row, idx);
                }
                Type::CHAR => {
                    handle_primitive_type!(builder, Type::CHAR, Int8Builder, i8, row, idx);
                }
                Type::INT2 => {
                    handle_primitive_type!(builder, Type::INT2, Int16Builder, i16, row, idx);
                }
                Type::INT4 => {
                    handle_primitive_type!(builder, Type::INT4, Int32Builder, i32, row, idx);
                }
                Type::INT8 => {
                    handle_primitive_type!(builder, Type::INT8, Int64Builder, i64, row, idx);
                }
                Type::FLOAT4 => {
                    handle_primitive_type!(builder, Type::FLOAT4, Float32Builder, f32, row, idx);
                }
                Type::FLOAT8 => {
                    handle_primitive_type!(builder, Type::FLOAT8, Float64Builder, f64, row, idx);
                }
                Type::TEXT => {
                    handle_primitive_type!(builder, Type::TEXT, StringBuilder, &str, row, idx);
                }
                Type::VARCHAR => {
                    handle_primitive_type!(builder, Type::VARCHAR, StringBuilder, &str, row, idx);
                }
                Type::BYTEA => {
                    handle_primitive_type!(builder, Type::BYTEA, BinaryBuilder, &[u8], row, idx);
                }
                Type::INT2_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT2_ARRAY,
                        Int16Builder,
                        i16,
                        row,
                        idx
                    );
                }
                Type::INT4_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT4_ARRAY,
                        Int32Builder,
                        i32,
                        row,
                        idx
                    );
                }
                Type::INT8_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT8_ARRAY,
                        Int64Builder,
                        i64,
                        row,
                        idx
                    );
                }
                Type::FLOAT4_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::FLOAT4_ARRAY,
                        Float32Builder,
                        f32,
                        row,
                        idx
                    );
                }
                Type::FLOAT8_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::FLOAT8_ARRAY,
                        Float64Builder,
                        f64,
                        row,
                        idx
                    );
                }
                Type::TEXT_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::TEXT_ARRAY,
                        StringBuilder,
                        &str,
                        row,
                        idx
                    );
                }
                Type::VARCHAR_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::VARCHAR_ARRAY,
                        StringBuilder,
                        &str,
                        row,
                        idx
                    );
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
