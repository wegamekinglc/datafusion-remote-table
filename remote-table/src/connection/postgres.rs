use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    Connection, DFResult, Pool, PostgresType, RemoteField, RemoteSchema, RemoteType, Transform,
};
use bb8_postgres::tokio_postgres::types::{FromSql, Type};
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bb8_postgres::PostgresConnectionManager;
use chrono::Timelike;
use datafusion::arrow::array::{
    make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    ListBuilder, RecordBatch, StringBuilder, Time64NanosecondBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{Date32Type, Schema, SchemaRef};
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
use std::string::ToString;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PostgresConnectionOptions {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

#[derive(Debug)]
pub struct PostgresPool {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Pool for PostgresPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get postgres connection due to {e:?}"))
        })?;
        Ok(Arc::new(PostgresConnection { conn }))
    }
}

pub(crate) async fn connect_postgres(
    options: &PostgresConnectionOptions,
) -> DFResult<PostgresPool> {
    let mut config = bb8_postgres::tokio_postgres::config::Config::new();
    config
        .host(&options.host)
        .port(options.port)
        .user(&options.username)
        .password(&options.password);
    if let Some(database) = &options.database {
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

    Ok(PostgresPool { pool })
}

#[derive(Debug)]
pub(crate) struct PostgresConnection {
    conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<&dyn Transform>,
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
        let mut stream = self
            .conn
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
        let mut stream = self
            .conn
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
    match pg_type {
        &Type::BOOL => Ok(RemoteType::Postgres(PostgresType::Bool)),
        &Type::CHAR => Ok(RemoteType::Postgres(PostgresType::Char)),
        &Type::INT2 => Ok(RemoteType::Postgres(PostgresType::Int2)),
        &Type::INT4 => Ok(RemoteType::Postgres(PostgresType::Int4)),
        &Type::INT8 => Ok(RemoteType::Postgres(PostgresType::Int8)),
        &Type::FLOAT4 => Ok(RemoteType::Postgres(PostgresType::Float4)),
        &Type::FLOAT8 => Ok(RemoteType::Postgres(PostgresType::Float8)),
        &Type::TEXT => Ok(RemoteType::Postgres(PostgresType::Text)),
        &Type::VARCHAR => Ok(RemoteType::Postgres(PostgresType::Varchar)),
        &Type::BPCHAR => Ok(RemoteType::Postgres(PostgresType::Bpchar)),
        &Type::BYTEA => Ok(RemoteType::Postgres(PostgresType::Bytea)),
        &Type::DATE => Ok(RemoteType::Postgres(PostgresType::Date)),
        &Type::TIMESTAMP => Ok(RemoteType::Postgres(PostgresType::Timestamp)),
        &Type::TIMESTAMPTZ => Ok(RemoteType::Postgres(PostgresType::TimestampTz)),
        &Type::TIME => Ok(RemoteType::Postgres(PostgresType::Time)),
        &Type::INT2_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int2Array)),
        &Type::INT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int4Array)),
        &Type::INT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Int8Array)),
        &Type::FLOAT4_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float4Array)),
        &Type::FLOAT8_ARRAY => Ok(RemoteType::Postgres(PostgresType::Float8Array)),
        &Type::TEXT_ARRAY => Ok(RemoteType::Postgres(PostgresType::TextArray)),
        &Type::VARCHAR_ARRAY => Ok(RemoteType::Postgres(PostgresType::VarcharArray)),
        &Type::BYTEA_ARRAY => Ok(RemoteType::Postgres(PostgresType::ByteaArray)),
        other if other.name().eq_ignore_ascii_case("geometry") => {
            Ok(RemoteType::Postgres(PostgresType::PostGisGeometry))
        }
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

pub struct GeometryFromSql<'a> {
    wkb: &'a [u8],
}

impl<'a> FromSql<'a> for GeometryFromSql<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GeometryFromSql { wkb: raw })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "geometry")
    }
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
            match pg_type {
                &Type::BOOL => {
                    handle_primitive_type!(builder, Type::BOOL, BooleanBuilder, bool, row, idx);
                }
                &Type::CHAR => {
                    handle_primitive_type!(builder, Type::CHAR, Int8Builder, i8, row, idx);
                }
                &Type::INT2 => {
                    handle_primitive_type!(builder, Type::INT2, Int16Builder, i16, row, idx);
                }
                &Type::INT4 => {
                    handle_primitive_type!(builder, Type::INT4, Int32Builder, i32, row, idx);
                }
                &Type::INT8 => {
                    handle_primitive_type!(builder, Type::INT8, Int64Builder, i64, row, idx);
                }
                &Type::FLOAT4 => {
                    handle_primitive_type!(builder, Type::FLOAT4, Float32Builder, f32, row, idx);
                }
                &Type::FLOAT8 => {
                    handle_primitive_type!(builder, Type::FLOAT8, Float64Builder, f64, row, idx);
                }
                &Type::TEXT => {
                    handle_primitive_type!(builder, Type::TEXT, StringBuilder, &str, row, idx);
                }
                &Type::VARCHAR => {
                    handle_primitive_type!(builder, Type::VARCHAR, StringBuilder, &str, row, idx);
                }
                &Type::BPCHAR => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Failed to downcast builder to StringBuilder for Type::BPCHAR");
                    let v: Option<&str> = row
                        .try_get(idx)
                        .expect("Failed to get &str value for column Type::BPCHAR");

                    match v {
                        Some(v) => builder.append_value(v.trim_end()),
                        None => builder.append_null(),
                    }
                }
                &Type::BYTEA => {
                    handle_primitive_type!(builder, Type::BYTEA, BinaryBuilder, Vec<u8>, row, idx);
                }
                &Type::TIMESTAMP => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .expect("Failed to downcast builder to TimestampNanosecondBuilder for Type::TIMESTAMP");
                    let v: Option<SystemTime> = row
                        .try_get(idx)
                        .expect("Failed to get SystemTime value for column Type::TIMESTAMP");

                    match v {
                        Some(v) => {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_nanos()
                                    .try_into()
                                    .expect("Failed to convert SystemTime to i64");
                                builder.append_value(timestamp);
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                &Type::TIMESTAMPTZ => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .expect("Failed to downcast builder to TimestampNanosecondBuilder for Type::TIMESTAMP");
                    let v: Option<chrono::DateTime<chrono::Utc>> = row.try_get(idx).expect(
                        "Failed to get chrono::DateTime<chrono::Utc> value for column Type::TIMESTAMPTZ",
                    );

                    match v {
                        Some(v) => {
                            let timestamp: i64 = v.timestamp_nanos_opt().expect(&format!("Failed to get timestamp in nanoseconds from {v} for Type::TIMESTAMP"));
                            builder.append_value(timestamp);
                        }
                        None => {}
                    }
                }
                &Type::TIME => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                        .expect(
                            "Failed to downcast builder to Time64NanosecondBuilder for Type::TIME",
                        );
                    let v: Option<chrono::NaiveTime> = row
                        .try_get(idx)
                        .expect("Failed to get chrono::NaiveTime value for column Type::TIME");

                    match v {
                        Some(v) => {
                            let timestamp: i64 = i64::from(v.num_seconds_from_midnight())
                                * 1_000_000_000
                                + i64::from(v.nanosecond());
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                &Type::DATE => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .expect("Failed to downcast builder to Date32Builder for Type::DATE");
                    let v: Option<chrono::NaiveDate> = row
                        .try_get(idx)
                        .expect("Failed to get chrono::NaiveDate value for column Type::DATE");

                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                &Type::INT2_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT2_ARRAY,
                        Int16Builder,
                        i16,
                        row,
                        idx
                    );
                }
                &Type::INT4_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT4_ARRAY,
                        Int32Builder,
                        i32,
                        row,
                        idx
                    );
                }
                &Type::INT8_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::INT8_ARRAY,
                        Int64Builder,
                        i64,
                        row,
                        idx
                    );
                }
                &Type::FLOAT4_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::FLOAT4_ARRAY,
                        Float32Builder,
                        f32,
                        row,
                        idx
                    );
                }
                &Type::FLOAT8_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::FLOAT8_ARRAY,
                        Float64Builder,
                        f64,
                        row,
                        idx
                    );
                }
                &Type::TEXT_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::TEXT_ARRAY,
                        StringBuilder,
                        &str,
                        row,
                        idx
                    );
                }
                &Type::VARCHAR_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::VARCHAR_ARRAY,
                        StringBuilder,
                        &str,
                        row,
                        idx
                    );
                }
                &Type::BYTEA_ARRAY => {
                    handle_primitive_array_type!(
                        builder,
                        Type::BYTEA_ARRAY,
                        BinaryBuilder,
                        Vec<u8>,
                        row,
                        idx
                    );
                }
                other if other.name().eq_ignore_ascii_case("geometry") => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .expect("Failed to downcast builder to BinaryBuilder for Type::geometry");
                    let v: Option<GeometryFromSql> = row
                        .try_get(idx)
                        .expect("Failed to get GeometryFromSql value for column Type::geometry");

                    match v {
                        Some(v) => builder.append_value(v.wkb),
                        None => builder.append_null(),
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
