use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    project_remote_schema, Connection, DFResult, Pool, RemoteField, RemoteSchema, RemoteType,
    SqliteType, Transform,
};
use datafusion::arrow::array::{
    make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, NullBuilder,
    RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use rusqlite::types::Type;
use rusqlite::{Row, Rows};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
pub struct SqlitePool {
    pool: tokio_rusqlite::Connection,
}

pub async fn connect_sqlite(path: &PathBuf) -> DFResult<SqlitePool> {
    let pool = tokio_rusqlite::Connection::open(path).await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to open sqlite connection: {e:?}"))
    })?;
    Ok(SqlitePool { pool })
}

#[async_trait::async_trait]
impl Pool for SqlitePool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.clone();
        Ok(Arc::new(SqliteConnection { conn }))
    }
}

#[derive(Debug)]
pub struct SqliteConnection {
    conn: tokio_rusqlite::Connection,
}

#[async_trait::async_trait]
impl Connection for SqliteConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchema, SchemaRef)> {
        let sql = sql.to_string();
        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let col_names = stmt
                    .column_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                let mut rows = stmt.query([])?;
                let Some(first_row) = rows.next()? else {
                    return Err(tokio_rusqlite::Error::Other(Box::new(
                        DataFusionError::Execution("No data returned from sqlite".to_string()),
                    )));
                };

                let remote_schema = build_remote_schema(first_row, col_names.as_slice());
                let arrow_schema = Arc::new(remote_schema.to_arrow_schema());

                if let Some(transform) = transform {
                    let batch = row_to_batch(first_row, arrow_schema)
                        .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                    let transformed_batch =
                        transform_batch(batch, transform.as_ref(), &remote_schema)
                            .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                    Ok((remote_schema, transformed_batch.schema()))
                } else {
                    Ok((remote_schema, arrow_schema))
                }
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to infer schema: {e:?}")))
    }

    async fn query(
        &self,
        sql: String,
        projection: Option<Vec<usize>>,
    ) -> DFResult<(SendableRecordBatchStream, RemoteSchema)> {
        let projection_clone = projection.clone();
        let (first_batch, batch, remote_schema) = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let col_names = stmt
                    .column_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                let mut rows = stmt.query([])?;

                let Some(first_row) = rows.next()? else {
                    return Err(tokio_rusqlite::Error::Other(Box::new(
                        DataFusionError::Execution("No data returned from sqlite".to_string()),
                    )));
                };

                let remote_schema = build_remote_schema(first_row, col_names.as_slice());
                let arrow_schema = Arc::new(remote_schema.to_arrow_schema());

                let first_batch = row_to_batch(first_row, arrow_schema.clone())
                    .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                let batch = rows_to_batch(rows, arrow_schema, projection_clone.as_ref())
                    .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                Ok((first_batch, batch, remote_schema))
            })
            .await
            .unwrap();
        let schema = first_batch.schema();
        Ok((
            Box::pin(MemoryStream::try_new(
                vec![first_batch, batch],
                schema,
                None,
            )?),
            project_remote_schema(&remote_schema, projection.as_ref()),
        ))
    }
}

fn sqlite_type_to_remote_type(sqlite_type: Type) -> RemoteType {
    match sqlite_type {
        Type::Null => RemoteType::Sqlite(SqliteType::Null),
        Type::Integer => RemoteType::Sqlite(SqliteType::Integer),
        Type::Real => RemoteType::Sqlite(SqliteType::Real),
        Type::Text => RemoteType::Sqlite(SqliteType::Text),
        Type::Blob => RemoteType::Sqlite(SqliteType::Blob),
    }
}

fn build_remote_schema(row: &Row, col_names: &[String]) -> RemoteSchema {
    let mut remote_fields = Vec::with_capacity(col_names.len());
    for (i, col_name) in col_names.iter().enumerate() {
        let remote_type = sqlite_type_to_remote_type(
            row.get_ref(i)
                .expect("Failed to get sqlite value ref")
                .data_type(),
        );
        remote_fields.push(RemoteField::new(col_name, remote_type, false));
    }
    RemoteSchema::new(remote_fields)
}

fn row_to_batch(row: &Row, arrow_schema: SchemaRef) -> DFResult<RecordBatch> {
    let mut array_builders = vec![];
    for field in arrow_schema.fields() {
        let builder = make_builder(field.data_type(), 1000);
        array_builders.push(builder);
    }

    append_rows_to_array_builders(row, array_builders.as_mut_slice());

    let columns = array_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

fn rows_to_batch(
    mut rows: Rows,
    arrow_schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(&arrow_schema, projection)?;
    let mut array_builders = vec![];
    for field in arrow_schema.fields() {
        let builder = make_builder(field.data_type(), 1000);
        array_builders.push(builder);
    }

    while let Some(row) = rows.next().map_err(|e| {
        DataFusionError::Execution(format!("Failed to get next row from sqlite: {e:?}"))
    })? {
        append_rows_to_array_builders(row, array_builders.as_mut_slice());
    }

    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}

macro_rules! handle_primitive_type {
    ($builder:expr, $sqlite_type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .expect(concat!(
                "Failed to downcast builder to ",
                stringify!($builder_ty),
                " for ",
                stringify!($sqlite_type)
            ));
        let v: Option<$value_ty> = $row.get($index).expect(concat!(
            "Failed to get optional",
            stringify!($value_ty),
            " value for column ",
            stringify!($sqlite_type)
        ));

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

fn append_rows_to_array_builders(row: &Row, array_builders: &mut [Box<dyn ArrayBuilder>]) {
    for (i, builder) in array_builders.iter_mut().enumerate() {
        let sqlite_type = row
            .get_ref(i)
            .expect("Failed to get sqlite value ref")
            .data_type();
        match sqlite_type {
            Type::Null => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<NullBuilder>()
                    .expect("Failed to downcast builder to NullBuilder");
                builder.append_null();
            }
            Type::Integer => {
                handle_primitive_type!(builder, Type::Integer, Int64Builder, i64, row, i);
            }
            Type::Real => {
                handle_primitive_type!(builder, Type::Real, Float64Builder, f64, row, i);
            }
            Type::Text => {
                handle_primitive_type!(builder, Type::Text, StringBuilder, String, row, i);
            }
            Type::Blob => {
                handle_primitive_type!(builder, Type::Blob, BinaryBuilder, Vec<u8>, row, i);
            }
        }
    }
}
