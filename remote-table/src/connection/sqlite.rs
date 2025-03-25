use crate::connection::projections_contains;
use crate::transform::transform_batch;
use crate::{
    Connection, ConnectionOptions, DFResult, Pool, RemoteField, RemoteSchema, RemoteSchemaRef,
    RemoteType, SqliteType, Transform,
};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, NullBuilder, RecordBatch,
    StringBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use rusqlite::{Column, Row, Rows};
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
    ) -> DFResult<(RemoteSchemaRef, SchemaRef)> {
        let sql = sql.to_string();
        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let columns: Vec<OwnedColumn> =
                    stmt.columns().iter().map(sqlite_col_to_owned_col).collect();

                let remote_schema = Arc::new(
                    build_remote_schema(columns.as_slice())
                        .map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?,
                );
                let arrow_schema = Arc::new(remote_schema.to_arrow_schema());

                if let Some(transform) = transform {
                    let mut rows = stmt.query([])?;
                    let Some(first_row) = rows.next()? else {
                        return Err(tokio_rusqlite::Error::Other(Box::new(
                            DataFusionError::Execution("No data returned from sqlite".to_string()),
                        )));
                    };
                    let batch = row_to_batch(first_row, &arrow_schema, columns)
                        .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                    let transformed_batch = transform_batch(
                        batch,
                        transform.as_ref(),
                        &arrow_schema,
                        None,
                        Some(&remote_schema),
                    )
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
        _conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;
        let projection = projection.cloned();
        let sql = sql.to_string();
        let batch = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let columns: Vec<OwnedColumn> =
                    stmt.columns().iter().map(sqlite_col_to_owned_col).collect();
                let rows = stmt.query([])?;

                let batch = rows_to_batch(rows, &table_schema, columns, projection.as_ref())
                    .map_err(|e| tokio_rusqlite::Error::Other(e.into()))?;
                Ok(batch)
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to exec query: {e:?}")))?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            projected_schema,
            None,
        )?))
    }
}

#[derive(Debug)]
struct OwnedColumn {
    name: String,
    decl_type: Option<String>,
}

fn sqlite_col_to_owned_col(sqlite_col: &Column) -> OwnedColumn {
    OwnedColumn {
        name: sqlite_col.name().to_string(),
        decl_type: sqlite_col.decl_type().map(|x| x.to_string()),
    }
}

fn sqlite_type_to_remote_type(sqlite_col: &OwnedColumn) -> DFResult<RemoteType> {
    match sqlite_col.decl_type.as_deref() {
        None => Ok(RemoteType::Sqlite(SqliteType::Null)),
        Some(t) if t.eq_ignore_ascii_case("integer") => Ok(RemoteType::Sqlite(SqliteType::Integer)),
        Some(t) if t.eq_ignore_ascii_case("real") => Ok(RemoteType::Sqlite(SqliteType::Real)),
        Some(t) if t.eq_ignore_ascii_case("text") => Ok(RemoteType::Sqlite(SqliteType::Text)),
        Some(t) if t.eq_ignore_ascii_case("blob") => Ok(RemoteType::Sqlite(SqliteType::Blob)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported sqlite type: {:?}",
            sqlite_col.decl_type
        ))),
    }
}

fn build_remote_schema(columns: &[OwnedColumn]) -> DFResult<RemoteSchema> {
    let mut remote_fields = Vec::with_capacity(columns.len());
    for col in columns.iter() {
        let remote_type = sqlite_type_to_remote_type(col)?;
        remote_fields.push(RemoteField::new(&col.name, remote_type, true));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn row_to_batch(
    row: &Row,
    table_schema: &SchemaRef,
    columns: Vec<OwnedColumn>,
) -> DFResult<RecordBatch> {
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), 1000);
        array_builders.push(builder);
    }

    append_rows_to_array_builders(
        row,
        table_schema,
        &columns,
        None,
        array_builders.as_mut_slice(),
    )?;

    let columns = array_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(table_schema.clone(), columns)?)
}

fn rows_to_batch(
    mut rows: Rows,
    table_schema: &SchemaRef,
    columns: Vec<OwnedColumn>,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), 1000);
        array_builders.push(builder);
    }

    while let Some(row) = rows.next().map_err(|e| {
        DataFusionError::Execution(format!("Failed to get next row from sqlite: {e:?}"))
    })? {
        append_rows_to_array_builders(
            row,
            table_schema,
            &columns,
            projection,
            array_builders.as_mut_slice(),
        )?;
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

fn append_rows_to_array_builders(
    row: &Row,
    table_schema: &SchemaRef,
    columns: &[OwnedColumn],
    projection: Option<&Vec<usize>>,
    array_builders: &mut [Box<dyn ArrayBuilder>],
) -> DFResult<()> {
    for (idx, field) in table_schema.fields.iter().enumerate() {
        if !projections_contains(projection, idx) {
            continue;
        }
        let builder = &mut array_builders[idx];
        let col = columns.get(idx);
        match field.data_type() {
            DataType::Null => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<NullBuilder>()
                    .expect("Failed to downcast builder to NullBuilder");
                builder.append_null();
            }
            DataType::Int64 => {
                handle_primitive_type!(builder, col, Int64Builder, i64, row, idx);
            }
            DataType::Float64 => {
                handle_primitive_type!(builder, col, Float64Builder, f64, row, idx);
            }
            DataType::Utf8 => {
                handle_primitive_type!(builder, col, StringBuilder, String, row, idx);
            }
            DataType::Binary => {
                handle_primitive_type!(builder, col, BinaryBuilder, Vec<u8>, row, idx);
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type {:?} for col: {:?}",
                    field.data_type(),
                    col
                )));
            }
        }
    }
    Ok(())
}
