use crate::connection::{ODBC_ENV, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, DmType, Pool, RemoteDbType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use async_stream::stream;
use datafusion::arrow::array::{
    BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::lock::Mutex;
use odbc_api::buffers::{BufferDesc, ColumnarAnyBuffer};
use odbc_api::handles::StatementImpl;
use odbc_api::{Bit, Cursor, CursorImpl, Environment, ResultSetMetadata, decimal_text_to_i128};
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone, With, Getters)]
pub struct DmConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) schema: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl DmConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            schema: None,
            pool_max_size: 10,
            stream_chunk_size: 2048,
        }
    }
}

#[derive(Debug)]
pub struct DmPool {
    options: DmConnectionOptions,
}

pub(crate) fn connect_dm(options: &DmConnectionOptions) -> DFResult<DmPool> {
    Ok(DmPool {
        options: options.clone(),
    })
}

#[async_trait::async_trait]
impl Pool for DmPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let env = ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
        let mut connection_str = format!(
            "Driver={};Server={};Port={};UID={};PWD={}",
            "{DM8 ODBC DRIVER}",
            self.options.host,
            self.options.port,
            self.options.username,
            self.options.password,
        );
        if let Some(schema) = &self.options.schema {
            connection_str.push_str(&format!(";SCHEMA={schema}"));
        }
        let connection = env
            .connect_with_connection_string(&connection_str, odbc_api::ConnectionOptions::default())
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create odbc connection: {e:?}"))
            })?;
        Ok(Arc::new(DmConnection {
            conn: Arc::new(Mutex::new(connection)),
        }))
    }
}

#[derive(Debug)]
pub struct DmConnection {
    conn: Arc<Mutex<odbc_api::Connection<'static>>>,
}

#[async_trait::async_trait]
impl Connection for DmConnection {
    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Dm.query_limit_1(sql)?;
        let conn = self.conn.lock().await;
        let cursor_opt = conn
            .execute(&sql, (), None)
            .map_err(|e| DataFusionError::Execution(format!("Failed to infer schema: {e:?}")))?;
        match cursor_opt {
            None => Err(DataFusionError::Execution(
                "No rows returned to infer schema".to_string(),
            )),
            Some(cursor) => {
                let remote_schema = Arc::new(build_remote_schema(cursor)?);
                Ok(remote_schema)
            }
        }
    }

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;
        let sql = RemoteDbType::Dm.try_rewrite_query(sql, filters, limit)?;
        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let projection = projection.cloned();
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            let cursor_opt = conn.execute(&sql, (), None).map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute query: {e:?}"))
            })?;

            match cursor_opt {
                None => {}
                Some(cursor) => {
                    let buffer_descs = table_schema
                        .fields()
                        .iter()
                        .map(|field| build_buffer_desc(field))
                        .collect::<DFResult<Vec<_>>>()?;
                    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(
                        chunk_size,
                        buffer_descs,
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to create buffer: {e:?}"))
                    })?;
                    let mut block_cursor = cursor.bind_buffer(row_set_buffer).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to bind buffer: {e:?}"))
                    })?;
                    loop {
                        match block_cursor.fetch_with_truncation_check(true) {
                            Ok(Some(buffer)) => {
                                let batch = buffer_to_batch(
                                    buffer,
                                    &table_schema,
                                    projection.as_ref(),
                                    chunk_size,
                                )?;
                                batch_tx.blocking_send(batch).map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Failed to send batch: {e:?}"
                                    ))
                                })?;
                            }
                            Ok(None) => break,
                            Err(odbc_error) => {
                                return Err(DataFusionError::External(Box::new(odbc_error)));
                            }
                        }
                    }
                }
            }

            Ok::<_, DataFusionError>(())
        });

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            if let Err(e) = join_handle.await {
                yield Err(DataFusionError::Execution(format!(
                    "Failed to execute ODBC query: {e}"
                )))
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            output_stream,
        )))
    }
}

fn build_remote_schema(mut cursor: CursorImpl<StatementImpl>) -> DFResult<RemoteSchema> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        let col_name = cursor
            .col_name(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let col_type = cursor
            .col_data_type(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let remote_type = RemoteType::Dm(dm_type_to_remote_type(col_type)?);
        let col_nullable = cursor
            .col_nullability(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .could_be_nullable();

        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

fn dm_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<DmType> {
    match data_type {
        odbc_api::DataType::TinyInt => Ok(DmType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(DmType::SmallInt),
        odbc_api::DataType::Integer => Ok(DmType::Integer),
        odbc_api::DataType::BigInt => Ok(DmType::BigInt),
        odbc_api::DataType::Real => Ok(DmType::Real),
        odbc_api::DataType::Double => Ok(DmType::Double),
        odbc_api::DataType::Numeric { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Numeric(precision as u8, scale as i8))
        }
        odbc_api::DataType::Decimal { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Decimal(precision as u8, scale as i8))
        }
        odbc_api::DataType::Char { length } => Ok(DmType::Char(length.map(|l| l.get() as u16))),
        odbc_api::DataType::Varchar { length } => {
            Ok(DmType::Varchar(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Binary { length } => {
            assert!(length.is_some());
            Ok(DmType::Binary(length.unwrap().get() as u16))
        }
        odbc_api::DataType::Varbinary { length } => {
            Ok(DmType::Varbinary(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarchar { .. } => Ok(DmType::Text),
        odbc_api::DataType::LongVarbinary { .. } => Ok(DmType::Image),
        odbc_api::DataType::Bit => Ok(DmType::Bit),
        odbc_api::DataType::Timestamp { precision } => {
            assert!(precision >= 0);
            assert!(precision <= 9);
            Ok(DmType::Timestamp(precision as u8))
        }
        odbc_api::DataType::Date => Ok(DmType::Date),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported DM type: {data_type:?}"
        ))),
    }
}

fn build_buffer_desc(field: &Field) -> DFResult<BufferDesc> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::Boolean => Ok(BufferDesc::Bit { nullable }),
        DataType::Int8 => Ok(BufferDesc::I8 { nullable }),
        DataType::Int16 => Ok(BufferDesc::I16 { nullable }),
        DataType::Int32 => Ok(BufferDesc::I32 { nullable }),
        DataType::Int64 => Ok(BufferDesc::I64 { nullable }),
        DataType::Float32 => Ok(BufferDesc::F32 { nullable }),
        DataType::Float64 => Ok(BufferDesc::F64 { nullable }),
        DataType::Decimal128(precision, _scale) => {
            Ok(BufferDesc::Text {
                // Must be able to hold num precision digits a sign and a decimal point
                max_str_len: *precision as usize + 2,
            })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported data type to build buffer desc: {:?}",
            field.data_type()
        ))),
    }
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $nullable:expr, $value_ty:ty, $col_slice:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        if $nullable {
            let values = $col_slice.as_nullable_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get nullable slice for {:?}", $field))
            })?;
            for value in values {
                builder.append_option(value.map($convert));
            }
        } else {
            let values = $col_slice.as_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get slice for {:?}", $field))
            })?;
            for value in values {
                builder.append_value($convert(value));
            }
        }
    }};
}

macro_rules! handle_variable_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $col_slice:expr, $slice_fn:ident, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let values = $col_slice.$slice_fn().ok_or_else(|| {
            DataFusionError::Execution(format!("Failed to get view for {:?}", $field))
        })?;
        for value in values.iter() {
            match value {
                Some(v) => {
                    builder.append_value($convert(v));
                }
                None => {
                    builder.append_null();
                }
            }
        }
    }};
}

fn buffer_to_batch(
    buffer: &ColumnarAnyBuffer,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    chunk_size: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;

    let mut arrays = Vec::with_capacity(projected_schema.fields().len());
    for (col_idx, field) in table_schema.fields().iter().enumerate() {
        if !projections_contains(projection, col_idx) {
            continue;
        }
        let mut builder = make_builder(field.data_type(), chunk_size);
        let col_slice = buffer.column(col_idx);
        let nullable = field.is_nullable();
        match field.data_type() {
            DataType::Boolean => {
                handle_primitive_type!(
                    builder,
                    field,
                    BooleanBuilder,
                    nullable,
                    Bit,
                    col_slice,
                    |bit: &Bit| bit.as_bool()
                );
            }
            DataType::Int8 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int8Builder,
                    nullable,
                    i8,
                    col_slice,
                    |value: &i8| *value
                );
            }
            DataType::Int16 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int16Builder,
                    nullable,
                    i16,
                    col_slice,
                    |value: &i16| *value
                );
            }
            DataType::Int32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int32Builder,
                    nullable,
                    i32,
                    col_slice,
                    |value: &i32| *value
                );
            }
            DataType::Int64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int64Builder,
                    nullable,
                    i64,
                    col_slice,
                    |value: &i64| *value
                );
            }
            DataType::Float32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float32Builder,
                    nullable,
                    f32,
                    col_slice,
                    |value: &f32| *value
                );
            }
            DataType::Float64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float64Builder,
                    nullable,
                    f64,
                    col_slice,
                    |value: &f64| *value
                );
            }
            DataType::Decimal128(_, scale) => {
                handle_variable_type!(
                    builder,
                    field,
                    Decimal128Builder,
                    col_slice,
                    as_text_view,
                    |value: &[u8]| decimal_text_to_i128(value, *scale as usize)
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported field to build record batch: {:?}",
                    field
                )));
            }
        }
        arrays.push(builder.finish());
    }
    Ok(RecordBatch::try_new(projected_schema, arrays)?)
}
