use crate::{DFResult, RemoteField, RemoteSchemaRef};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct TransformArgs<'a> {
    pub col_index: usize,
    pub field: &'a Field,
    pub remote_field: Option<&'a RemoteField>,
}

pub trait Transform: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn transform_null(
        &self,
        array: &NullArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_boolean(
        &self,
        array: &BooleanArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int8(
        &self,
        array: &Int8Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int16(
        &self,
        array: &Int16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int32(
        &self,
        array: &Int32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int64(
        &self,
        array: &Int64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint8(
        &self,
        array: &UInt8Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint16(
        &self,
        array: &UInt16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint32(
        &self,
        array: &UInt32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint64(
        &self,
        array: &UInt64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float16(
        &self,
        array: &Float16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float32(
        &self,
        array: &Float32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float64(
        &self,
        array: &Float64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_utf8(
        &self,
        array: &StringArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_binary(
        &self,
        array: &BinaryArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_second(
        &self,
        array: &TimestampSecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_millisecond(
        &self,
        array: &TimestampMillisecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_microsecond(
        &self,
        array: &TimestampMicrosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_nanosecond(
        &self,
        array: &TimestampNanosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_time64_nanosecond(
        &self,
        array: &Time64NanosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_date32(
        &self,
        array: &Date32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_list(
        &self,
        array: &ListArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }
}

pub(crate) struct TransformStream {
    input: SendableRecordBatchStream,
    transform: Arc<dyn Transform>,
    table_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    remote_schema: Option<RemoteSchemaRef>,
}

impl TransformStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        transform: Arc<dyn Transform>,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        remote_schema: Option<RemoteSchemaRef>,
    ) -> DFResult<Self> {
        let projected_schema = project_schema(&table_schema, projection.as_ref())?;
        Ok(Self {
            input,
            transform,
            table_schema,
            projection,
            projected_schema,
            remote_schema,
        })
    }
}

impl Stream for TransformStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match transform_batch(
                    batch,
                    self.transform.as_ref(),
                    &self.table_schema,
                    self.projection.as_ref(),
                    self.remote_schema.as_ref(),
                ) {
                    Ok(result) => Poll::Ready(Some(Ok(result))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TransformStream {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    remote_schema: Option<&RemoteSchemaRef>,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(batch.schema().fields.len());
    let mut new_fields: Vec<Field> = Vec::with_capacity(batch.schema().fields.len());
    let all_col_indexes = (0..table_schema.fields.len()).collect::<Vec<usize>>();
    let projected_col_indexes = projection.unwrap_or(&all_col_indexes);

    for (idx, col_index) in projected_col_indexes.iter().enumerate() {
        let field = table_schema.field(*col_index);
        let remote_field = remote_schema.map(|schema| &schema.fields[*col_index]);
        let args = TransformArgs {
            col_index: *col_index,
            field,
            remote_field,
        };

        let (new_array, new_field) = match &field.data_type() {
            DataType::Null => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<NullArray>()
                    .expect("Failed to downcast to NullArray");
                transform.transform_null(array, args)?
            }
            DataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                transform.transform_boolean(array, args)?
            }
            DataType::Int8 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("Failed to downcast to Int8Array");
                transform.transform_int8(array, args)?
            }
            DataType::Int16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("Failed to downcast to Int16Array");
                transform.transform_int16(array, args)?
            }
            DataType::Int32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Failed to downcast to Int32Array");
                transform.transform_int32(array, args)?
            }
            DataType::Int64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Failed to downcast to Int64Array");
                transform.transform_int64(array, args)?
            }
            DataType::UInt8 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("Failed to downcast to UInt8Array");
                transform.transform_uint8(array, args)?
            }
            DataType::UInt16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .expect("Failed to downcast to UInt16Array");
                transform.transform_uint16(array, args)?
            }
            DataType::UInt32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("Failed to downcast to UInt32Array");
                transform.transform_uint32(array, args)?
            }
            DataType::UInt64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("Failed to downcast to UInt64Array");
                transform.transform_uint64(array, args)?
            }
            DataType::Float16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float16Array>()
                    .expect("Failed to downcast to Float16Array");
                transform.transform_float16(array, args)?
            }
            DataType::Float32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Failed to downcast to Float32Array");
                transform.transform_float32(array, args)?
            }
            DataType::Float64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Failed to downcast to Float64Array");
                transform.transform_float64(array, args)?
            }
            DataType::Utf8 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Failed to downcast to StringArray");
                transform.transform_utf8(array, args)?
            }
            DataType::Binary => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("Failed to downcast to BinaryArray");
                transform.transform_binary(array, args)?
            }
            DataType::Date32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("Failed to downcast to Date32Array");
                transform.transform_date32(array, args)?
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .expect("Failed to downcast to TimestampSecondArray");
                transform.transform_timestamp_second(array, args)?
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("Failed to downcast to TimestampMillisecondArray");
                transform.transform_timestamp_millisecond(array, args)?
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Failed to downcast to TimestampMicrosecondArray");
                transform.transform_timestamp_microsecond(array, args)?
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("Failed to downcast to TimestampNanosecondArray");
                transform.transform_timestamp_nanosecond(array, args)?
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .expect("Failed to downcast to Time64NanosecondArray");
                transform.transform_time64_nanosecond(array, args)?
            }
            DataType::List(_field) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Failed to downcast to ListArray");
                transform.transform_list(array, args)?
            }
            data_type => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported arrow type {data_type:?}",
                )));
            }
        };
        new_arrays.push(new_array);
        new_fields.push(new_field);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_arrays)?)
}

pub fn transform_schema(
    schema: SchemaRef,
    transform: &dyn Transform,
    remote_schema: Option<&RemoteSchemaRef>,
) -> DFResult<SchemaRef> {
    let empty_record = RecordBatch::new_empty(schema.clone());
    transform_batch(empty_record, transform, &schema, None, remote_schema)
        .map(|batch| batch.schema())
}
