use crate::{DFResult, RemoteField, RemoteSchema};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, ListArray, RecordBatch, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use std::fmt::Debug;
use std::sync::Arc;

pub trait Transform: Debug + Send + Sync {
    fn transform_boolean(
        &self,
        array: &BooleanArray,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_int8(
        &self,
        array: &Int8Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_int16(
        &self,
        array: &Int16Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_int32(
        &self,
        array: &Int32Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_int64(
        &self,
        array: &Int64Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_uint8(
        &self,
        array: &UInt8Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_uint16(
        &self,
        array: &UInt16Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_uint32(
        &self,
        array: &UInt32Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_uint64(
        &self,
        array: &UInt64Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_float16(
        &self,
        array: &Float16Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_float32(
        &self,
        array: &Float32Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_float64(
        &self,
        array: &Float64Array,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }

    fn transform_list(
        &self,
        array: &ListArray,
        remote_field: &RemoteField,
    ) -> DFResult<(ArrayRef, Field)> {
        Ok((Arc::new(array.clone()), remote_field.to_arrow_field()))
    }
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    remote_schema: &RemoteSchema,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(remote_schema.fields.len());
    let mut new_fields: Vec<Field> = Vec::with_capacity(remote_schema.fields.len());
    for (idx, remote_field) in remote_schema.fields.iter().enumerate() {
        let (new_array, new_field) = match &remote_field.remote_type.to_arrow_type() {
            // TODO use a macro to reduce boilerplate
            DataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                transform.transform_boolean(array, &remote_field)?
            }
            DataType::Int8 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("Failed to downcast to Int8Array");
                transform.transform_int8(array, &remote_field)?
            }
            DataType::Int16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("Failed to downcast to Int16Array");
                transform.transform_int16(array, &remote_field)?
            }
            DataType::Int32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Failed to downcast to Int32Array");
                transform.transform_int32(array, &remote_field)?
            }
            DataType::Int64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Failed to downcast to Int64Array");
                transform.transform_int64(array, &remote_field)?
            }
            DataType::UInt8 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("Failed to downcast to UInt8Array");
                transform.transform_uint8(array, &remote_field)?
            }
            DataType::UInt16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .expect("Failed to downcast to UInt16Array");
                transform.transform_uint16(array, &remote_field)?
            }
            DataType::UInt32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("Failed to downcast to UInt32Array");
                transform.transform_uint32(array, &remote_field)?
            }
            DataType::UInt64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("Failed to downcast to UInt64Array");
                transform.transform_uint64(array, &remote_field)?
            }
            DataType::Float16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float16Array>()
                    .expect("Failed to downcast to Float16Array");
                transform.transform_float16(array, &remote_field)?
            }
            DataType::Float32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Failed to downcast to Float32Array");
                transform.transform_float32(array, &remote_field)?
            }
            DataType::Float64 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Failed to downcast to Float64Array");
                transform.transform_float64(array, &remote_field)?
            }
            DataType::List(_field) => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Failed to downcast to ListArray");
                transform.transform_list(array, &remote_field)?
            }
            data_type => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported arrow type {data_type:?}",
                )))
            }
        };
        new_arrays.push(new_array);
        new_fields.push(new_field);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_arrays)?)
}
