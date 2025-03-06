use crate::{DFResult, RemoteDataType, RemoteField, RemoteSchema};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema};
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
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    remote_schema: &RemoteSchema,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(remote_schema.fields.len());
    let mut new_fields: Vec<Field> = Vec::with_capacity(remote_schema.fields.len());
    for (idx, remote_field) in remote_schema.fields.iter().enumerate() {
        let (new_array, new_field) = match remote_field.data_type {
            RemoteDataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                transform.transform_boolean(array, &remote_field)?
            }
            RemoteDataType::Int16 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("Failed to downcast to Int16Array");
                transform.transform_int16(array, &remote_field)?
            }
            RemoteDataType::Int32 => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Failed to downcast to Int32Array");
                transform.transform_int32(array, &remote_field)?
            }
        };
        new_arrays.push(new_array);
        new_fields.push(new_field);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_arrays)?)
}
