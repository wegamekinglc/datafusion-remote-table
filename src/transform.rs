use crate::{DFResult, RemoteDataType, RemoteField, RemoteSchema};
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use std::fmt::Debug;
use std::sync::Arc;

pub trait Transform: Debug + Send + Sync {
    fn transform_boolean(
        &self,
        array: &BooleanArray,
        _remote_field: &RemoteField,
        _target_field: &Field,
    ) -> DFResult<ArrayRef> {
        Ok(Arc::new(array.clone()))
    }
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    remote_schema: &RemoteSchema,
    target_schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(remote_schema.fields.len());
    for (idx, remote_field) in remote_schema.fields.iter().enumerate() {
        match remote_field.data_type {
            RemoteDataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                let new_array =
                    transform.transform_boolean(array, &remote_field, target_schema.field(idx))?;
                new_arrays.push(new_array);
            }
        }
    }
    Ok(RecordBatch::try_new(target_schema, new_arrays)?)
}
