use crate::{DFResult, RemoteDataType, RemoteField, RemoteSchema};
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch};
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
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    remote_schema: &RemoteSchema,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(remote_schema.fields.len());
    let mut new_fields: Vec<Field> = Vec::with_capacity(remote_schema.fields.len());
    for (idx, remote_field) in remote_schema.fields.iter().enumerate() {
        match remote_field.data_type {
            RemoteDataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                let (new_array, new_field ) =
                    transform.transform_boolean(array, &remote_field)?;
                new_arrays.push(new_array);
                new_fields.push(new_field);
            }
        }
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_arrays)?)
}
