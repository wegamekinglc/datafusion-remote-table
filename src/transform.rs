use crate::{DFResult, RemoteDataType};
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use std::fmt::Debug;
use std::sync::Arc;

pub trait Transform: Debug + Send + Sync {
    fn transform_boolean(
        &self,
        array: &BooleanArray,
        _remote_type: &RemoteDataType,
        _target_type: &DataType,
    ) -> DFResult<ArrayRef> {
        Ok(Arc::new(array.clone()))
    }
}

#[derive(Debug)]
pub struct DefaultTransform;

impl Transform for DefaultTransform {}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    remote_schema: &[RemoteDataType],
    target_schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(remote_schema.len());
    for (idx, remote_type) in remote_schema.iter().enumerate() {
        match remote_type {
            RemoteDataType::Boolean => {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Failed to downcast to BooleanArray");
                let new_array = transform.transform_boolean(
                    array,
                    remote_type,
                    target_schema.field(idx).data_type(),
                )?;
                new_arrays.push(new_array);
            }
        }
    }
    Ok(RecordBatch::try_new(target_schema, new_arrays)?)
}
