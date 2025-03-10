use crate::{DFResult, RemoteTableExec, Transform};
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::fmt::Debug;
use std::sync::Arc;

pub trait TransformCodec: Debug + Send + Sync {
    fn try_encode(&self, value: &dyn Transform) -> DFResult<Vec<u8>>;
    fn try_decode(&self, value: &[u8]) -> DFResult<Arc<dyn Transform>>;
}

#[derive(Debug)]
pub struct RemotePhysicalCodec {
    transform_codec: Option<Arc<dyn TransformCodec>>,
}

impl RemotePhysicalCodec {
    pub fn new(transform_codec: Option<Arc<dyn TransformCodec>>) -> Self {
        Self { transform_codec }
    }
}

impl PhysicalExtensionCodec for RemotePhysicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, _buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(exec) = node.as_any().downcast_ref::<RemoteTableExec>() {
            let _serialized_transform = if let Some(transform) = exec.transform.as_ref() {
                let Some(transform_codec) = self.transform_codec.as_ref() else {
                    return Err(DataFusionError::Execution(
                        "No transform codec found".to_string(),
                    ));
                };
                let bytes = transform_codec.try_encode(transform.as_ref())?;
                Some(bytes)
            } else {
                None
            };
            todo!()
        } else {
            Err(DataFusionError::Execution(format!(
                "Failed to encode {}",
                RemoteTableExec::static_name()
            )))
        }
    }
}
