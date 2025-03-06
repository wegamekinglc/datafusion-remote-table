use std::fmt::Debug;
use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use crate::{DFResult, RemoteTableExec, Transform};

pub trait TransformCodec: Debug + Send + Sync {
    fn try_encode(&self, value: &dyn Transform) -> DFResult<Vec<u8>>;
    fn try_decode(&self, value: &[u8]) -> DFResult<Arc<dyn Transform>>;
}

#[derive(Debug)]
pub struct RemotePhysicalCodec {
    transform_codec: Arc<dyn TransformCodec>
}

impl RemotePhysicalCodec {
    pub fn new(transform_codec: Arc<dyn TransformCodec>) -> Self {
        Self { transform_codec }
    }
}

impl PhysicalExtensionCodec for RemotePhysicalCodec {
    fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], registry: &dyn FunctionRegistry) -> DFResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(exec) = node.as_any().downcast_ref::<RemoteTableExec>() {
            todo!()
        } else {
            Err(DataFusionError::Execution(format!("Failed to encode {}", RemoteTableExec::static_name())))
        }
    }
}