use crate::transform::transform_batch;
use crate::{connect, ConnectionArgs, DFResult, RemoteSchema, Transform};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt, TryStreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct RemoteTableExec {
    conn_args: ConnectionArgs,
    sql: String,
    projection: Option<Vec<usize>>,
    transform: Arc<dyn Transform>,
    plan_properties: PlanProperties,
}

impl RemoteTableExec {
    pub async fn try_new(
        conn_args: ConnectionArgs,
        projected_schema: SchemaRef,
        sql: String,
        projection: Option<Vec<usize>>,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            conn_args,
            sql,
            projection,
            transform,
            plan_properties,
        })
    }
}

impl ExecutionPlan for RemoteTableExec {
    fn name(&self) -> &str {
        "RemoteTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let fut = transform_stream(
            self.conn_args.clone(),
            self.sql.clone(),
            self.projection.clone(),
            self.transform.clone(),
            schema.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub async fn transform_stream(
    conn_args: ConnectionArgs,
    sql: String,
    projection: Option<Vec<usize>>,
    transform: Arc<dyn Transform>,
    schema: SchemaRef,
) -> DFResult<SendableRecordBatchStream> {
    let conn = connect(&conn_args).await?;
    let (stream, remote_schema) = conn.query(sql, projection).await?;
    assert_eq!(schema.fields().len(), remote_schema.fields.len());
    Ok(Box::pin(RemoteTableExecStream {
        input: stream,
        transform,
        schema,
        remote_schema,
    }))
}

pub struct RemoteTableExecStream {
    input: SendableRecordBatchStream,
    transform: Arc<dyn Transform>,
    schema: SchemaRef,
    remote_schema: RemoteSchema,
}

impl Stream for RemoteTableExecStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match transform_batch(
                    batch,
                    self.transform.as_ref(),
                    &self.remote_schema,
                    self.schema.clone(),
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

impl RecordBatchStream for RemoteTableExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DisplayAs for RemoteTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExec")
    }
}
