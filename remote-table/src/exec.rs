use crate::{Connection, ConnectionOptions, DFResult, RemoteSchemaRef, Transform, TransformStream};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::TryStreamExt;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTableExec {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) sql: String,
    pub(crate) table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) transform: Option<Arc<dyn Transform>>,
    conn: Arc<dyn Connection>,
    plan_properties: PlanProperties,
}

impl RemoteTableExec {
    pub fn try_new(
        conn_options: ConnectionOptions,
        sql: String,
        table_schema: SchemaRef,
        remote_schema: Option<RemoteSchemaRef>,
        projection: Option<Vec<usize>>,
        transform: Option<Arc<dyn Transform>>,
        conn: Arc<dyn Connection>,
    ) -> DFResult<Self> {
        let projected_schema = project_schema(&table_schema, projection.as_ref())?;
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            conn_options,
            sql,
            table_schema,
            remote_schema,
            projection,
            transform,
            conn,
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let schema = self.schema();
        let fut = build_and_transform_stream(
            self.conn.clone(),
            self.conn_options.clone(),
            self.sql.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            self.projection.clone(),
            self.transform.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn build_and_transform_stream(
    conn: Arc<dyn Connection>,
    conn_options: ConnectionOptions,
    sql: String,
    table_schema: SchemaRef,
    remote_schema: Option<RemoteSchemaRef>,
    projection: Option<Vec<usize>>,
    transform: Option<Arc<dyn Transform>>,
) -> DFResult<SendableRecordBatchStream> {
    let stream = conn
        .query(
            &conn_options,
            &sql,
            table_schema.clone(),
            projection.as_ref(),
        )
        .await?;
    if let Some(transform) = transform.as_ref() {
        Ok(Box::pin(TransformStream::try_new(
            stream,
            transform.clone(),
            table_schema,
            projection,
            remote_schema,
        )?))
    } else {
        Ok(stream)
    }
}

impl DisplayAs for RemoteTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExec")
    }
}
