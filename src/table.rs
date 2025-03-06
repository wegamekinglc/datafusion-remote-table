use crate::{connect, ConnectionArgs, DFResult, RemoteTableExec, Transform};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTable {
    pub(crate) conn_args: ConnectionArgs,
    pub(crate) sql: String,
    pub(crate) schema: SchemaRef,
    pub(crate) transform: Option<Arc<dyn Transform>>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_args: ConnectionArgs,
        sql: String,
        schema: Option<SchemaRef>,
    ) -> DFResult<Self> {
        let schema = match schema {
            None => {
                let conn = connect(&conn_args).await?;
                let remote_schema = conn.infer_schema(&sql).await?;
                Arc::new(remote_schema.to_arrow_schema())
            }
            Some(schema) => schema,
        };
        Ok(RemoteTable {
            conn_args,
            sql,
            schema,
            transform: None,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(
            RemoteTableExec::try_new(
                self.conn_args.clone(),
                projected_schema,
                self.sql.clone(),
                projection.cloned(),
                self.transform.clone(),
            )
            .await?,
        ))
    }
}
