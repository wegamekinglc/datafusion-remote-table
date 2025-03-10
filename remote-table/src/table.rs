use crate::{connect, ConnectionOptions, DFResult, RemoteTableExec, Transform};
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
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) sql: String,
    pub(crate) schema: SchemaRef,
    pub(crate) transform: Option<Arc<dyn Transform>>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<Self> {
        let sql = sql.into();
        let conn = connect(&conn_options).await?;
        let (_remote_schema, arrow_schema) = conn.infer_schema(&sql, transform.as_deref()).await?;
        Ok(RemoteTable {
            conn_options,
            sql,
            schema: arrow_schema,
            transform,
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
        TableType::View
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
                self.conn_options.clone(),
                projected_schema,
                self.sql.clone(),
                projection.cloned(),
                self.transform.clone(),
            )
            .await?,
        ))
    }
}
