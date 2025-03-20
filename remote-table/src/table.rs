use crate::{connect, ConnectionOptions, DFResult, Pool, RemoteSchema, RemoteTableExec, Transform};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTable {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) sql: String,
    pub(crate) table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchema>,
    pub(crate) transform: Option<Arc<dyn Transform>>,
    pub(crate) pool: Arc<dyn Pool>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
        table_schema: Option<SchemaRef>,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<Self> {
        let sql = sql.into();
        let pool = connect(&conn_options).await?;
        let conn = pool.get().await?;
        let (table_schema, remote_schema) = match conn.infer_schema(&sql, transform.clone()).await {
            Ok((remote_schema, inferred_table_schema)) => (
                table_schema.unwrap_or(inferred_table_schema),
                Some(remote_schema),
            ),
            Err(e) => {
                if let Some(table_schema) = table_schema {
                    (table_schema, None)
                } else {
                    return Err(e);
                }
            }
        };
        Ok(RemoteTable {
            conn_options,
            sql,
            table_schema,
            remote_schema,
            transform,
            pool,
        })
    }

    pub fn remote_schema(&self) -> Option<&RemoteSchema> {
        self.remote_schema.as_ref()
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
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
        Ok(Arc::new(RemoteTableExec::try_new(
            self.conn_options.clone(),
            self.sql.clone(),
            self.table_schema.clone(),
            // TODO RemoteSchemaRef
            self.remote_schema.clone(),
            projection.cloned(),
            self.transform.clone(),
            self.pool.get().await?,
        )?))
    }
}
