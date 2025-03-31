use crate::{
    ConnectionOptions, DFResult, Pool, RemoteSchemaRef, RemoteTableExec, Transform, connect,
    transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTable {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) sql: String,
    pub(crate) table_schema: SchemaRef,
    pub(crate) transformed_table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) transform: Option<Arc<dyn Transform>>,
    pub(crate) pool: Arc<dyn Pool>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform(conn_options, sql, None, None).await
    }

    pub async fn try_new_with_schema(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
        table_schema: SchemaRef,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform(conn_options, sql, Some(table_schema), None).await
    }

    pub async fn try_new_with_transform(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform(conn_options, sql, None, Some(transform)).await
    }

    pub async fn try_new_with_schema_transform(
        conn_options: ConnectionOptions,
        sql: impl Into<String>,
        table_schema: Option<SchemaRef>,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<Self> {
        let sql = sql.into();
        let pool = connect(&conn_options).await?;
        let conn = pool.get().await?;
        let (table_schema, remote_schema) = match conn.infer_schema(&sql).await {
            Ok((remote_schema, inferred_table_schema)) => (
                table_schema.unwrap_or(inferred_table_schema),
                Some(remote_schema),
            ),
            Err(e) => {
                if let Some(table_schema) = table_schema {
                    (table_schema, None)
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to infer schema: {e}"
                    )));
                }
            }
        };
        let transformed_table_schema = if let Some(transform) = transform.as_ref() {
            transform_schema(
                table_schema.clone(),
                transform.as_ref(),
                remote_schema.as_ref(),
            )?
        } else {
            table_schema.clone()
        };
        Ok(RemoteTable {
            conn_options,
            sql,
            table_schema,
            transformed_table_schema,
            remote_schema,
            transform,
            pool,
        })
    }

    pub fn remote_schema(&self) -> Option<RemoteSchemaRef> {
        self.remote_schema.clone()
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.transformed_table_schema.clone()
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
        // TODO support limit pushdown
        Ok(Arc::new(RemoteTableExec::try_new(
            self.conn_options.clone(),
            self.sql.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            projection.cloned(),
            self.transform.clone(),
            self.pool.get().await?,
        )?))
    }
}
