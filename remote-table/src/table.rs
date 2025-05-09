use crate::connection::RemoteDbType;
use crate::{
    ConnectionOptions, DFResult, Pool, RemoteSchemaRef, RemoteTableExec, Transform, connect,
    transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect, SqliteDialect};
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

        let (table_schema, remote_schema) = if let Some(table_schema) = table_schema {
            let remote_schema = if transform.is_some() {
                // Infer remote schema
                let conn = pool.get().await?;
                conn.infer_schema(&sql).await.ok()
            } else {
                None
            };
            (table_schema, remote_schema)
        } else {
            // Infer table schema
            let conn = pool.get().await?;
            match conn.infer_schema(&sql).await {
                Ok(remote_schema) => {
                    let inferred_table_schema = Arc::new(remote_schema.to_arrow_schema());
                    (inferred_table_schema, Some(remote_schema))
                }
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to infer schema: {e}"
                    )));
                }
            }
        };

        let transformed_table_schema = transform_schema(
            table_schema.clone(),
            transform.as_ref(),
            remote_schema.as_ref(),
        )?;

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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RemoteTableExec::try_new(
            self.conn_options.clone(),
            self.sql.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.transform.clone(),
            self.pool.get().await?,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| support_filter_pushdown(self.conn_options.db_type(), &self.sql, f))
            .collect())
    }
}

pub(crate) fn support_filter_pushdown(
    db_type: RemoteDbType,
    sql: &str,
    filter: &Expr,
) -> TableProviderFilterPushDown {
    if !db_type.support_rewrite_with_filters_limit(sql) {
        return TableProviderFilterPushDown::Unsupported;
    }
    let unparser = match db_type {
        RemoteDbType::Mysql => Unparser::new(&MySqlDialect {}),
        RemoteDbType::Postgres => Unparser::new(&PostgreSqlDialect {}),
        RemoteDbType::Sqlite => Unparser::new(&SqliteDialect {}),
        RemoteDbType::Oracle => return TableProviderFilterPushDown::Unsupported,
        RemoteDbType::Dm => todo!(),
    };
    if unparser.expr_to_sql(filter).is_err() {
        return TableProviderFilterPushDown::Unsupported;
    }

    let mut pushdown = TableProviderFilterPushDown::Exact;
    filter
        .apply(|e| {
            if matches!(e, Expr::ScalarFunction(_)) {
                pushdown = TableProviderFilterPushDown::Unsupported;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("won't fail");

    pushdown
}
