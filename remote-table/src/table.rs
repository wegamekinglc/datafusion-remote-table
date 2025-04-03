use crate::{
    ConnectionOptions, DFResult, Pool, RemoteSchemaRef, RemoteTableExec, Transform, connect,
    transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
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

    pub fn rewrite_filters(&self, filters: Vec<Expr>) -> DFResult<Vec<Expr>> {
        filters
            .into_iter()
            .map(|f| {
                f.transform_down(|e| {
                    if let Expr::Column(col) = e {
                        let col_idx = self.transformed_table_schema.index_of(col.name())?;
                        let row_name = self.table_schema.field(col_idx).name().to_string();
                        Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                            row_name,
                        ))))
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
                .map(|trans| trans.data)
            })
            .collect::<DFResult<Vec<_>>>()
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
        // TODO support filter pushdown
        let rewritten_filters = self.rewrite_filters(filters.to_vec())?;
        Ok(Arc::new(RemoteTableExec::try_new(
            self.conn_options.clone(),
            self.sql.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            projection.cloned(),
            rewritten_filters,
            limit,
            self.transform.clone(),
            self.pool.get().await?,
        )?))
    }

    // fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
    //     Ok(filters.iter().map(|e| support_filter_pushdown(&self.conn_options, *e)).collect())
    // }
}

// pub(crate) fn support_filter_pushdown(
//     options: &ConnectionOptions,
//     filter: &Expr,
// ) -> TableProviderFilterPushDown {
//     let unparser = match options {
//         ConnectionOptions::Mysql(_) => Unparser::new(&MySqlDialect {}),
//         ConnectionOptions::Postgres(_) => Unparser::new(&PostgreSqlDialect {}),
//         ConnectionOptions::Sqlite(_) => Unparser::new(&SqliteDialect {}),
//         ConnectionOptions::Oracle(_) => return TableProviderFilterPushDown::Unsupported,
//     };
//     if let Err(_) = unparser.expr_to_sql(filter) {
//         return TableProviderFilterPushDown::Unsupported;
//     }
//
//     let mut pushdown = TableProviderFilterPushDown::Exact;
//     filter
//         .apply(|e| {
//             if matches!(e, Expr::ScalarFunction(_)) {
//                 pushdown = TableProviderFilterPushDown::Unsupported;
//             }
//             Ok(TreeNodeRecursion::Continue)
//         })
//         .expect("won't fail");
//
//     pushdown
// }
