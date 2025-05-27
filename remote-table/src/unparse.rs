use crate::{DFResult, RemoteDbType};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use std::fmt::Debug;

pub trait Unparse: Debug + Send + Sync {
    fn support_filter_pushdown(
        &self,
        filter: &Expr,
        db_type: RemoteDbType,
    ) -> DFResult<TableProviderFilterPushDown>;

    fn unparse_filter(&self, filter: &Expr, db_type: RemoteDbType) -> DFResult<String>;
}

#[derive(Debug)]
pub struct DefaultUnparser {}

impl Unparse for DefaultUnparser {
    fn support_filter_pushdown(
        &self,
        filter: &Expr,
        db_type: RemoteDbType,
    ) -> DFResult<TableProviderFilterPushDown> {
        let unparser = match db_type.create_unparser() {
            Ok(unparser) => unparser,
            Err(_) => return Ok(TableProviderFilterPushDown::Unsupported),
        };
        if unparser.expr_to_sql(filter).is_err() {
            return Ok(TableProviderFilterPushDown::Unsupported);
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

        Ok(pushdown)
    }

    fn unparse_filter(&self, filter: &Expr, db_type: RemoteDbType) -> DFResult<String> {
        let unparser = db_type.create_unparser()?;
        let ast = unparser.expr_to_sql(filter)?;
        Ok(format!("{ast}"))
    }
}
