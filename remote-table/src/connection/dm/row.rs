use crate::DFResult;
use datafusion::arrow::array::RecordBatch;
use odbc_api::CursorRow;

pub(crate) fn row_to_batch(_row: CursorRow) -> DFResult<RecordBatch> {
    todo!()
}
