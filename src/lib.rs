mod connection;
mod exec;
mod table;
mod transform;

pub use connection::*;
pub use exec::*;
pub use table::*;
pub use transform::*;

pub(crate) type DFResult<T> = datafusion::common::Result<T>;

pub enum RemoteDataType {
    Boolean,
}
