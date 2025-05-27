mod codec;
mod connection;
mod exec;
mod generated;
mod schema;
mod table;
mod transform;
mod unparse;
mod utils;

pub use codec::*;
pub use connection::*;
pub use exec::*;
pub use schema::*;
pub use table::*;
pub use transform::*;
pub use unparse::*;
pub use utils::*;

pub(crate) type DFResult<T> = datafusion::common::Result<T>;

#[cfg(not(any(
    feature = "mysql",
    feature = "postgres",
    feature = "oracle",
    feature = "sqlite",
    feature = "dm",
)))]
compile_error!(
    "At least one of the following features must be enabled: postgres, mysql, oracle, sqlite, dm"
);
