use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions::new(
        "localhost",
        5432,
        "user",
        "password",
    ));
    let remote_table = RemoteTable::try_new(options, "SELECT * from supported_data_types").await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("SELECT * from remote_table").await?.show().await?;

    Ok(())
}
