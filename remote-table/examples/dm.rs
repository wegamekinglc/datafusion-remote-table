use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, DmConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectionOptions::Dm(DmConnectionOptions::new(
        "localhost",
        5236,
        "SYSDBA",
        "Password123",
    ));
    let remote_table = RemoteTable::try_new(options, "select * from sysgrants").await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("select * from remote_table").await?.show().await?;

    Ok(())
}
