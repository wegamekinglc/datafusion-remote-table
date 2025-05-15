use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, DmConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectionOptions::Dm(
        DmConnectionOptions::new("localhost", 5236, "SYSDBA", "Password123")
            .with_schema(Some("SYSDBA".to_string())),
    );
    // let options = ConnectionOptions::Dm(
    //     DmConnectionOptions::new("192.168.0.227", 5236, "SYSDBA", "SYSDBA001")
    //         .with_schema(Some("SYSDBA".to_string())),
    // );
    let remote_table = RemoteTable::try_new(options, "select * from SUPPORTED_DATA_TYPES").await?;
    println!("remote_schema: {:?}", remote_table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("select * from remote_table").await?.show().await?;

    Ok(())
}
