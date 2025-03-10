use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions {
        host: "localhost".to_string(),
        port: 5432,
        username: "user".to_string(),
        password: "password".to_string(),
        database: None,
    });
    let remote_table = RemoteTable::try_new(options, "SELECT * from supported_data_types", None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))
        .unwrap();

    ctx.sql("SELECT * from remote_table")
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}
