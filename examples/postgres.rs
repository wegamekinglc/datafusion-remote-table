use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions {
        host: "192.168.0.227".to_string(),
        port: 33448,
        username: "postgres".to_string(),
        password: "bjsh".to_string(),
        database: None,
    });
    let remote_table =
        RemoteTable::try_new(options, "SELECT * from lwz_remote_test4".to_string(), None)
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
