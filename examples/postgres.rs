use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionArgs, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let conn_args = ConnectionArgs::Postgresql {
        host: "192.168.0.227".to_string(),
        port: 33448,
        username: "postgres".to_string(),
        password: "bjsh".to_string(),
        database: None,
    };
    let remote_table = RemoteTable::try_new(
        conn_args,
        "SELECT success, code from lwz_remote_test3".to_string(),
        None,
    )
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
