use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};

pub mod shared_containers;
pub mod utils;

#[tokio::test]
pub async fn supported_postgres_all_types() {
    shared_containers::setup_v2();
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions {
        host: "127.0.0.1".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: "password".to_string(),
        database: Some("postgres".to_string()),
    });
    let table = RemoteTable::try_new(options, "select * from supported_data_types", None).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    ctx.sql("SELECT * FROM remote_table").await.unwrap().show().await.unwrap();
}