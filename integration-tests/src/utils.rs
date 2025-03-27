use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{
    ConnectionOptions, MysqlConnectionOptions, OracleConnectionOptions, PostgresConnectionOptions,
    RemoteTable,
};
use std::path::PathBuf;
use std::sync::Arc;

pub async fn assert_result(database: &str, remote_sql: &str, df_sql: &str, expected_result: &str) {
    let options = build_conn_options(database);
    let table = RemoteTable::try_new(options, remote_sql).await.unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx.sql(df_sql).await.unwrap().collect().await.unwrap();
    println!("{}", pretty_format_batches(result.as_slice()).unwrap());

    assert_eq!(
        pretty_format_batches(result.as_slice())
            .unwrap()
            .to_string(),
        expected_result
    );
}

pub async fn assert_sqls(database: &str, remote_sqls: Vec<&str>) {
    let options = build_conn_options(database);

    for sql in remote_sqls.into_iter() {
        println!("Testing sql: {sql}");

        let table = RemoteTable::try_new(options.clone(), sql).await.unwrap();
        println!("remote schema: {:#?}", table.remote_schema());

        let ctx = SessionContext::new();
        ctx.register_table("remote_table", Arc::new(table)).unwrap();
        ctx.sql("select * from remote_table")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
    }
}

pub fn build_conn_options(database: &str) -> ConnectionOptions {
    match database.to_lowercase().as_str() {
        "mysql" => ConnectionOptions::Mysql(
            MysqlConnectionOptions::new("127.0.0.1", 3306, "root", "password")
                .with_database(Some("test".to_string())),
        ),
        "postgres" => ConnectionOptions::Postgres(
            PostgresConnectionOptions::new("localhost", 5432, "postgres", "password")
                .with_database(Some("postgres".to_string())),
        ),
        "oracle" => ConnectionOptions::Oracle(OracleConnectionOptions::new(
            "127.0.0.1",
            49161,
            "system",
            "oracle",
            "free",
        )),
        "sqlite" => ConnectionOptions::Sqlite(PathBuf::from(format!(
            "{}/testdata/sqlite3.db",
            env!("CARGO_MANIFEST_DIR")
        ))),
        _ => panic!("database {database} not supported"),
    }
}
