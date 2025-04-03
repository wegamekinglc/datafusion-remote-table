use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
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

    let df = ctx.sql(df_sql).await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        expected_result
    );
}

pub async fn assert_plan_and_result(
    database: &str,
    remote_sql: &str,
    df_sql: &str,
    expected_plan: &str,
    expected_result: &str,
) {
    let options = build_conn_options(database);
    let table = RemoteTable::try_new(options, remote_sql).await.unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql(df_sql).await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );
    assert_eq!(
        DisplayableExecutionPlan::new(exec_plan.as_ref())
            .indent(true)
            .to_string(),
        expected_plan
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
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

fn build_conn_options(database: &str) -> ConnectionOptions {
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
