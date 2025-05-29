use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, RemoteDbType, RemoteTable, SqliteConnectionOptions,
};
use integration_tests::shared_containers::setup_sqlite_db;
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn supported_sqlite_types() {
    assert_result(
        RemoteDbType::Sqlite,
        "select * from supported_data_types",
        "select * from remote_table",
        r#"+-------------+--------------+---------+------------+-----------+------------+----------+--------------------+--------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | real_col | real_precision_col | real_precision_scale_col | char_col | char_len_col | varchar_col | varchar_len_col | text_col | text_len_col | binary_col | binary_len_col | varbinary_col | varbinary_len_col | blob_col |
+-------------+--------------+---------+------------+-----------+------------+----------+--------------------+--------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| 1           | 2            | 3       | 4          | 1.1       | 2.2        | 3.3      | 4.4                | 5.5                      | char     | char(10)     | varchar     | varchar(120)    | text     | text(200)    | 01         | 02             | 03            | 04                | 05       |
|             |              |         |            |           |            |          |                    |                          |          |              |             |                 |          |              |            |                |               |                   |          |
+-------------+--------------+---------+------------+-----------+------------+----------+--------------------+--------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+"#,
    )
    .await;

    assert_result(
        RemoteDbType::Sqlite,
        "select count(1) from supported_data_types",
        "select * from remote_table",
        r#"+----------+
| count(1) |
+----------+
| 2        |
+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn streaming_execution() {
    let db_path = setup_sqlite_db();
    let options = ConnectionOptions::Sqlite(
        SqliteConnectionOptions::new(db_path).with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, "select * from simple_table")
        .await
        .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from remote_table").await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+----+-------+
| id | name  |
+----+-------+
| 1  | Tom   |
| 2  | Jerry |
| 3  | Spike |
+----+-------+"#,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_limit() {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        "select * from simple_table",
        "select * from remote_table limit 1",
        "RemoteTableExec: limit=Some(1), filters=[]\n",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_filters() {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        "select * from simple_table",
        "select * from remote_table where id = 1",
        "RemoteTableExec: limit=None, filters=[(`id` = 1)]\n",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn count1_agg() {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        "select * from simple_table",
        "select count(*) from remote_table",
        "ProjectionExec: expr=[3 as count(*)]\n  PlaceholderRowExec\n",
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#,
    )
    .await;
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// async fn empty_projection() {
//     let options = build_conn_options(RemoteDbType::Sqlite);
//     let table = RemoteTable::try_new(options, "select * from simple_table")
//         .await
//         .unwrap();
//
//     let config = SessionConfig::new().with_target_partitions(12);
//     let ctx = SessionContext::new_with_config(config);
//
//     let df = ctx.read_table(Arc::new(table)).unwrap();
//     let df = df.select_columns(&[]).unwrap();
//
//     let exec_plan = df.create_physical_plan().await.unwrap();
//     println!(
//         "{}",
//         DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
//     );
//     let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
//     println!("{}", pretty_format_batches(&result).unwrap());
//     assert_eq!(result.len(), 0);
// }
