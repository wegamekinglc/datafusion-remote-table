use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    ConnectionOptions, PostgresConnectionOptions, RemotePhysicalCodec, RemoteTable,
};
use integration_tests::shared_containers::setup_shared_containers;
use std::sync::Arc;

#[tokio::test]
pub async fn supported_postgres_types() {
    setup_shared_containers();
    let options = ConnectionOptions::Postgres(
        PostgresConnectionOptions::new("localhost", 5432, "postgres", "password")
            .with_database(Some("postgres".to_string())),
    );
    let table = RemoteTable::try_new(options, "select * from supported_data_types", None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx
        .sql("SELECT * FROM remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{}", pretty_format_batches(result.as_slice()).unwrap());

    assert_eq!(
        &pretty_format_batches(&result).unwrap().to_string(),
        r#"+--------------+-------------+------------+----------+------------+----------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+--------------------+-------------------+------------------+----------------+------------------+----------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | char_col | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | boolean_col | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | text_array_col |
+--------------+-------------+------------+----------+------------+----------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+--------------------+-------------------+------------------+----------------+------------------+----------------+
| 1            | 2           | 3          | 1.1      | 2.2        | char     | varchar     | bpchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | true        | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [text0, text1] |
|              |             |            |          |            |          |             |            |          |           |            |          |                     |                      |             |                    |                   |                  |                |                  |                |
+--------------+-------------+------------+----------+------------+----------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+--------------------+-------------------+------------------+----------------+------------------+----------------+"#,
    );

    let result = ctx
        .sql("SELECT integer_col, char_col FROM remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{}", pretty_format_batches(result.as_slice()).unwrap());

    assert_eq!(
        &pretty_format_batches(&result).unwrap().to_string(),
        "+-------------+----------+
| integer_col | char_col |
+-------------+----------+
| 2           | char     |
|             |          |
+-------------+----------+"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn exec_plan_serialization() {
    setup_shared_containers();
    let options = ConnectionOptions::Postgres(
        PostgresConnectionOptions::new("localhost", 5432, "postgres", "password")
            .with_database(Some("postgres".to_string())),
    );
    let table = RemoteTable::try_new(options, "select * from simple_table", None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let codec = RemotePhysicalCodec::new(None);
    let plan = ctx.sql("SELECT * FROM remote_table").await.unwrap();
    let exec_plan = plan.create_physical_plan().await.unwrap();

    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec).unwrap();
    plan_proto.try_encode(&mut plan_buf).unwrap();

    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec))
        .unwrap();

    let result = collect(new_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        "+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"
    )
}
