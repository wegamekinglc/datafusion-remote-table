use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    ConnectionOptions, PostgresConnectionOptions, RemotePhysicalCodec, RemoteTable,
};
use integration_tests::shared_containers::setup;
use std::sync::Arc;

#[tokio::test]
pub async fn all_supported_postgres_types() {
    setup();
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions {
        host: "localhost".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: "password".to_string(),
        database: Some("postgres".to_string()),
    });
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
    assert_eq!(&pretty_format_batches(&result).unwrap().to_string(),
               "+-----------------+----------------+---------------+---------------+------------------+-------------+----------------+-------------+--------------+-------------+-------------+---------------------+----------------------+----------------+----------------------+-------------------+
| smallint_column | integer_column | bigint_column | serial_column | bigserial_column | char_column | varchar_column | text_column | bytea_column | date_column | time_column | timestamp_column    | timestamptz_column   | boolean_column | integer_array_column | text_array_column |
+-----------------+----------------+---------------+---------------+------------------+-------------+----------------+-------------+--------------+-------------+-------------+---------------------+----------------------+----------------+----------------------+-------------------+
| 1               | 2              | 3             | 4             | 5                | char        | varchar        | text        | deadbeef     | 2023-10-01  | 12:34:56    | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | true           | [1, 2]               | [text0, text1]    |
+-----------------+----------------+---------------+---------------+------------------+-------------+----------------+-------------+--------------+-------------+-------------+---------------------+----------------------+----------------+----------------------+-------------------+")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn exec_plan_serialization() {
    setup();
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions {
        host: "localhost".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: "password".to_string(),
        database: Some("postgres".to_string()),
    });
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
