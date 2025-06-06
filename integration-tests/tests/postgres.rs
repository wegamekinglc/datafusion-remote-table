use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{RemoteDbType, RemoteTable};
use integration_tests::shared_containers::setup_shared_containers;
use integration_tests::utils::{
    assert_plan_and_result, assert_result, assert_sqls, build_conn_options,
};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn supported_postgres_types() {
    setup_shared_containers();
    assert_result(
        RemoteDbType::Postgres,
        "SELECT * from supported_data_types",
        "SELECT * FROM remote_table",
        r#"+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | interval_col   | boolean_col | json_col          | jsonb_col         | geometry_col                                       | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | char_array_col           | varchar_array_col    | bpchar_array_col   | text_array_col | bool_array_col |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.30        | char       | varchar     | bpchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | 010100002038010000000000000000f03f000000000000f03f | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [char0     , char1     ] | [varchar0, varchar1] | [bpchar0, bpchar1] | [text0, text1] | [true, false]  |
|              |             |            |          |            |             |            |             |            |          |           |            |          |                     |                      |                |             |                   |                   |                                                    |                    |                   |                  |                |                  |                          |                      |                    |                |                |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+"#,
    ).await;

    assert_result(
        RemoteDbType::Postgres,
        "SELECT * from supported_data_types",
        "SELECT integer_col, varchar_col FROM remote_table",
        r#"+-------------+-------------+
| integer_col | varchar_col |
+-------------+-------------+
| 2           | varchar     |
|             |             |
+-------------+-------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn table_columns() {
    setup_shared_containers();
    let sql = format!(
        r#"
        SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    t.typname AS udt_name
FROM
    pg_catalog.pg_attribute a
JOIN
    pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN
    pg_catalog.pg_type t ON a.atttypid = t.oid
WHERE
    c.relname = '{}'
    AND c.relkind IN ('r', 'v', 'm')
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum
        "#,
        "simple_table"
    );
    assert_result(
        RemoteDbType::Postgres,
        &sql,
        "SELECT * FROM remote_table",
        r#"+-------------+------------------------+-------------+----------+
| column_name | data_type              | is_nullable | udt_name |
+-------------+------------------------+-------------+----------+
| id          | integer                | NO          | int4     |
| name        | character varying(255) | NO          | varchar  |
+-------------+------------------------+-------------+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn various_sqls() {
    setup_shared_containers();

    assert_sqls(
        RemoteDbType::Postgres,
        vec!["select * from pg_catalog.pg_stat_all_tables"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_limit() {
    setup_shared_containers();
    assert_plan_and_result(
        RemoteDbType::Postgres,
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
    setup_shared_containers();
    assert_plan_and_result(
        RemoteDbType::Postgres,
        "select * from simple_table",
        "select * from remote_table where id = 1",
        "RemoteTableExec: limit=None, filters=[(\"id\" = 1)]\n",
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
    setup_shared_containers();
    assert_plan_and_result(
        RemoteDbType::Postgres,
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

    assert_plan_and_result(
        RemoteDbType::Postgres,
        "select * from simple_table",
        "select count(*) from remote_table where id > 1",
        "ProjectionExec: expr=[2 as count(*)]\n  PlaceholderRowExec\n",
        r#"+----------+
| count(*) |
+----------+
| 2        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Postgres,
        "select * from simple_table",
        "select count(*) from (select * from remote_table where id > 1 limit 1)",
        "ProjectionExec: expr=[1 as count(*)]\n  PlaceholderRowExec\n",
        r#"+----------+
| count(*) |
+----------+
| 1        |
+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn empty_projection() {
    setup_shared_containers();

    let options = build_conn_options(RemoteDbType::Postgres);
    let table = RemoteTable::try_new(options, "select * from simple_table")
        .await
        .unwrap();

    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(Arc::new(table)).unwrap();
    let df = df.select_columns(&[]).unwrap();

    let exec_plan = df.create_physical_plan().await.unwrap();
    let plan_display = DisplayableExecutionPlan::new(exec_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{plan_display}");
    assert_eq!(plan_display, "RemoteTableExec: limit=None, filters=[]\n");

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}
