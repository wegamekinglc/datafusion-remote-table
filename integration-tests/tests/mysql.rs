use datafusion_remote_table::RemoteDbType;
use integration_tests::shared_containers::setup_shared_containers;
use integration_tests::utils::{assert_plan_and_result, assert_result, assert_sqls};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn supported_mysql_types() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_result(
        RemoteDbType::Mysql,
        "select * from supported_data_types",
        "SELECT * FROM remote_table",
        r#"+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+----------------------+----------+----------+-------------+----------------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+
| tinyint_col | tinyint_unsigned_col | smallint_col | smallint_unsigned_col | integer_col | integer_unsigned_col | mediumint_col | mediumint_unsigned_col | bigint_col | bigint_unsigned_col | float_col | double_col | decimal_col  | date_col   | datetime_col        | time_col | timestamp_col        | year_col | char_col | varchar_col | varchar_utf8_bin_col | binary_col           | varbinary_col | tinytext_col | text_col | mediumtext_col | longtext_col | tinyblob_col | blob_col | mediumblob_col | longblob_col | json_col         | geometry_col                                       |
+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+----------------------+----------+----------+-------------+----------------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+
| 1           | 2                    | 3            | 4                     | 5           | 6                    | 7             | 8                      | 9          | 10                  | 1.1       | 2.2        | 3.3300000000 | 2025-03-14 | 2025-03-14T17:36:25 | 11:11:11 | 2025-03-14T11:11:11Z | 1999     | char     | varchar     | varchar_utf8_bin     | 01000000000000000000 | 02            | tinytext     | text     | mediumtext     | longtext     | 01           | 02       | 03             | 04           | {"key": "value"} | 0000000001010000000000000000002e400000000000003440 |
|             |                      |              |                       |             |                      |               |                        |            |                     |           |            |              |            |                     |          |                      |          |          |             |                      |                      |               |              |          |                |              |              |          |                |              |                  |                                                    |
+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+----------------------+----------+----------+-------------+----------------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+"#,
    ).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn describe_table() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_result(
        RemoteDbType::Mysql,
        "describe simple_table",
        "SELECT * FROM remote_table",
        r#"+-------+--------------+------+-----+---------+-------+
| Field | Type         | Null | Key | Default | Extra |
+-------+--------------+------+-----+---------+-------+
| id    | int          | NO   | PRI |         |       |
| name  | varchar(255) | NO   |     |         |       |
+-------+--------------+------+-----+---------+-------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn various_sqls() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    assert_sqls(
        RemoteDbType::Mysql,
        vec!["select * from mysql.innodb_table_stats"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_limit() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    assert_result(
        RemoteDbType::Mysql,
        "select * from simple_table",
        "select * from remote_table limit 1",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;

    // should not push down limit
    assert_result(
        RemoteDbType::Mysql,
        "describe simple_table",
        "SELECT * FROM remote_table limit 1",
        r#"+-------+------+------+-----+---------+-------+
| Field | Type | Null | Key | Default | Extra |
+-------+------+------+-----+---------+-------+
| id    | int  | NO   | PRI |         |       |
+-------+------+------+-----+---------+-------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_filters() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    assert_plan_and_result(
        RemoteDbType::Mysql,
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

    // should not push down filters
    assert_plan_and_result(
        RemoteDbType::Mysql,
        "describe simple_table",
        r#"SELECT * FROM remote_table where "Key" = 'PRI'"#,
        r#"CoalesceBatchesExec: target_batch_size=8192
  FilterExec: Key@3 = PRI
    RemoteTableExec: limit=None, filters=[]
"#,
        r#"+-------+------+------+-----+---------+-------+
| Field | Type | Null | Key | Default | Extra |
+-------+------+------+-----+---------+-------+
| id    | int  | NO   | PRI |         |       |
+-------+------+------+-----+---------+-------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn count1_agg() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    assert_plan_and_result(
        RemoteDbType::Mysql,
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
