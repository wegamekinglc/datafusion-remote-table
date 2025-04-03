use integration_tests::shared_containers::setup_shared_containers;
use integration_tests::utils::{assert_plan_and_result, assert_result, assert_sqls};

#[tokio::test]
pub async fn supported_oracle_types() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_result(
        "oracle",
        "SELECT * from SYS.supported_data_types",
        "SELECT * FROM remote_table",
        r#"+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| BOOLEAN_COL | SMALLINT_COL | INTEGER_COL | BINARY_FLOAT_COL | BINARY_DOUBLE_COL | NUMBER_COL | REAL_COL | FLOAT_COL | VARCHAR2_COL | NVARCHAR2_COL | CHAR_COL   | NCHAR_COL  | CLOB_COL | NCLOB_COL | RAW_COL | LONG_RAW_COL     | BLOB_COL | DATE_COL            | TIMESTAMP_COL              |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| true        | 1            | 2           | 1.1              | 2.2               | 3.30       | 4.4      | 5.5       | varchar2     | nvarchar2     | char       | nchar      | clob     | nclob     | 726177  | 6c6f6e6720726177 | 626c6f62 | 2003-05-03T21:02:44 | 2023-10-01T14:30:45.123456 |
|             |              |             |                  |                   |            |          |           |              |               |            |            |          |           |         |                  |          |                     |                            |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+"#,
    ).await;
}

// ORA-01754: a table may contain only one column of type LONG
#[tokio::test]
pub async fn supported_oracle_types2() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_result(
        "oracle",
        "SELECT * from SYS.supported_data_types2",
        "SELECT * FROM remote_table",
        r#"+----------+
| LONG_COL |
+----------+
| long     |
|          |
+----------+"#,
    )
    .await;
}

#[tokio::test]
pub async fn various_sqls() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    assert_sqls("oracle", vec!["select * from USER_TABLES"]).await;
}

#[tokio::test]
async fn pushdown_limit() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_plan_and_result(
        "oracle",
        "select * from SYS.simple_table",
        "select * from remote_table limit 1",
        "RemoteTableExec: limit=Some(1), filters=[]\n",
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test]
async fn pushdown_filters() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    assert_plan_and_result(
        "oracle",
        "select * from SYS.simple_table",
        r#"select * from remote_table where "ID" = 1"#,
        "CoalesceBatchesExec: target_batch_size=8192
  FilterExec: ID@0 = Some(1),38,0
    RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
      RemoteTableExec: limit=None, filters=[]\n",
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}
