use datafusion_remote_table::RemoteDbType;
use integration_tests::utils::{assert_plan_and_result, assert_result};

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn pushdown_filters() {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        "select * from simple_table",
        "select * from remote_table where id = 1",
        "RemoteTableExec: limit=None, filters=[id = Int64(1)]\n",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}
