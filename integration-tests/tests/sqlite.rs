use integration_tests::utils::assert_result;

#[tokio::test]
pub async fn supported_sqlite_types() {
    assert_result(
        "sqlite",
        "select * from supported_data_types",
        "select * from remote_table",
        r#"+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+
| null_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | real_col | char_col | varchar_col | text_col | binary_col | varbinary_col | blob_col |
+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+
|          | 1           | 2            | 3       | 4          | 1.1       | 2.2        | 3.3      | char     | varchar     | text     | 01         | 02            | 03       |
|          |             |              |         |            |           |            |          |          |             |          |            |               |          |
+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+"#,
    )
    .await;
}

#[tokio::test]
async fn pushdown_limit() {
    assert_result(
        "sqlite",
        "select * from simple_table",
        "select * from remote_table limit 1",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}
