use integration_tests::utils::assert_result;

#[tokio::test]
pub async fn supported_sqlite_types() {
    assert_result(
        "sqlite",
        "SELECT * from supported_data_types",
        "SELECT * FROM remote_table",
        "+----------+---------+----------+----------+----------+
| null_col | int_col | real_col | text_col | blob_col |
+----------+---------+----------+----------+----------+
|          | 1       | 1.1      | text     | 010203   |
|          |         |          |          |          |
+----------+---------+----------+----------+----------+",
    )
    .await;
}
