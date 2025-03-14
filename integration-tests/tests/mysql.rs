use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, MysqlConnectionOptions, RemoteTable};
use integration_tests::shared_containers::setup_shared_containers;
use std::sync::Arc;

#[tokio::test]
pub async fn supported_mysql_types() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    let options = ConnectionOptions::Mysql(
        MysqlConnectionOptions::new("127.0.0.1", 3306, "root", "password")
            .with_database(Some("test".to_string())),
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
        "+-------------+--------------+-------------+---------------+------------+-----------+------------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+
| tinyint_col | smallint_col | integer_col | mediumint_col | bigint_col | float_col | double_col | char_col | varchar_col | binary_col           | varbinary_col | tinytext_col | text_col | mediumtext_col | longtext_col | tinyblob_col | blob_col | mediumblob_col | longblob_col |
+-------------+--------------+-------------+---------------+------------+-----------+------------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+
| 1           | 2            | 3           | 4             | 5          | 1.1       | 2.2        | char     | varchar     | 01000000000000000000 | 02            | tinytext     | text     | mediumtext     | longtext     | 01           | 02       | 03             | 04           |
|             |              |             |               |            |           |            |          |             |                      |               |              |          |                |              |              |          |                |              |
+-------------+--------------+-------------+---------------+------------+-----------+------------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+"
    )
}
