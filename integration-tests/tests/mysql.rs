use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, MysqlConnectionOptions, RemoteTable};
use integration_tests::shared_containers::setup;
use std::sync::Arc;

#[tokio::test]
pub async fn all_supported_mysql_types() {
    setup();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
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
        "+-------------+--------------+-------------+------------+-----------+------------+
| tinyint_col | smallint_col | integer_col | bigint_col | float_col | double_col |
+-------------+--------------+-------------+------------+-----------+------------+
| 1           | 2            | 3           | 4          | 1.1       | 2.2        |
+-------------+--------------+-------------+------------+-----------+------------+"
    )
}
