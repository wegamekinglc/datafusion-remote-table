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
        "+--------------+-------------+------------+------------+---------------+----------+-------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+-------------------+----------------+
| smallint_col | integer_col | bigint_col | serial_col | bigserial_col | char_col | varchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | boolean_col | integer_array_col | text_array_col |
+--------------+-------------+------------+------------+---------------+----------+-------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+-------------------+----------------+
| 1            | 2           | 3          | 4          | 5             | char     | varchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | true        | [1, 2]            | [text0, text1] |
+--------------+-------------+------------+------------+---------------+----------+-------------+----------+-----------+------------+----------+---------------------+----------------------+-------------+-------------------+----------------+
"
    )
}
