use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};
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
