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
    let table = RemoteTable::try_new(options, "select * from supported_data_types")
        .await
        .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

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
        r#"+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+---------------------+----------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+
| tinyint_col | tinyint_unsigned_col | smallint_col | smallint_unsigned_col | integer_col | integer_unsigned_col | mediumint_col | mediumint_unsigned_col | bigint_col | bigint_unsigned_col | float_col | double_col | decimal_col  | date_col   | datetime_col        | time_col | timestamp_col       | year_col | char_col | varchar_col | binary_col           | varbinary_col | tinytext_col | text_col | mediumtext_col | longtext_col | tinyblob_col | blob_col | mediumblob_col | longblob_col | json_col         | geometry_col                                       |
+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+---------------------+----------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+
| 1           | 2                    | 3            | 4                     | 5           | 6                    | 7             | 8                      | 9          | 10                  | 1.1       | 2.2        | 3.3300000000 | 2025-03-14 | 2025-03-14T17:36:25 | 11:11:11 | 2025-03-14T11:11:11 | 1999     | char     | varchar     | 01000000000000000000 | 02            | tinytext     | text     | mediumtext     | longtext     | 01           | 02       | 03             | 04           | {"key": "value"} | 0000000001010000000000000000002e400000000000003440 |
|             |                      |              |                       |             |                      |               |                        |            |                     |           |            |              |            |                     |          |                     |          |          |             |                      |               |              |          |                |              |              |          |                |              |                  |                                                    |
+-------------+----------------------+--------------+-----------------------+-------------+----------------------+---------------+------------------------+------------+---------------------+-----------+------------+--------------+------------+---------------------+----------+---------------------+----------+----------+-------------+----------------------+---------------+--------------+----------+----------------+--------------+--------------+----------+----------------+--------------+------------------+----------------------------------------------------+"#,
    )
}

#[tokio::test]
pub async fn describe_table() {
    setup_shared_containers();
    // Wait for the database to be ready to connect
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    let options = ConnectionOptions::Mysql(
        MysqlConnectionOptions::new("127.0.0.1", 3306, "root", "password")
            .with_database(Some("test".to_string())),
    );
    let table = RemoteTable::try_new(options, "describe simple_table")
        .await
        .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

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
        r#"+-------+--------------------------+------+--------+---------+-------+
| Field | Type                     | Null | Key    | Default | Extra |
+-------+--------------------------+------+--------+---------+-------+
| id    | 696e74                   | NO   | 505249 |         |       |
| name  | 766172636861722832353529 | NO   |        |         |       |
+-------+--------------------------+------+--------+---------+-------+"#,
    )
}
