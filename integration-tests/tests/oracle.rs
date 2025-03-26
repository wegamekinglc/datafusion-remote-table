use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, OracleConnectionOptions, RemoteTable};
use integration_tests::shared_containers::setup_shared_containers;
use std::sync::Arc;

#[tokio::test]
pub async fn supported_oracle_types() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    let options = ConnectionOptions::Oracle(OracleConnectionOptions::new(
        "127.0.0.1",
        49161,
        "system",
        "oracle",
        "free",
    ));
    let table = RemoteTable::try_new(options, "SELECT * from SYS.supported_data_types")
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
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| BOOLEAN_COL | SMALLINT_COL | INTEGER_COL | BINARY_FLOAT_COL | BINARY_DOUBLE_COL | NUMBER_COL | REAL_COL | FLOAT_COL | VARCHAR2_COL | NVARCHAR2_COL | CHAR_COL   | NCHAR_COL  | CLOB_COL | NCLOB_COL | RAW_COL | LONG_RAW_COL     | BLOB_COL | DATE_COL            | TIMESTAMP_COL              |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| true        | 1            | 2           | 1.1              | 2.2               | 3.30       | 4.4      | 5.5       | varchar2     | nvarchar2     | char       | nchar      | clob     | nclob     | 726177  | 6c6f6e6720726177 | 626c6f62 | 2003-05-03T21:02:44 | 2023-10-01T14:30:45.123456 |
|             |              |             |                  |                   |            |          |           |              |               |            |            |          |           |         |                  |          |                     |                            |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+"#,
    );
}

// ORA-01754: a table may contain only one column of type LONG
#[tokio::test]
pub async fn supported_oracle_types2() {
    setup_shared_containers();
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    let options = ConnectionOptions::Oracle(OracleConnectionOptions::new(
        "127.0.0.1",
        49161,
        "system",
        "oracle",
        "free",
    ));
    let table = RemoteTable::try_new(options, "SELECT * from SYS.supported_data_types2")
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
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+----------+
| LONG_COL |
+----------+
| long     |
|          |
+----------+"#,
    );
}
