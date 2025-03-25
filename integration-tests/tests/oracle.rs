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
        r#"+-------------+--------------+------------+------------+---------------------+----------------------------+
| BOOLEAN_COL | VARCHAR2_COL | CHAR_COL   | NUMBER_COL | DATE_COL            | TIMESTAMP_COL              |
+-------------+--------------+------------+------------+---------------------+----------------------------+
| true        | varchar2     | char       | 1.10       | 2003-05-03T21:02:44 | 2023-10-01T14:30:45.123456 |
|             |              |            |            |                     |                            |
+-------------+--------------+------------+------------+---------------------+----------------------------+"#,
    );
}
