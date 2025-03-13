use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, OracleConnectionOptions};
use integration_tests::shared_containers::setup;
use std::sync::Arc;

#[tokio::test]
pub async fn all_supported_oracle_types() {
    setup();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let options = ConnectionOptions::Oracle(OracleConnectionOptions::new(
        "127.0.0.1",
        49161,
        "system",
        "oracle",
        "xe",
    ));
    let table = datafusion_remote_table::RemoteTable::try_new(
        options,
        "SELECT * from SYS.supported_data_types",
        None,
    )
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
        pretty_format_batches(result.as_slice())
            .unwrap()
            .to_string(),
        "+--------------+
| VARCHAR2_COL |
+--------------+
| varchar2     |
+--------------+"
    );
}
