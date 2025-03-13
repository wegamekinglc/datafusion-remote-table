use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::ConnectionOptions;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
pub async fn supported_sqlite_types() {
    let options = ConnectionOptions::Sqlite(PathBuf::from(format!(
        "{}/testdata/sqlite3.db",
        env!("CARGO_MANIFEST_DIR")
    )));
    let table = datafusion_remote_table::RemoteTable::try_new(
        options,
        "SELECT * from supported_data_types",
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

    assert_eq!(
        pretty_format_batches(result.as_slice())
            .unwrap()
            .to_string(),
        "+----------+---------+----------+----------+----------+
| null_col | int_col | real_col | text_col | blob_col |
+----------+---------+----------+----------+----------+
|          | 1       | 1.1      | text     | 010203   |
+----------+---------+----------+----------+----------+"
    );
}
