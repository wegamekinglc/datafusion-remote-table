use datafusion::prelude::SessionContext;
use datafusion_remote_table::{ConnectionOptions, PostgresConnectionOptions, RemoteTable};
use std::sync::Arc;
use datafusion::execution::options::CsvReadOptions;
use tokio::time::Instant;


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions::new(
        "localhost",
        5432,
        "postgres",
        "we083826",
    ));

    let remote_table = RemoteTable::try_new(options,
                                                    "select foo, bar, id from public.pg_test_data").await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;
    ctx.register_csv("csv_table",
                     "D:/dev/github/datafusion-flight-sql-server/datafusion-flight-sql-server/examples/test.csv",
                     CsvReadOptions::new()).await?;

    let sql = "select count(1) from remote_table as a, csv_table as b where a.foo = 'a' AND a.foo = b.foo";

    // warm up
    ctx.sql(sql).await?;

    // time the execution
    let start = Instant::now();
    ctx.sql(sql).await?.show().await?;
    let duration = start.elapsed();
    println!("Time elapsed in query is: {:?}", duration);

    Ok(())
}
