# datafusion-remote-table
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/datafusion-remote-table.svg)](https://crates.io/crates/datafusion-remote-table)

## Goals
1. Execute SQL queries on remote databases and make results as datafusion table provider
2. Execution plan can be serialized for distributed execution
3. Record batches can be transformed before outputting to next operator

## Usage
```rust
#[tokio::main]
pub async fn main() {
    let options = ConnectionOptions::Postgres(PostgresConnectionOptions::new(
        "localhost",
        5432,
        "user",
        "password",
    ));
    let remote_table = RemoteTable::try_new(options, "SELECT * from supported_data_types", None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))
        .unwrap();

    ctx.sql("SELECT * from remote_table")
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}
```

## Supported databases
- [x] Postgres
- [x] MySQL
- [x] Oracle
- [x] SQLite

## Thanks
- [datafusion-table-providers](https://crates.io/crates/datafusion-table-providers)