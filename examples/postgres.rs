use datafusion_remote_table::{connect, ConnectionArgs};

#[tokio::main]
pub async fn main() {
    let conn_args = ConnectionArgs::Postgresql {
        host: "127.0.0.1".to_string(),
        port: 33448,
        username: "postgres".to_string(),
        password: "".to_string(),
        database: None,
    };
    let conn = connect(&conn_args).await.unwrap();
    let schema = conn
        .infer_schema("SELECT * from lwz_remote_test ")
        .await
        .unwrap();
    println!("{:?}", schema);
}
