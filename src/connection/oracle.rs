#[derive(Debug, Clone)]
pub struct OracleConnectionOptions {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}
