use crate::docker::DockerCompose;
use std::path::PathBuf;
use std::sync::LazyLock;

pub static SHARED_CONTAINERS: LazyLock<DockerCompose> = LazyLock::new(|| {
    let compose = DockerCompose::new(
        "shared_containers",
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );
    compose.down();
    compose.up();
    compose
});

pub fn setup_shared_containers() {
    let _ = SHARED_CONTAINERS.project_name();
}

pub fn setup_sqlite_db() -> PathBuf {
    let tmpdir = std::env::temp_dir();
    let db_path = tmpdir.join(uuid::Uuid::new_v4().to_string());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(include_str!("../testdata/sqlite_init.sql"))
        .unwrap();
    db_path
}

#[ctor::dtor]
fn shutdown() {
    SHARED_CONTAINERS.down();
}
