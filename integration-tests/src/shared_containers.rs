use crate::docker::DockerCompose;
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

#[ctor::dtor]
fn shutdown() {
    SHARED_CONTAINERS.down();
}
