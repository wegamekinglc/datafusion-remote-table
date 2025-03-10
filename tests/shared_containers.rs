use std::sync::{LazyLock, Once};
use super::utils::docker::DockerCompose;

static SHARED_CONTAINERS: LazyLock<DockerCompose> = LazyLock::new(|| {
    DockerCompose::new("shared_containers", format!("{}/tests/testdata", env!("CARGO_MANIFEST_DIR")))
});

#[ctor::ctor]
fn setup() {
    // https://github.com/mmastrac/rust-ctor?tab=readme-ov-file#warnings
    // The code that runs in the ctor and dtor functions should be careful
    // to limit itself to libc functions and code that does not rely on
    // Rust's stdlib services.
    SHARED_CONTAINERS.down();
    SHARED_CONTAINERS.up();
}

#[ctor::dtor]
fn teardown() {
    SHARED_CONTAINERS.down();
}

pub fn setup_v2() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::init();
        let compose = DockerCompose::new("shared_containers", format!("{}/tests/testdata", env!("CARGO_MANIFEST_DIR")));
        compose.down();
        compose.up();
    });
}