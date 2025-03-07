mod utils;

use std::sync::LazyLock;
use utils::DockerCompose;

static SHARED_CONTAINERS: LazyLock<DockerCompose> = LazyLock::new(|| {
    DockerCompose::new("shared_containers", format!("{}/tests/testdata", env!("CARGO_MANIFEST_DIR")))
});

#[ctor::ctor]
fn setup() {
    SHARED_CONTAINERS.down();
    SHARED_CONTAINERS.up();
}

#[ctor::dtor]
fn teardown() {
    SHARED_CONTAINERS.down();
}