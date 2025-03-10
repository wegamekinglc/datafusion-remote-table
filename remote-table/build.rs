use std::path::Path;

fn main() -> Result<(), String> {
    use std::io::Write;

    let proto_path = Path::new("proto/remote_table.proto");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rerun-if-changed=proto/remote_table.proto");

    let path = "src/generated/prost.rs";

    prost_build::Config::new()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_well_known_types()
        .compile_protos(&[proto_path], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let generated_source_path = out.join("remote_table.rs");
    let code = std::fs::read_to_string(generated_source_path).unwrap();
    let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();
    file.write_all(code.as_str().as_ref()).unwrap();

    Ok(())
}