use std::{env, path::Path};

use lenso_contract_codegen::{
    ProjectionLanguage, check_projection, check_source_snapshot, write_source_snapshot,
};

pub fn run(snapshot: &lenso_contract_authoring::CapabilitySnapshot, label: &str) {
    println!("cargo:rerun-if-changed=capability.json");
    println!("cargo:rerun-if-changed=schemas");
    println!("cargo:rerun-if-changed=src/contract.rs");
    println!("cargo:rerun-if-changed=src/generated.rs");
    println!("cargo:rerun-if-changed=../../contract_build_support.rs");
    println!("cargo:rerun-if-env-changed=LENSO_UPDATE_CONTRACT_SNAPSHOT");
    if env::var_os("LENSO_UPDATE_CONTRACT_SNAPSHOT").is_some() {
        write_source_snapshot(snapshot, Path::new("capability.json"))
            .unwrap_or_else(|error| panic!("failed to update {label} snapshot: {error}"));
    } else {
        check_source_snapshot(snapshot, Path::new("capability.json"))
            .unwrap_or_else(|error| panic!("{label} Descriptor or Schemas are stale: {error}"));
    }
    check_projection(
        Path::new("capability.json"),
        ProjectionLanguage::Rust,
        Path::new("src/generated.rs"),
    )
    .unwrap_or_else(|error| panic!("{label} generated Rust projection is stale: {error}"));
}
