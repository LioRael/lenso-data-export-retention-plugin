#[allow(dead_code)]
#[path = "src/contract.rs"]
mod contract_source;

#[path = "../../contract_build_support.rs"]
mod support;

fn main() {
    support::run(
        &contract_source::__lenso_capability_snapshot(),
        "Data Export",
    );
}
