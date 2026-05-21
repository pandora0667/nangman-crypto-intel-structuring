use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

const SCHEMA_DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn read_json(relative: &str) -> Value {
    let path = repo_path(relative);
    let bytes = fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

#[test]
fn json_schemas_are_strict_draft_2020_12_contracts() {
    let expected_versions = [
        (
            "schemas/raw_intel_event_created_v2.schema.json",
            "raw_intel_event_created_v2",
        ),
        (
            "schemas/structured_intel_packet_v1.schema.json",
            "structured_intel_packet_v1",
        ),
        (
            "schemas/context_flag_packet_v1.schema.json",
            "context_flag_packet_v1",
        ),
        ("schemas/story_cluster_v1.schema.json", "story_cluster_v1"),
        (
            "schemas/structuring_health_event_v1.schema.json",
            "structuring_health_event_v1",
        ),
        (
            "schemas/intel_l1_manifest_v1.schema.json",
            "intel_l1_manifest_v1",
        ),
        (
            "schemas/intel_l1_index_pointer_v1.schema.json",
            "intel_l1_index_pointer_v1",
        ),
        (
            "schemas/intel_l1_packet_revision_index_v1.schema.json",
            "intel_l1_packet_revision_index_v1",
        ),
        (
            "schemas/structured_pointer_v1.schema.json",
            "structured_pointer_v1",
        ),
    ];

    for (schema_file, schema_version) in expected_versions {
        let schema = read_json(schema_file);
        assert_eq!(schema["$schema"], SCHEMA_DRAFT_2020_12, "{schema_file}");
        assert_eq!(schema["type"], "object", "{schema_file}");
        assert_eq!(schema["additionalProperties"], false, "{schema_file}");
        assert!(
            schema["required"]
                .as_array()
                .expect("required must be array")
                .iter()
                .any(|field| field == "schema_version"),
            "{schema_file} must require schema_version"
        );
        assert_eq!(
            schema["properties"]["schema_version"]["const"], schema_version,
            "{schema_file}"
        );
    }
}

#[test]
fn asyncapi_contract_tracks_nats_subjects_and_payload_schemas() {
    let contract = read_json("asyncapi/nats.asyncapi.json");
    assert_eq!(contract["asyncapi"], "3.0.0");

    let channels = contract["channels"].as_object().expect("channels object");
    let subjects = [
        "raw_intel_event.created",
        "structured_intel_packet.created",
        "context_flag_packet.created",
        "structuring_health_event.created",
    ];
    for subject in subjects {
        assert!(
            channels
                .values()
                .any(|channel| channel["address"].as_str() == Some(subject)),
            "missing subject {subject}"
        );
    }

    let operations = contract["operations"]
        .as_object()
        .expect("operations object");
    for operation in [
        "consumeRawIntelEventCreated",
        "publishStructuredIntelPacketCreated",
        "publishContextFlagPacketCreated",
        "publishStructuringHealthEventCreated",
    ] {
        assert!(
            operations.contains_key(operation),
            "missing operation {operation}"
        );
    }
}

#[test]
fn machine_contracts_do_not_expose_trading_decision_semantics() {
    let contract_files = [
        "asyncapi/nats.asyncapi.json",
        "schemas/raw_intel_event_created_v2.schema.json",
        "schemas/structured_intel_packet_v1.schema.json",
        "schemas/context_flag_packet_v1.schema.json",
        "schemas/story_cluster_v1.schema.json",
        "schemas/structuring_health_event_v1.schema.json",
        "schemas/intel_l1_manifest_v1.schema.json",
        "schemas/intel_l1_index_pointer_v1.schema.json",
        "schemas/intel_l1_packet_revision_index_v1.schema.json",
        "schemas/structured_pointer_v1.schema.json",
    ];
    let forbidden = ["position_size", "live_ready", "execution_approved"];

    for file in contract_files {
        let body = fs::read_to_string(repo_path(file))
            .unwrap_or_else(|error| panic!("read {file}: {error}"));
        for term in forbidden {
            assert!(
                !body.contains(term),
                "{file} contains forbidden term {term}"
            );
        }
    }
}
