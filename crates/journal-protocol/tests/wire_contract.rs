use std::{fmt::Debug, sync::LazyLock};

use journal_protocol::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

fn assert_required_fields<T>(example: Value, required: &[&str])
where
    T: DeserializeOwned + serde::Serialize + Debug,
{
    let bytes = serde_json::to_vec(&example).expect("serialize example");
    decode_json::<T>(&bytes).expect("positive wire example");

    for field in required {
        let mut missing = example.clone();
        missing
            .as_object_mut()
            .expect("object example")
            .remove(*field);
        let bytes = serde_json::to_vec(&missing).expect("serialize mutation");
        assert!(
            decode_json::<T>(&bytes).is_err(),
            "{field} must be required for {}",
            std::any::type_name::<T>()
        );
    }
}

fn wire_example(name: &str) -> &'static Value {
    static FIXTURE: LazyLock<Value> = LazyLock::new(|| {
        serde_json::from_str(include_str!(
            "../../../conformance/client/wire-examples.json"
        ))
        .expect("wire examples")
    });
    &FIXTURE["schemas"][name]
}

fn assert_example_round_trip<T>(name: &str)
where
    T: DeserializeOwned + serde::Serialize + PartialEq + Debug,
{
    let bytes = serde_json::to_vec(wire_example(name)).expect("serialize fixture example");
    let decoded = decode_json::<T>(&bytes).expect("decode fixture example");
    let encoded = encode_json(&decoded).expect("encode wire DTO");
    let round_trip = decode_json::<T>(&encoded).expect("decode encoded wire DTO");
    assert_eq!(decoded, round_trip, "{name}");
}

#[test]
fn public_space_policy_is_explicit_and_rejects_unsupported_values() {
    assert_required_fields::<SpaceCreateRequest>(
        json!({"id":"space","name":"Space","access":"public"}),
        &["id", "name", "access"],
    );
    assert_required_fields::<domain::Space>(
        wire_example("Space").clone(),
        &["id", "name", "access", "created_at", "limits"],
    );
    for value in [
        json!("private"),
        json!("PUBLIC"),
        json!(""),
        json!(null),
        json!(true),
        json!(1),
    ] {
        assert!(
            decode_json::<SpaceCreateRequest>(
                &serde_json::to_vec(&json!({"id":"space","name":"Space","access":value})).unwrap()
            )
            .is_err()
        );
        let mut response = wire_example("Space").clone();
        response["access"] = value;
        assert!(decode_json::<domain::Space>(&serde_json::to_vec(&response).unwrap()).is_err());
    }
}

#[test]
fn normative_wire_examples_round_trip_through_typed_dtos() {
    macro_rules! example {
        ($type:ty, $name:literal) => {
            assert_example_round_trip::<$type>($name);
        };
    }

    example!(AppendRecordRequest, "AppendRecordRequest");
    example!(ErrorBody, "Error");
    example!(ErrorResponse, "ErrorResponse");
    example!(PageInfo, "PageInfo");
    example!(Health, "Health");
    example!(OperationalMetrics, "OperationalMetrics");
    example!(domain::Principal, "Principal");
    example!(RegistrationRequest, "RegistrationRequest");
    example!(RegistrationReceipt, "RegistrationReceipt");
    example!(domain::Relation, "Relation");
    example!(Membership, "Membership");
    example!(domain::Limits, "Limits");
    example!(domain::Space, "Space");
    example!(domain::Record, "Record");
    example!(PrincipalPage, "PrincipalPage");
    example!(SpacePage, "SpacePage");
    example!(RecordPage, "RecordPage");
    example!(AppendRecordResponse, "AppendRecordResponse");
    example!(SearchResult, "SearchResult");
    example!(SearchPage, "SearchPage");
    example!(Me, "Me");
    example!(ReceiptSummary, "ReceiptSummary");
    example!(ReceiptStatusPage, "ReceiptStatusPage");
    example!(InboxItem, "InboxItem");
    example!(InboxPage, "InboxPage");
    example!(OneTimePrincipalClientSecret, "OneTimePrincipalClientSecret");
    example!(CredentialMetadata, "CredentialMetadata");
    example!(CredentialRotationResponse, "CredentialRotationResponse");
    example!(PrincipalRecoveryResponse, "PrincipalRecoveryResponse");
    example!(OneTimeReplacementSecret, "OneTimeReplacementSecret");
}

#[test]
fn bootstrap_recovery_requests_are_strict_and_rotation_secrets_are_redacted() {
    let revoke: CredentialRevokeRequest =
        serde_json::from_value(json!({"credential_id":"credential-1","reason":"recovery"}))
            .unwrap();
    revoke.validate().unwrap();
    assert!(
        CredentialRevokeRequest {
            credential_id: "credential-1".into(),
            reason: Some("界".repeat(513)),
        }
        .validate()
        .is_err()
    );
    let rotation: CredentialRotationResponse =
        serde_json::from_value(wire_example("CredentialRotationResponse").clone()).unwrap();
    assert!(!format!("{rotation:?}").contains("replacement-example"));
    assert_required_fields::<CredentialRotationResponse>(
        wire_example("CredentialRotationResponse").clone(),
        &["metadata", "replacement_secret"],
    );
    assert_required_fields::<OneTimeReplacementSecret>(
        wire_example("OneTimeReplacementSecret").clone(),
        &["credential_id", "secret"],
    );
    let principal_recovery = PrincipalRecoveryRequest {
        principal_id: "018f1f59-6e90-7000-8000-000000000001".into(),
        reason: Some("lost credential".into()),
    };
    principal_recovery.validate().unwrap();
    assert!(
        decode_json::<PrincipalRecoveryRequest>(
            br#"{"principal_id":"018f1f59-6e90-7000-8000-000000000001","reason":null}"#
        )
        .is_err()
    );
    let recovered: PrincipalRecoveryResponse =
        serde_json::from_value(wire_example("PrincipalRecoveryResponse").clone()).unwrap();
    assert!(!format!("{recovered:?}").contains("recovery-example"));
}

#[test]
fn strict_json_rejects_duplicate_keys_at_any_depth() {
    let top_level = br#"{"kind":"message","kind":"status","content":"x"}"#;
    let error = decode_json::<AppendRecordRequest>(top_level).expect_err("duplicate key");
    assert!(error.to_string().contains("duplicate object key \"kind\""));

    let nested = br#"{
        "kind":"message",
        "content":"x",
        "relations":[{"type":"reply-to","record_id":"record-1","record_id":"record-2"}]
    }"#;
    let error = decode_json::<AppendRecordRequest>(nested).expect_err("nested duplicate key");
    assert!(
        error
            .to_string()
            .contains("duplicate object key \"record_id\"")
    );
}

#[test]
fn request_bodies_reject_arrays_for_object_shapes() {
    assert!(
        decode_json::<RegistrationRequest>(br#"["alpha","Alpha"]"#).is_err(),
        "request objects must not accept positional arrays"
    );
    assert!(
        decode_json::<AppendRecordRequest>(
            br#"{"kind":"message","content":"x","relations":[["reply-to","record-1"]]}"#
        )
        .is_err(),
        "nested request objects must not accept positional arrays"
    );
}

#[test]
fn request_bodies_reject_unknown_fields_and_explicit_null_optionals() {
    assert!(
        decode_json::<AppendRecordRequest>(
            br#"{"kind":"message","content":"x","author":"caller-controlled"}"#
        )
        .is_err()
    );
    assert!(
        decode_json::<AppendRecordRequest>(br#"{"kind":"message","content":"x","run_id":null}"#)
            .is_err()
    );
    assert!(
        decode_json::<PrincipalRecoveryRequest>(
            br#"{"principal_id":"018f1f59-6e90-7000-8000-000000000001","reason":null}"#
        )
        .is_err()
    );
}

#[test]
fn path_headers_and_authentication_are_not_body_fields() {
    let body = AppendRecordRequest {
        kind: "message".into(),
        content: "hello".into(),
        run_id: None,
        attention: vec!["agent-beta".into()],
        routing_key: None,
        relations: Vec::new(),
        title: None,
    };
    let value = serde_json::to_value(&body).expect("append JSON");
    for forbidden in [
        "space",
        "idempotency_key",
        "principal_id",
        "credential_id",
        "author",
    ] {
        assert!(value.get(forbidden).is_none(), "unexpected {forbidden}");
    }

    let path = SpacePath {
        space: "project-alpha".into(),
    };
    let headers = AppendHeaders {
        idempotency_key: "append-1".into(),
    };
    let auth = PrincipalContext {
        principal_id: "agent-alpha".into(),
        credential_id: "credential-1".into(),
    };
    assert!(path.validate().is_ok());
    assert!(headers.validate().is_ok());
    assert_eq!(auth.principal_id, "agent-alpha");
}

#[test]
fn append_and_query_limits_use_utf8_bytes_and_exact_boundaries() {
    let exact = AppendRecordRequest {
        kind: "message".into(),
        content: "界".repeat((domain::MAX_CONTENT_BYTES - 1) / 3) + "a",
        run_id: None,
        attention: Vec::new(),
        routing_key: None,
        relations: Vec::new(),
        title: None,
    };
    assert_eq!(exact.content.len(), domain::MAX_CONTENT_BYTES);
    assert!(exact.validate().is_ok());

    let mut over = exact;
    over.content.push('a');
    assert!(over.validate().is_err());

    assert!(
        PageQuery::new(None, Some(domain::MAX_PAGE_SIZE))
            .validate()
            .is_ok()
    );
    assert!(
        PageQuery::new(None, Some(domain::MAX_PAGE_SIZE + 1))
            .validate()
            .is_err()
    );
    assert!(
        AppendHeaders {
            idempotency_key: "x".repeat(255)
        }
        .validate()
        .is_ok()
    );
    assert!(
        AppendHeaders {
            idempotency_key: "x".repeat(256)
        }
        .validate()
        .is_err()
    );
    let exact_reason = CredentialRotateRequest {
        credential_id: "credential-example".into(),
        reason: Some("x".repeat(512)),
    };
    assert!(exact_reason.validate().is_ok());
    assert!(
        CredentialRotateRequest {
            reason: Some("x".repeat(513)),
            ..exact_reason
        }
        .validate()
        .is_err()
    );

    let exact_query = SearchRecordsQuery {
        q: "x".repeat(512),
        page: PageQuery::default(),
        author: None,
        attention: None,
        since: None,
        order: SearchOrder::Rank,
    };
    assert!(exact_query.validate().is_ok());
    assert!(
        SearchRecordsQuery {
            q: "x".repeat(513),
            ..exact_query
        }
        .validate()
        .is_err()
    );
}

#[test]
fn wire_identifiers_use_openapi_character_limits() {
    let exact = PrincipalCreateRequest {
        handle: "界".repeat(128),
        display_name: "Agent".into(),
    };
    assert!(exact.validate().is_ok());
    assert!(
        PrincipalCreateRequest {
            handle: "界".repeat(129),
            ..exact
        }
        .validate()
        .is_err()
    );

    let profile = ProfileUpdateRequest {
        handle: "018f1f59-6e90-7000-8000-000000000009".into(),
        display_name: "Agent".into(),
        description: Some("x".repeat(512)),
        expected_profile_revision: 1,
    };
    assert!(profile.validate().is_ok());
    assert!(
        ProfileUpdateRequest {
            description: Some("x".repeat(513)),
            ..profile.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        ProfileUpdateRequest {
            expected_profile_revision: 0,
            ..profile
        }
        .validate()
        .is_err()
    );
    assert!(decode_json::<ProfileUpdateRequest>(
        br#"{"handle":"agent","display_name":"Agent","expected_profile_revision":1,"id":"caller-provided"}"#
    )
    .is_err());

    assert!(
        ListRecordsQuery {
            author: Some(String::new()),
            ..ListRecordsQuery::default()
        }
        .validate()
        .is_ok(),
        "optional filters without minLength accept an empty value"
    );
}

#[test]
fn search_since_requires_an_rfc3339_timestamp() {
    let query = SearchRecordsQuery {
        q: "journal".into(),
        page: PageQuery::default(),
        author: None,
        attention: None,
        since: Some("2026-01-02T03:04:05+05:30".into()),
        order: SearchOrder::Rank,
    };
    assert!(query.validate().is_ok());
    assert!(
        SearchRecordsQuery {
            since: Some("not-a-time".into()),
            ..query
        }
        .validate()
        .is_err()
    );
}

#[test]
fn response_dtos_require_every_normative_field() {
    assert_required_fields::<ErrorBody>(
        json!({"code":"not-found","message":"not found","request_id":"request-1"}),
        &["code", "message", "request_id"],
    );
    assert_required_fields::<ErrorResponse>(
        json!({"error":{"code":"not-found","message":"not found","request_id":"request-1"}}),
        &["error"],
    );
    assert_required_fields::<PageInfo>(json!({"next_cursor":null}), &["next_cursor"]);
    assert_required_fields::<OperationalMetrics>(
        wire_example("OperationalMetrics").clone(),
        &[
            "sampled_at",
            "database_bytes",
            "wal_bytes",
            "unacknowledged_inbox_count",
            "oldest_unacknowledged_at",
            "last_backup_at",
            "last_verified_restore_at",
        ],
    );
    assert_required_fields::<Health>(
        json!({"status":"ok","version":"0.1.0","checks":{}}),
        &["status", "version"],
    );
    assert_required_fields::<domain::Relation>(
        json!({"type":"reply-to","record_id":"record-0"}),
        &["type", "record_id"],
    );
    assert_required_fields::<domain::Principal>(
        json!({"id":"018f1f59-6e90-7000-8000-000000000001","handle":"agent-alpha","display_name":"Agent Alpha","profile_revision":1,"created_at":"2026-01-01T00:00:00Z","disabled":false}),
        &[
            "id",
            "handle",
            "display_name",
            "profile_revision",
            "created_at",
            "disabled",
        ],
    );
    assert_required_fields::<RegistrationReceipt>(
        wire_example("RegistrationReceipt").clone(),
        &["principal", "credential_id"],
    );
    assert_required_fields::<PrincipalRecoveryResponse>(
        wire_example("PrincipalRecoveryResponse").clone(),
        &["principal", "replacement_secret"],
    );
    assert_required_fields::<Membership>(
        json!({
            "space_id":"project-alpha","principal_id":"agent-alpha",
            "can_read":true,"can_append":true,"can_admin":false
        }),
        &[
            "space_id",
            "principal_id",
            "can_read",
            "can_append",
            "can_admin",
        ],
    );
    assert_required_fields::<domain::Limits>(
        json!({
            "content_bytes":65536,"relations":32,"attention_recipients":16,
            "page_size":100,

        }),
        &[
            "content_bytes",
            "relations",
            "attention_recipients",
            "page_size",
        ],
    );
    assert_required_fields::<domain::Space>(
        json!({
            "id":"project-alpha","name":"Project Alpha","access":"public","created_at":"2026-01-01T00:00:00Z",
            "limits":{
                "content_bytes":65536,"relations":32,"attention_recipients":16,
                "page_size":100,

            }
        }),
        &["id", "name", "created_at", "limits"],
    );
    assert_required_fields::<domain::Record>(
        json!({
            "id":"record-1","space_id":"project-alpha","seq":1,"author":"agent-alpha",
            "kind":"message","content":"hello","created_at":"2026-01-01T00:00:00Z",
            "relations":[]
        }),
        &[
            "id",
            "space_id",
            "seq",
            "author",
            "kind",
            "content",
            "created_at",
            "relations",
        ],
    );
    assert_required_fields::<PrincipalPage>(
        json!({"items":[],"next_cursor":null}),
        &["items", "next_cursor"],
    );
    assert_required_fields::<SpacePage>(
        json!({"items":[],"next_cursor":null}),
        &["items", "next_cursor"],
    );
    assert_required_fields::<RecordPage>(
        json!({"items":[],"next_cursor":null}),
        &["items", "next_cursor"],
    );
    assert_required_fields::<AppendRecordResponse>(
        json!({
            "record": {
                "id":"record-1","space_id":"project-alpha","seq":1,"author":"agent-alpha",
                "kind":"message","content":"hello","created_at":"2026-01-01T00:00:00Z",
                "relations":[]
            },
            "mailbox_created":0,
            "replayed":false
        }),
        &["record", "mailbox_created", "replayed"],
    );
    assert_required_fields::<SearchResult>(
        json!({
            "id":"record-1","space_id":"project-alpha","seq":1,"author":"agent-alpha",
            "kind":"message","content":"hello","created_at":"2026-01-01T00:00:00Z",
            "relations":[],"score":1.25
        }),
        &[
            "id",
            "space_id",
            "seq",
            "author",
            "kind",
            "content",
            "created_at",
            "relations",
            "score",
        ],
    );
    assert_required_fields::<SearchPage>(
        json!({"items":[],"next_cursor":null,"order":"rank"}),
        &["items", "next_cursor", "order"],
    );
    assert_required_fields::<Me>(
        json!({
            "principal":{"id":"018f1f59-6e90-7000-8000-000000000001","handle":"agent-alpha","display_name":"Agent Alpha","profile_revision":1,"created_at":"2026-01-01T00:00:00Z","disabled":false},
            "memberships":[],
            "limits":{
                "content_bytes":65536,"relations":32,"attention_recipients":16,
                "page_size":100,

            }
        }),
        &["principal", "memberships", "limits"],
    );
    assert_required_fields::<ReceiptSummary>(
        json!({
            "inbox_item_id":"mailbox-1","recipient":"agent-beta",
            "state":"unacknowledged","created_at":"2026-01-01T00:00:00Z","acknowledged_at":null
        }),
        &[
            "inbox_item_id",
            "recipient",
            "state",
            "created_at",
            "acknowledged_at",
        ],
    );
    assert_required_fields::<ReceiptStatusPage>(
        json!({"items":[],"next_cursor":null}),
        &["items", "next_cursor"],
    );
    assert_required_fields::<InboxItem>(
        wire_example("InboxItem").clone(),
        &[
            "inbox_item_id",
            "recipient",
            "seq",
            "created_at",
            "acknowledged_at",
            "record",
        ],
    );
    assert_required_fields::<InboxPage>(
        json!({"items":[],"next_cursor":null}),
        &["items", "next_cursor"],
    );
    assert_required_fields::<OneTimePrincipalClientSecret>(
        json!({"credential_id":"principal-credential","secret":"principal-secret"}),
        &["credential_id", "secret"],
    );
    assert_required_fields::<CredentialMetadata>(
        json!({
            "credential_id":"credential-1","principal_id":"agent-alpha",
            "class":"principal-client","rotated_at":"2026-01-01T00:00:00Z"
        }),
        &["credential_id", "principal_id", "class", "rotated_at"],
    );
}

#[test]
fn pages_serialize_empty_items_and_null_cursor() {
    let page = PrincipalPage {
        items: Vec::new(),
        next_cursor: None,
    };
    assert_eq!(
        serde_json::to_value(page).expect("page JSON"),
        json!({"items":[],"next_cursor":null})
    );
}

#[test]
fn one_time_secrets_are_redacted_from_debug_output() {
    let credential = OneTimePrincipalClientSecret {
        credential_id: "principal-credential".into(),
        secret: "p-example".into(),
    };
    assert!(!format!("{credential:?}").contains("p-example"));
}

#[test]
fn retired_credential_class_is_not_a_wire_authority() {
    assert!(decode_json::<CredentialClass>(br#""delivery-adapter""#).is_err());
    assert_eq!(
        decode_json::<CredentialClass>(br#""principal-client""#).unwrap(),
        CredentialClass::PrincipalClient
    );
}
