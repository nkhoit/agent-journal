use journal_protocol::{AppendRecordRequest, canonical_append, decode_json};

fn parse(json: &[u8]) -> AppendRecordRequest {
    decode_json(json).expect("append request")
}

#[test]
fn canonical_append_normalizes_only_attention_set_order() {
    let first = parse(
        br#"{
            "kind":"message",
            "content":"hello",
            "attention":["agent-beta","agent-alpha"],
            "relations":[
                {"type":"reply-to","record_id":"record-1"},
                {"type":"refers-to","record_id":"record-2"}
            ]
        }"#,
    );
    let second = parse(
        br#"{
            "relations":[
                {"type":"reply-to","record_id":"record-1"},
                {"type":"refers-to","record_id":"record-2"}
            ],
            "attention":["agent-alpha","agent-beta"],
            "content":"hello",
            "kind":"message"
        }"#,
    );

    let canonical = canonical_append(&first).expect("canonical first");
    assert_eq!(
        canonical,
        canonical_append(&second).expect("canonical second")
    );
    assert_eq!(
        std::str::from_utf8(&canonical).expect("UTF-8"),
        r#"{"kind":"message","content":"hello","attention":["agent-alpha","agent-beta"],"relations":[{"type":"reply-to","record_id":"record-1"},{"type":"refers-to","record_id":"record-2"}]}"#
    );
}

#[test]
fn omitted_and_explicit_empty_sets_have_one_canonical_form() {
    let omitted = parse(br#"{"kind":"message","content":"hello"}"#);
    let explicit = parse(br#"{"kind":"message","content":"hello","attention":[],"relations":[]}"#);
    assert_eq!(
        canonical_append(&omitted).expect("omitted"),
        canonical_append(&explicit).expect("explicit")
    );
}

#[test]
fn canonical_append_preserves_ordered_relations() {
    let first = parse(
        br#"{
            "kind":"message","content":"hello",
            "relations":[
                {"type":"reply-to","record_id":"record-1"},
                {"type":"refers-to","record_id":"record-2"}
            ]
        }"#,
    );
    let reversed = parse(
        br#"{
            "kind":"message","content":"hello",
            "relations":[
                {"type":"refers-to","record_id":"record-2"},
                {"type":"reply-to","record_id":"record-1"}
            ]
        }"#,
    );
    assert_ne!(
        canonical_append(&first).expect("first"),
        canonical_append(&reversed).expect("reversed")
    );
}

#[test]
fn canonical_append_preserves_content_and_identifiers_exactly() {
    let original =
        parse(br#"{"kind":"message","content":"Hello  world\n","routing_key":"Project:Alpha"}"#);
    let changed =
        parse(br#"{"kind":"message","content":"hello world\n","routing_key":"project:alpha"}"#);
    assert_ne!(
        canonical_append(&original).expect("original"),
        canonical_append(&changed).expect("changed")
    );
}

#[test]
fn canonical_append_rejects_invalid_payloads_before_encoding() {
    let duplicate_attention = parse(
        br#"{
            "kind":"message","content":"hello",
            "attention":["agent-alpha","agent-alpha"]
        }"#,
    );
    assert!(canonical_append(&duplicate_attention).is_err());

    let over_limit = AppendRecordRequest {
        content: "x".repeat(journal_protocol::domain::MAX_CONTENT_BYTES + 1),
        ..parse(br#"{"kind":"message","content":"hello"}"#)
    };
    assert!(canonical_append(&over_limit).is_err());
}
