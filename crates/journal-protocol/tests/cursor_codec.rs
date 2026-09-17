use journal_protocol::{
    CursorCodec, CursorError, CursorOrder, CursorPosition, CursorRoute, CursorScope,
    MAX_CURSOR_CHARS,
};

fn codec() -> CursorCodec {
    CursorCodec::new(b"0123456789abcdef0123456789abcdef").expect("cursor key")
}

fn records_scope() -> CursorScope {
    CursorScope::new(
        CursorRoute::Records,
        br#"{"space":"project-alpha","author":"agent-alpha"}"#,
        CursorOrder::Sequence,
    )
}

#[test]
fn cursor_round_trips_valid_continuation() {
    let position = CursorPosition::Sequence {
        sequence: 42,
        id: "record-42".into(),
    };
    let token = codec()
        .encode(&records_scope(), &position)
        .expect("encode cursor");
    assert!(token.len() <= MAX_CURSOR_CHARS);
    assert_eq!(
        codec().decode(&records_scope(), &token).expect("decode"),
        position
    );
}

#[test]
fn cursor_rejects_payload_and_signature_tampering() {
    let token = codec()
        .encode(
            &records_scope(),
            &CursorPosition::Sequence {
                sequence: 42,
                id: "record-42".into(),
            },
        )
        .expect("encode cursor");
    let (payload, signature) = token.split_once('.').expect("cursor envelope");

    let mut tampered_payload = payload.as_bytes().to_vec();
    tampered_payload[0] = if tampered_payload[0] == b'A' {
        b'B'
    } else {
        b'A'
    };
    let tampered_payload = format!(
        "{}.{}",
        String::from_utf8(tampered_payload).expect("base64 text"),
        signature
    );
    assert_eq!(
        codec().decode(&records_scope(), &tampered_payload),
        Err(CursorError::InvalidSignature)
    );

    let mut tampered_signature = signature.as_bytes().to_vec();
    tampered_signature[0] = if tampered_signature[0] == b'A' {
        b'B'
    } else {
        b'A'
    };
    let tampered_signature = format!(
        "{}.{}",
        payload,
        String::from_utf8(tampered_signature).expect("base64 text")
    );
    assert_eq!(
        codec().decode(&records_scope(), &tampered_signature),
        Err(CursorError::InvalidSignature)
    );
}

#[test]
fn cursor_is_bound_to_route_filter_and_order() {
    let token = codec()
        .encode(
            &records_scope(),
            &CursorPosition::Sequence {
                sequence: 42,
                id: "record-42".into(),
            },
        )
        .expect("encode cursor");

    let wrong_route = CursorScope::new(
        CursorRoute::RecordThread,
        br#"{"space":"project-alpha","author":"agent-alpha"}"#,
        CursorOrder::Sequence,
    );
    assert_eq!(
        codec().decode(&wrong_route, &token),
        Err(CursorError::WrongRoute)
    );

    let wrong_filter = CursorScope::new(
        CursorRoute::Records,
        br#"{"space":"project-alpha","author":"agent-beta"}"#,
        CursorOrder::Sequence,
    );
    assert_eq!(
        codec().decode(&wrong_filter, &token),
        Err(CursorError::WrongFilter)
    );

    let wrong_order = CursorScope::new(
        CursorRoute::Records,
        br#"{"space":"project-alpha","author":"agent-alpha"}"#,
        CursorOrder::Rank,
    );
    assert_eq!(
        codec().decode(&wrong_order, &token),
        Err(CursorError::WrongOrder)
    );
}

#[test]
fn cursor_rejects_truncation_malformed_envelopes_and_wrong_positions() {
    let token = codec()
        .encode(
            &records_scope(),
            &CursorPosition::Sequence {
                sequence: 42,
                id: "record-42".into(),
            },
        )
        .expect("encode cursor");
    assert!(matches!(
        codec().decode(&records_scope(), &token[..token.len() - 1]),
        Err(CursorError::InvalidSignature | CursorError::InvalidEncoding)
    ));
    assert_eq!(
        codec().decode(&records_scope(), "not-a-cursor"),
        Err(CursorError::Malformed)
    );
    assert_eq!(
        codec().encode(
            &records_scope(),
            &CursorPosition::Rank {
                score_bits: 1.0_f64.to_bits(),
                id: "record-42".into(),
            }
        ),
        Err(CursorError::PositionOrderMismatch)
    );
}

#[test]
fn cursor_enforces_key_and_token_bounds() {
    assert_eq!(CursorCodec::new(b"short"), Err(CursorError::KeyTooShort));

    assert!(
        !format!("{:?}", codec()).contains("0123456789abcdef"),
        "cursor MAC keys must be redacted"
    );

    let oversized = CursorPosition::Sequence {
        sequence: 42,
        id: "x".repeat(MAX_CURSOR_CHARS * 2),
    };
    assert_eq!(
        codec().encode(&records_scope(), &oversized),
        Err(CursorError::TooLong)
    );
    assert_eq!(
        codec().decode(&records_scope(), &"x".repeat(MAX_CURSOR_CHARS + 1)),
        Err(CursorError::TooLong)
    );
}
