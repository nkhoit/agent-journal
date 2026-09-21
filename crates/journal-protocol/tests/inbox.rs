use journal_protocol::*;

#[test]
fn inbox_query_is_strict_and_bounded() {
    assert_eq!(InboxQuery::default().page.effective_limit(), 50);
    for query in [
        "",
        "limit=1",
        "limit=100&state=all",
        "state=acknowledged",
        "state=unacknowledged&cursor=",
    ] {
        let parsed = InboxQuery::from_query(query).unwrap();
        assert_eq!(
            InboxQuery::from_query(&query_string(&parsed.pairs())).unwrap(),
            parsed
        );
    }
    for query in [
        "limit=0",
        "limit=101",
        "limit=-1",
        "state=",
        "state=pending",
        "recipient=beta",
        "state=all&state=all",
        "limit=1&limit=1",
        "cursor=%ff",
        "cursor=%xx",
        "after_seq=1",
    ] {
        assert!(InboxQuery::from_query(query).is_err(), "{query}");
    }
}

#[test]
fn receipt_timestamp_is_required_nullable_and_legacy_states_are_rejected() {
    let json = br#"{"inbox_item_id":"item-example","recipient":"recipient-example","state":"unacknowledged","created_at":"2026-01-01T00:00:00Z","acknowledged_at":null}"#;
    let receipt: ReceiptSummary = decode_json(json).unwrap();
    assert!(receipt.acknowledged_at.is_none());
    let mut value: serde_json::Value = serde_json::from_slice(json).unwrap();
    value.as_object_mut().unwrap().remove("acknowledged_at");
    assert!(decode_json::<ReceiptSummary>(&serde_json::to_vec(&value).unwrap()).is_err());
    value["acknowledged_at"] = serde_json::Value::Null;
    value["state"] = "host-accepted".into();
    assert!(decode_json::<ReceiptSummary>(&serde_json::to_vec(&value).unwrap()).is_err());
}
