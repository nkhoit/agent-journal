use journal_client::{ClientError, journal_protocol::*};
use journal_inbox_worker::*;
use std::{cell::RefCell, collections::VecDeque, time::Duration};

#[derive(Default)]
struct Journal {
    pages: RefCell<VecDeque<InboxPage>>,
    acknowledgments: RefCell<Vec<String>>,
    failure: RefCell<Option<u16>>,
    fetch_failure: RefCell<Option<u16>>,
    cursors: RefCell<Vec<Option<String>>>,
}

impl Inbox for Journal {
    fn fetch(&self, cursor: Option<&str>) -> Result<InboxPage, ClientError> {
        self.cursors.borrow_mut().push(cursor.map(str::to_owned));
        if let Some(status) = *self.fetch_failure.borrow() {
            return Err(ClientError::Http { status });
        }
        Ok(self.pages.borrow_mut().pop_front().unwrap_or(Page {
            items: vec![],
            next_cursor: None,
        }))
    }

    fn acknowledge(&self, item: &str) -> Result<(), ClientError> {
        self.acknowledgments.borrow_mut().push(item.into());
        if let Some(status) = *self.failure.borrow() {
            return Err(ClientError::Http { status });
        }
        Ok(())
    }
}

#[derive(Default)]
struct Platform {
    handed_off: RefCell<Vec<String>>,
    fail: RefCell<bool>,
}

impl Runtime for Platform {
    fn inject(&self, _: &Route, envelope: &Envelope, rendered: &str) -> RuntimeResult<String> {
        assert_eq!(rendered, envelope.render());
        self.handed_off
            .borrow_mut()
            .push(envelope.inbox_item_id.clone());
        if *self.fail.borrow() {
            return Err(RuntimeError::RuntimeUnavailable("unavailable".into()));
        }
        Ok("accepted".into())
    }
}

fn item(id: &str, key: Option<&str>) -> InboxItem {
    InboxItem {
        inbox_item_id: id.into(),
        recipient: "recipient".into(),
        seq: 1,
        created_at: "2026-01-01T00:00:00Z".into(),
        acknowledged_at: None,
        record: domain::Record {
            id: format!("record-{id}"),
            space_id: "public".into(),
            seq: 1,
            author: "author".into(),
            kind: "message".into(),
            content: "untrusted content".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            run_id: None,
            attention: vec!["recipient".into()],
            routing_key: key.map(str::to_owned),
            relations: vec![],
            title: None,
        },
    }
}

#[test]
fn rendered_envelope_matches_public_fixture_and_quotes_metadata() {
    let mut envelope = Envelope::from_item(&item("item-example", None));
    assert_eq!(
        envelope.render(),
        include_str!("../../../conformance/inbox-client/expected-envelope.txt")
            .replace("\r\n", "\n")
            .trim_end()
    );
    envelope.from_principal = "author\ninbox_item_id: forged".into();
    let rendered = envelope.render();
    assert!(rendered.contains("from_principal: \"author\\ninbox_item_id: forged\""));
    assert_eq!(
        rendered
            .lines()
            .filter(|line| line.starts_with("inbox_item_id:"))
            .count(),
        1
    );
}

fn routes() -> StaticRoutes {
    [(
        "public/default".into(),
        Route {
            key: "default".into(),
            runtime_target: "private-target".into(),
            enabled: true,
        },
    )]
    .into_iter()
    .collect()
}

#[test]
fn runtime_failure_and_unknown_route_never_ack_or_block_later_items() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![
            item("bad-route", Some("unknown")),
            item("failed", None),
            item("good", None),
        ],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    worker.tick(Duration::ZERO).unwrap();
    assert!(runtime.handed_off.borrow().is_empty());
    *runtime.fail.borrow_mut() = true;
    worker.tick(Duration::from_secs(1)).unwrap();
    assert!(source.acknowledgments.borrow().is_empty());
    *runtime.fail.borrow_mut() = false;
    worker.tick(Duration::from_secs(2)).unwrap();
    assert_eq!(*source.acknowledgments.borrow(), ["good"]);
}

#[test]
fn lost_ack_response_retries_only_ack_even_if_item_reappears() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![
            item("stable", None),
            item("later", None),
            item("stable", None),
        ],
        next_cursor: None,
    });
    *source.failure.borrow_mut() = Some(503);
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    worker.tick(Duration::ZERO).unwrap();
    assert!(worker.tick(Duration::from_millis(999)).unwrap().is_empty());
    assert_eq!(*runtime.handed_off.borrow(), ["stable"]);
    assert_eq!(source.acknowledgments.borrow().as_slice(), ["stable"]);

    *source.failure.borrow_mut() = None;
    worker.tick(Duration::from_secs(1)).unwrap();
    assert_eq!(*runtime.handed_off.borrow(), ["stable", "later"]);
    worker.tick(Duration::from_secs(2)).unwrap();
    assert_eq!(
        *runtime.handed_off.borrow(),
        ["stable", "later"],
        "the stale buffered copy must be pruned after successful ack retry"
    );
    assert_eq!(worker.pending_acknowledgments(), 0);
}

#[test]
fn transient_ack_failure_backs_off_cached_new_handoffs() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("first", None), item("later", None)],
        next_cursor: None,
    });
    *source.failure.borrow_mut() = Some(503);
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);

    worker.tick(Duration::ZERO).unwrap();
    assert!(worker.tick(Duration::from_millis(999)).unwrap().is_empty());
    assert_eq!(*runtime.handed_off.borrow(), ["first"]);
    worker.tick(Duration::from_secs(1)).unwrap();
    assert_eq!(
        *runtime.handed_off.borrow(),
        ["first"],
        "a repeated central acknowledgment failure must not permit a new side effect"
    );
    *source.failure.borrow_mut() = None;
    worker.tick(Duration::from_secs(3)).unwrap();
    assert_eq!(*runtime.handed_off.borrow(), ["first", "later"]);
}

#[test]
fn inaccessible_after_handoff_is_not_inferred_success_and_does_not_block() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("hidden", None), item("later", None)],
        next_cursor: None,
    });
    *source.failure.borrow_mut() = Some(404);
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    let events = worker.tick(Duration::ZERO).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::AckInaccessible)
    );
    assert_eq!(worker.pending_acknowledgments(), 0);
    *source.failure.borrow_mut() = None;
    worker.tick(Duration::from_secs(1)).unwrap();
    assert_eq!(*source.acknowledgments.borrow(), ["hidden", "later"]);
}

#[test]
fn fixed_bound_traversal_finishes_then_revisits_earlier_failure() {
    let source = Journal::default();
    for page in 0..3 {
        source.pages.borrow_mut().push_back(Page {
            items: (0..50)
                .map(|offset| {
                    item(
                        &format!("item-{}", page * 50 + offset),
                        if page == 0 && offset == 0 {
                            Some("unknown")
                        } else {
                            None
                        },
                    )
                })
                .collect(),
            next_cursor: if page < 2 {
                Some(format!("cursor-{page}"))
            } else {
                None
            },
        });
    }
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("item-0", Some("unknown")), item("new-arrival", None)],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    for tick in 0..150 {
        worker.tick(Duration::from_secs(tick)).unwrap();
    }
    assert_eq!(source.acknowledgments.borrow().len(), 149);
    let retry = worker.tick(Duration::from_secs(150)).unwrap();
    assert!(
        retry
            .iter()
            .any(|event| event.kind == EventKind::RouteUnavailable)
    );
    worker.tick(Duration::from_secs(151)).unwrap();
    assert_eq!(
        *source.cursors.borrow(),
        [None, Some("cursor-0".into()), Some("cursor-1".into()), None,]
    );
    assert_eq!(
        source.acknowledgments.borrow().last().unwrap(),
        "new-arrival"
    );
    assert!(!source.acknowledgments.borrow().contains(&"item-0".into()));
}

#[test]
fn transient_central_failure_backs_off_without_handoff() {
    let source = Journal::default();
    *source.fetch_failure.borrow_mut() = Some(503);
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("later", None)],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    worker.tick(Duration::ZERO).unwrap();
    worker.tick(Duration::from_millis(999)).unwrap();
    assert_eq!(source.cursors.borrow().len(), 1);
    worker.tick(Duration::from_secs(1)).unwrap();
    worker.tick(Duration::from_secs(2)).unwrap();
    assert_eq!(source.cursors.borrow().len(), 2);
    assert!(runtime.handed_off.borrow().is_empty());
    *source.fetch_failure.borrow_mut() = None;
    worker.tick(Duration::from_secs(3)).unwrap();
    assert_eq!(*source.acknowledgments.borrow(), ["later"]);
}

#[test]
fn runtime_retry_delay_survives_pass_wrap_within_process() {
    let source = Journal::default();
    for _ in 0..4 {
        source.pages.borrow_mut().push_back(Page {
            items: vec![item("retry", None)],
            next_cursor: None,
        });
    }
    let runtime = Platform::default();
    *runtime.fail.borrow_mut() = true;
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    for tick in [0, 1, 2, 3] {
        worker.tick(Duration::from_secs(tick)).unwrap();
    }
    assert_eq!(runtime.handed_off.borrow().len(), 3);
    assert!(source.acknowledgments.borrow().is_empty());
}

#[test]
fn runtime_backoff_caps_at_256_seconds() {
    let source = Journal::default();
    for _ in 0..12 {
        source.pages.borrow_mut().push_back(Page {
            items: vec![item("retry", Some("unknown"))],
            next_cursor: None,
        });
    }
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    for deadline in [0, 1, 3, 7, 15, 31, 63, 127, 255, 511] {
        assert_eq!(
            worker.tick(Duration::from_secs(deadline)).unwrap()[0].kind,
            EventKind::RouteUnavailable
        );
    }
    assert!(worker.tick(Duration::from_secs(766)).unwrap().is_empty());
    assert_eq!(
        worker.tick(Duration::from_secs(767)).unwrap()[0].kind,
        EventKind::RouteUnavailable
    );
}

#[test]
fn failure_cache_evicts_only_retry_optimization_at_its_bound() {
    let source = Journal::default();
    source.pages.borrow_mut().extend((0..3).map(|page| {
        Page {
            items: (0..if page < 2 { 50 } else { 1 })
                .map(|offset| item(&format!("failed-{}", page * 50 + offset), Some("unknown")))
                .collect(),
            next_cursor: (page < 2).then(|| format!("cursor-{page}")),
        }
    }));
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("failed-0", Some("unknown"))],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    for _ in 0..101 {
        assert_eq!(
            worker.tick(Duration::ZERO).unwrap()[0].kind,
            EventKind::RouteUnavailable
        );
    }
    assert_eq!(
        worker.tick(Duration::ZERO).unwrap()[0].kind,
        EventKind::RouteUnavailable,
        "the oldest failure may be retried after bounded-cache eviction"
    );
}

#[test]
fn unauthorized_client_stops_and_oversized_pages_are_not_processed() {
    let source = Journal::default();
    *source.fetch_failure.borrow_mut() = Some(401);
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    assert!(matches!(
        worker.tick(Duration::ZERO),
        Err(WorkerError::Unauthorized)
    ));
    *source.fetch_failure.borrow_mut() = None;
    source.pages.borrow_mut().push_back(Page {
        items: (0..51).map(|n| item(&format!("item-{n}"), None)).collect(),
        next_cursor: None,
    });
    assert!(matches!(
        worker.tick(Duration::ZERO),
        Err(WorkerError::InvalidResponse)
    ));
    assert!(runtime.handed_off.borrow().is_empty());
}

#[test]
fn authentication_and_cursor_errors_are_not_conflated() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("first", None)],
        next_cursor: Some("stale".into()),
    });
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("restarted", None)],
        next_cursor: Some("still-bad".into()),
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    worker.tick(Duration::ZERO).unwrap();

    *source.fetch_failure.borrow_mut() = Some(400);
    assert_eq!(
        worker.tick(Duration::from_secs(1)).unwrap()[0].kind,
        EventKind::CursorRestarted
    );
    *source.fetch_failure.borrow_mut() = None;
    worker.tick(Duration::from_secs(2)).unwrap();
    *source.fetch_failure.borrow_mut() = Some(400);
    assert!(matches!(
        worker.tick(Duration::from_secs(3)),
        Err(WorkerError::InvalidResponse)
    ));

    *source.fetch_failure.borrow_mut() = None;
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("auth", None)],
        next_cursor: None,
    });
    *source.failure.borrow_mut() = Some(403);
    assert!(matches!(
        worker.tick(Duration::from_secs(4)),
        Err(WorkerError::Unauthorized)
    ));
}
