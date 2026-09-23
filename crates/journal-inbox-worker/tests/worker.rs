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
fn buffered_items_continue_without_the_idle_delay_until_unavailability() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![
            item("first", None),
            item("bad-route", Some("unknown")),
            item("second", None),
            item("third", None),
        ],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    let events = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(events[0].kind, EventKind::Acknowledged);
    assert!(worker.continue_immediately(&events));
    // An item-specific route failure does not pause the pass.
    let events = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(events[0].kind, EventKind::RouteUnavailable);
    assert!(worker.continue_immediately(&events));
    // Runtime unavailability pauses even while items remain buffered.
    *runtime.fail.borrow_mut() = true;
    let events = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(events[0].kind, EventKind::RuntimeUnavailable);
    assert!(!worker.continue_immediately(&events));
    *runtime.fail.borrow_mut() = false;
    let events = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(events[0].kind, EventKind::Acknowledged);
    // Once the page is drained the next tick would fetch, so the delay applies.
    assert!(!worker.continue_immediately(&events));
    assert_eq!(*source.acknowledgments.borrow(), ["first", "third"]);
    assert_eq!(source.cursors.borrow().len(), 1);
}

#[test]
fn pending_ack_failure_pauses_the_pass() {
    let source = Journal::default();
    source.pages.borrow_mut().push_back(Page {
        items: vec![item("first", None), item("later", None)],
        next_cursor: None,
    });
    *source.failure.borrow_mut() = Some(503);
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    let events = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(events[0].kind, EventKind::AckPending);
    assert!(!worker.continue_immediately(&events));
}

/// Serves `count` items in fixed-size pages; each cursorless fetch restarts.
struct Backlog {
    count: usize,
}

impl Inbox for Backlog {
    fn fetch(&self, cursor: Option<&str>) -> Result<InboxPage, ClientError> {
        let start: usize = cursor.map_or(0, |cursor| cursor.parse().unwrap());
        let end = (start + PAGE_LIMIT as usize).min(self.count);
        Ok(Page {
            items: (start..end)
                .map(|index| item(&format!("item-{index:05}"), None))
                .collect(),
            next_cursor: (end < self.count).then(|| end.to_string()),
        })
    }

    fn acknowledge(&self, _: &str) -> Result<(), ClientError> {
        panic!("rejected items are never acknowledged")
    }
}

#[derive(Default)]
struct Rejecting(std::cell::Cell<usize>);

impl Runtime for Rejecting {
    fn inject(&self, _: &Route, _: &Envelope, _: &str) -> RuntimeResult<String> {
        self.0.set(self.0.get() + 1);
        Err(RuntimeError::RuntimeRejected)
    }
}

fn first_pass(worker: &mut Worker<'_, Backlog, Rejecting>, runtime: &Rejecting, count: usize) {
    for _ in 0..count * 2 {
        if runtime.0.get() == count {
            return;
        }
        worker.tick(Duration::ZERO).unwrap();
    }
    panic!("first pass did not attempt every item");
}

#[test]
fn failure_backoff_survives_a_large_backlog_of_rejected_items() {
    let inbox = Backlog { count: 1000 };
    let runtime = Rejecting::default();
    let routes = routes();
    let mut worker = Worker::new(&inbox, &runtime, &routes);
    first_pass(&mut worker, &runtime, inbox.count);
    // Two more full passes at the same instant: every item is still backing off.
    for _ in 0..inbox.count * 2 + 100 {
        worker.tick(Duration::ZERO).unwrap();
    }
    assert_eq!(runtime.0.get(), inbox.count, "no retry inside its backoff");
    // Once the backoff elapses, each item is retried exactly once more.
    for _ in 0..inbox.count * 2 {
        worker.tick(Duration::from_secs(1)).unwrap();
    }
    assert_eq!(runtime.0.get(), inbox.count * 2);
}

#[test]
fn failure_backoff_cache_stays_bounded() {
    let inbox = Backlog {
        count: FAILURE_LIMIT + 10,
    };
    let runtime = Rejecting::default();
    let routes = routes();
    let mut worker = Worker::new(&inbox, &runtime, &routes);
    first_pass(&mut worker, &runtime, inbox.count);
    assert_eq!(worker.failure_backoffs(), FAILURE_LIMIT);
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
fn failure_cache_evicts_the_soonest_due_entry_at_its_bound() {
    let source = Journal::default();
    let failed = (0..=FAILURE_LIMIT)
        .map(|index| item(&format!("failed-{index:05}"), Some("unknown")))
        .collect::<Vec<_>>();
    source
        .pages
        .borrow_mut()
        .extend(
            failed
                .chunks(PAGE_LIMIT as usize)
                .enumerate()
                .map(|(page, items)| Page {
                    items: items.to_vec(),
                    next_cursor: Some(format!("cursor-{page}")),
                }),
        );
    let last = format!("failed-{FAILURE_LIMIT:05}");
    source.pages.borrow_mut().push_back(Page {
        items: vec![
            item("failed-00000", Some("unknown")),
            item(&last, Some("unknown")),
        ],
        next_cursor: None,
    });
    let runtime = Platform::default();
    let routes = routes();
    let mut worker = Worker::new(&source, &runtime, &routes);
    // The first failure is recorded earliest, so its retry is due soonest.
    let first = worker.tick(Duration::ZERO).unwrap();
    assert_eq!(first[0].kind, EventKind::RouteUnavailable);
    for _ in 0..FAILURE_LIMIT {
        assert_eq!(
            worker.tick(Duration::from_millis(1)).unwrap()[0].kind,
            EventKind::RouteUnavailable
        );
    }
    assert_eq!(worker.failure_backoffs(), FAILURE_LIMIT);
    assert_eq!(
        worker.tick(Duration::from_millis(2)).unwrap()[0].kind,
        EventKind::RouteUnavailable,
        "the evicted soonest-due failure may be retried early"
    );
    assert!(
        worker.tick(Duration::from_millis(2)).unwrap().is_empty(),
        "a retained failure keeps its backoff"
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
