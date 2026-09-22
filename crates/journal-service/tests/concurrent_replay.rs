use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use std::sync::{Arc, Barrier};

#[test]
fn simultaneous_first_use_of_a_key_commits_exactly_one_append() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("replay-race-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    service
        .register(
            &"a".repeat(64),
            &RegistrationRequest {
                handle: "writer".into(),
                display_name: "Writer".into(),
            },
        )
        .unwrap();
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "space".into(),
            name: "Space".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "writer".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let principal_token = "a".repeat(64);
    let barrier = Arc::new(Barrier::new(8));
    let results = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let barrier = barrier.clone();
                let token = &principal_token;
                let service = &service;
                scope.spawn(move || {
                    let input =
                        decode_json(br#"{"kind":"note","content":"hello","attention":["writer"]}"#)
                            .unwrap();
                    barrier.wait();
                    service
                        .append_record(token, "space", "same-key", &input)
                        .unwrap()
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    // Exactly one thread commits; the rest replay the winner's record.
    // The winner reports replayed=false, losers report replayed=true.
    assert_eq!(
        results.iter().filter(|r| !r.replayed).count(),
        1,
        "exactly one winner"
    );
    assert!(
        results
            .iter()
            .all(|result| result.record == results[0].record)
    );
    assert!(
        results
            .iter()
            .all(|result| result.mailbox_created == results[0].mailbox_created)
    );
    let connection = db.connect().unwrap();
    for table in ["records", "attention", "mailbox_items", "idempotency_keys"] {
        let count: i64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
    drop(connection);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}
