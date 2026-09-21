use journal_client::{Client, ClientError, HttpTransport, private_file};
use journal_inbox_worker::{Inbox, PrincipalInbox, Runtime, StaticRoutes, Worker};
use journal_protocol::{InboxPage, OneTimePrincipalClientSecret, decode_json};
use std::{
    io::Write,
    path::Path,
    process::{Child, Command},
    time::{Duration, Instant},
};

pub struct Process(pub Child);

impl std::ops::Deref for Process {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Process {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct BlockAck<'a> {
    source: &'a PrincipalInbox,
    root: &'a Path,
}

impl Inbox for BlockAck<'_> {
    fn fetch(&self, cursor: Option<&str>) -> Result<InboxPage, ClientError> {
        self.source.fetch(cursor)
    }

    fn acknowledge(&self, _: &str) -> Result<(), ClientError> {
        let mut marker = std::fs::File::create(self.root.join("ack-blocked")).unwrap();
        marker
            .write_all(b"handoff succeeded; ack not sent")
            .unwrap();
        marker.sync_all().unwrap();
        std::fs::File::open(self.root).unwrap().sync_all().unwrap();
        loop {
            std::thread::park();
        }
    }
}

pub fn handoff_until_ack(root: &Path, endpoint: &str, runtime: &impl Runtime) {
    let credential: OneTimePrincipalClientSecret = decode_json(
        private_file::read(&root.join("principal.credential"))
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let routes: StaticRoutes = decode_json(
        private_file::read(&root.join("routes.json"))
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let source = PrincipalInbox::new(
        Client::new(HttpTransport::new(endpoint).unwrap()),
        credential.secret,
    );
    let source = BlockAck {
        source: &source,
        root,
    };
    Worker::new(&source, runtime, &routes)
        .tick(Duration::ZERO)
        .unwrap();
    panic!("child did not reach successful handoff");
}

pub fn kill_after_handoff(root: &Path, endpoint: &str, runtime_endpoint: &str) {
    let mut process = Process(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env("INBOX_CRASH_ROOT", root)
            .env("INBOX_CRASH_ENDPOINT", endpoint)
            .env("INBOX_RUNTIME_ENDPOINT", runtime_endpoint)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while !root.join("ack-blocked").exists() {
        assert!(
            process.try_wait().unwrap().is_none(),
            "handoff child exited early"
        );
        assert!(
            Instant::now() < deadline,
            "handoff child did not reach ack boundary"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    process.kill().unwrap();
    assert!(!process.wait().unwrap().success());
}
