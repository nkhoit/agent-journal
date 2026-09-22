use journal_client::{Client, HttpTransport, journal_protocol::*};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

#[test]
fn full_escaped_record_page_fits_bounded_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let record = domain::Record {
        id: "record".into(),
        space_id: "space".into(),
        seq: 1,
        author: "writer".into(),
        kind: "note".into(),
        content: "\u{0001}".repeat(domain::MAX_CONTENT_BYTES),
        run_id: None,
        created_at: "2026-01-01T00:00:00Z".into(),
        attention: vec![],
        routing_key: None,
        relations: vec![],
        title: None,
    };
    let body = serde_json::to_vec(&Page {
        items: vec![record; domain::MAX_PAGE_SIZE],
        next_cursor: None,
    })
    .unwrap();
    assert!(body.len() > 2 * 1024 * 1024);
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 4096];
        let length = stream.read(&mut request).unwrap();
        assert!(length > 0);
        write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).unwrap();
        stream.write_all(&body).unwrap();
    });
    let page = Client::new(HttpTransport::new(&endpoint).unwrap())
        .list(
            &"a".repeat(64),
            "space",
            &ListRecordsQuery {
                page: PageQuery::new(None, Some(100)),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.items.len(), 100);
    assert_eq!(page.items[99].content.len(), domain::MAX_CONTENT_BYTES);
    thread.join().unwrap();
}
