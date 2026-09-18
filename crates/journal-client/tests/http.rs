use journal_client::{
    HttpTransport,
    journal_protocol::{Request, Transport},
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

#[test]
fn public_http_does_not_follow_redirect_or_retry_post() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = [0; 4096];
        let length = stream.read(&mut bytes).unwrap();
        assert!(
            String::from_utf8_lossy(&bytes[..length]).starts_with("POST /v1/enrollment/exchange ")
        );
        stream.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/secret\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    });
    let transport = HttpTransport::new(&endpoint).unwrap();
    let response = transport
        .send(Request::new(
            "POST",
            "/v1/enrollment/exchange",
            b"{}".to_vec(),
        ))
        .unwrap();
    assert_eq!(response.status, 307);
    server.join().unwrap();
    assert!(
        transport
            .send(Request::new("POST", "/v1/admin/principals", b"{}".to_vec()))
            .is_err()
    );
}

#[cfg(not(unix))]
#[test]
fn unix_is_explicitly_unsupported() {
    let error = match HttpTransport::unix(std::path::Path::new("socket")) {
        Ok(_) => panic!("Unix transport unexpectedly available"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("unsupported"));
}

#[cfg(unix)]
#[test]
fn admin_uses_unix_http_without_bearer() {
    use std::os::unix::net::UnixListener;
    let path = std::path::PathBuf::from(format!(".admin-test-{}.sock", std::process::id()));
    let listener = UnixListener::bind(&path).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = [0; 4096];
        let length = stream.read(&mut bytes).unwrap();
        let request = String::from_utf8_lossy(&bytes[..length]);
        assert!(request.starts_with("POST /v1/admin/principals "));
        assert!(!request.to_ascii_lowercase().contains("authorization:"));
        stream
            .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .unwrap();
    });
    let transport = HttpTransport::unix(&path).unwrap();
    let response = transport
        .send(Request::new("POST", "/v1/admin/principals", b"{}".to_vec()))
        .unwrap();
    assert_eq!(response.status, 201);
    server.join().unwrap();
    std::fs::remove_file(path).unwrap();
}
