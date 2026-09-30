#![cfg(unix)]
use journal_client::private_file;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::mpsc,
    time::Duration,
};

#[test]
fn publication_is_private_durable_and_never_clobbers() {
    let directory = std::env::current_dir()
        .unwrap()
        .join(format!(".credential-test-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("credential");
    private_file::write(&path, b"synthetic-secret").unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(private_file::read(&path).unwrap(), "synthetic-secret");
    assert!(private_file::write(&path, b"overwrite").is_err());
    assert_eq!(fs::read(&path).unwrap(), b"synthetic-secret");
    assert!(private_file::replace(&path, b"wrong-pending-state", b"replacement").is_err());
    assert_eq!(fs::read(&path).unwrap(), b"synthetic-secret");
    fs::write(
        directory.join(format!(".credential.replacement.{}.0", std::process::id())),
        b"stale",
    )
    .unwrap();
    private_file::replace(&path, b"synthetic-secret", b"replacement").unwrap();
    assert_eq!(private_file::read(&path).unwrap(), "replacement");
    let link = directory.join("link");
    symlink(&path, &link).unwrap();
    assert!(private_file::read(&link).is_err());
    assert!(private_file::write(&link, b"overwrite").is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(private_file::read(&path).is_err());
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(private_file::write(&directory.join("unsafe"), b"secret").is_err());
    fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn state_lock_serializes_resumed_writers_and_survives_stale_lock_files() {
    let directory = std::env::current_dir()
        .unwrap()
        .join(format!(".credential-lock-test-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("registration");
    let first = private_file::lock(&path).unwrap();
    let (send, receive) = mpsc::channel();
    let second_path = path.clone();
    let thread = std::thread::spawn(move || {
        let lock = private_file::lock(&second_path).unwrap();
        send.send(lock).unwrap();
    });
    assert!(receive.recv_timeout(Duration::from_millis(100)).is_err());
    drop(first);
    let second = receive.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(second);
    thread.join().unwrap();
    fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn larger_state_replacement_is_bounded_and_keeps_credential_limits() {
    let directory =
        std::env::temp_dir().join(format!("private-large-state-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("post");
    let pending = vec![b'p'; 32_768];
    let completed = vec![b'c'; 65_536];
    private_file::write(&path, &pending).unwrap();
    assert!(private_file::read(&path).is_err());
    assert!(private_file::read_with_limit(&path, 32_767).is_err());
    assert_eq!(
        private_file::read_with_limit(&path, 32_768)
            .unwrap()
            .as_bytes(),
        pending
    );
    assert!(private_file::replace(&path, &pending, &completed).is_err());
    assert!(private_file::replace_with_limit(&path, &pending, &completed, 65_535).is_err());
    assert_eq!(fs::read(&path).unwrap(), pending);
    private_file::replace_with_limit(&path, &pending, &completed, 65_536).unwrap();
    assert_eq!(fs::read(&path).unwrap(), completed);
    let lock_path = directory.join(".post.lock");
    symlink(&path, &lock_path).unwrap();
    assert!(private_file::lock(&path).is_err());
    fs::remove_file(&lock_path).unwrap();
    fs::hard_link(&path, &lock_path).unwrap();
    assert!(private_file::lock(&path).is_err());
    fs::remove_dir_all(directory).unwrap();
}
