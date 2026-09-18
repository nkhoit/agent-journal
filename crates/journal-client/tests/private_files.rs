#![cfg(unix)]
use journal_client::private_file;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
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
