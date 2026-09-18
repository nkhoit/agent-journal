//! Durable no-clobber publication of secret files.
use std::{io, path::Path};

#[cfg(unix)]
pub fn check_destination(path: &Path) -> io::Result<()> {
    use std::{fs, os::unix::fs::PermissionsExt};
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if fs::metadata(parent)?.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "credential directory must be private (0700)",
        ));
    }
    path.file_name()
        .ok_or_else(|| io::Error::other("invalid credential path"))?;
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "credential destination already exists",
        )),
    }
}

#[cfg(not(unix))]
pub fn check_destination(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}

#[cfg(unix)]
pub fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    check_destination(path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let metadata = fs::metadata(parent)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "credential directory must be private (0700)",
        ));
    }
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("invalid credential path"))?;
    let staging = parent.join(format!(
        ".{}.{}.pending",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&staging)?;
    let result = (|| {
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::hard_link(&staging, path)?;
        fs::remove_file(&staging)?;
        fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(not(unix))]
pub fn write(_path: &Path, _contents: &[u8]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}

#[cfg(unix)]
pub fn read(path: &Path) -> io::Result<String> {
    use std::{
        fs,
        io::Read,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "secret input must be a private regular file",
        ));
    }
    let file = fs::File::open(path)?;
    let opened = file.metadata()?;
    if opened.ino() != metadata.ino()
        || opened.dev() != metadata.dev()
        || opened.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::other("secret input changed during read"));
    }
    let mut text = String::new();
    file.take(16_385).read_to_string(&mut text)?;
    if text.len() > 16_384 {
        return Err(io::Error::other("secret input is too large"));
    }
    Ok(text.trim().to_owned())
}

#[cfg(not(unix))]
pub fn read(_path: &Path) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}
