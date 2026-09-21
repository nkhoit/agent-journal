//! Durable no-clobber publication of secret files.
use std::{io, path::Path};

#[cfg(unix)]
pub fn lock(path: &Path) -> io::Result<std::fs::File> {
    use fs2::FileExt;
    use std::{
        fs::{self, OpenOptions},
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if fs::metadata(parent)?.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "credential directory must be private (0700)",
        ));
    }
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("invalid credential path"))?;
    let lock_path = parent.join(format!(".{}.lock", name.to_string_lossy()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(lock_path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other("invalid credential state lock"));
    }
    file.lock_exclusive()?;
    Ok(file)
}

#[cfg(not(unix))]
pub fn lock(_path: &Path) -> io::Result<std::fs::File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}

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
    use std::{fs, io::Write, os::unix::fs::PermissionsExt};
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
    let (staging, mut file) = create_staging(parent, name, "pending")?;
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

#[cfg(unix)]
pub fn replace(path: &Path, expected: &[u8], contents: &[u8]) -> io::Result<()> {
    replace_with_parent_sync(path, expected, contents, |parent| {
        std::fs::File::open(parent)?.sync_all()
    })
}

#[cfg(unix)]
fn replace_with_parent_sync(
    path: &Path,
    expected: &[u8],
    contents: &[u8],
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    use std::{
        fs,
        io::{Read, Write},
        os::unix::fs::{MetadataExt, PermissionsExt},
    };
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::other(
            "secret input must be a private regular file",
        ));
    }
    let current = fs::File::open(path)?;
    let opened = current.metadata()?;
    if opened.ino() != metadata.ino() || opened.dev() != metadata.dev() {
        return Err(io::Error::other("secret input changed during read"));
    }
    let mut actual = Vec::new();
    current.take(16_385).read_to_end(&mut actual)?;
    if actual != expected {
        return Err(io::Error::other("secret input changed before replacement"));
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("invalid credential path"))?;
    let (staging, mut file) = create_staging(parent, name, "replacement")?;
    let result = (|| {
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&staging, path)?;
        sync_parent(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(unix)]
fn create_staging(
    parent: &Path,
    name: &std::ffi::OsStr,
    operation: &str,
) -> io::Result<(std::path::PathBuf, std::fs::File)> {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    for sequence in 0_u32.. {
        let staging = parent.join(format!(
            ".{}.{}.{}.{}",
            name.to_string_lossy(),
            operation,
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staging)
        {
            Ok(file) => return Ok((staging, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    unreachable!()
}

#[cfg(not(unix))]
pub fn replace(_path: &Path, _expected: &[u8], _contents: &[u8]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}

#[cfg(not(unix))]
pub fn write(_path: &Path, _contents: &[u8]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}

pub fn read(path: &Path) -> io::Result<String> {
    read_with_limit(path, 16_384)
}

#[cfg(unix)]
pub fn read_with_limit(path: &Path, max_bytes: u64) -> io::Result<String> {
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
    file.take(max_bytes.saturating_add(1))
        .read_to_string(&mut text)?;
    if text.len() as u64 > max_bytes {
        return Err(io::Error::other("secret input is too large"));
    }
    Ok(text.trim().to_owned())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    #[test]
    fn rename_success_is_visible_after_directory_sync_failure() {
        let directory =
            std::env::temp_dir().join(format!("private-file-sync-failure-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("registration");
        write(&path, b"pending").unwrap();
        let error = replace_with_parent_sync(&path, b"pending", b"completed", |_| {
            Err(io::Error::other("synthetic directory sync failure"))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(read(&path).unwrap(), "completed");
        fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(not(unix))]
pub fn read_with_limit(_path: &Path, _max_bytes: u64) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private credential files require Unix",
    ))
}
