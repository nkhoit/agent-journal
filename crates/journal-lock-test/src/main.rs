#[cfg(not(unix))]
fn main() {
    eprintln!("journal-lock-fixture is Unix-only");
    std::process::exit(2);
}

#[cfg(unix)]
mod unix {
    use std::{
        env,
        fs::OpenOptions,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        process::Command,
    };

    use fs2::FileExt;
    use journal_storage_sqlite::Database;

    pub fn run() {
        let mut args = env::args_os();
        let _program = args.next();
        let mode = args.next().expect("fixture mode");
        if mode == "probe" {
            probe(Path::new(&args.next().expect("lock path")));
            return;
        }
        if mode == "central-open" {
            let database = PathBuf::from(args.next().expect("central path"));
            let audit = PathBuf::from(args.next().expect("audit path"));
            match Database::open_protected(database, audit) {
                Ok(_) => std::process::exit(0),
                Err(_) => std::process::exit(1),
            }
        }
        if mode == "sleep" {
            // This process reached exec. It must not retain any lock descriptor.
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        let root = PathBuf::from(args.next().expect("fixture root"));
        private_directory(&root);
        match mode.to_str().expect("UTF-8 mode") {
            "recovery-child-drop" => recovery_child_drop(&root),
            "recovery-parent-drop" => recovery_parent_drop(&root),
            "recovery-clone-exec" => recovery_clone_and_exec(&root),
            "recovery-constructor-error" => recovery_constructor_error(&root),
            "central-hardlink-alias" => central_hardlink_alias(&root),
            other => panic!("unknown fixture mode: {other}"),
        }
    }

    fn probe(path: &Path) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open probe lock");
        match file.try_lock_exclusive() {
            Ok(()) => {
                FileExt::unlock(&file).expect("unlock probe lock");
                std::process::exit(0);
            }
            Err(_) => std::process::exit(1),
        }
    }

    fn private_directory(path: &Path) {
        std::fs::create_dir_all(path).expect("create private fixture directory");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .expect("protect fixture directory");
    }

    fn probe_acquires(path: &Path) -> bool {
        Command::new(env::current_exe().expect("fixture executable"))
            .arg("probe")
            .arg(path)
            .status()
            .expect("run independent lock probe")
            .success()
    }

    fn expect_blocked(path: &Path) {
        assert!(
            !probe_acquires(path),
            "independent process acquired lock while owner remained live: {}",
            path.display()
        );
    }

    fn expect_released(path: &Path) {
        assert!(
            probe_acquires(path),
            "independent process could not acquire released lock: {}",
            path.display()
        );
    }

    fn fork() -> libc::pid_t {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
        pid
    }

    fn wait_for(pid: libc::pid_t) {
        let mut status = 0;
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(
            waited,
            pid,
            "waitpid failed: {}",
            std::io::Error::last_os_error()
        );
        assert!(
            libc::WIFEXITED(status),
            "child did not exit normally: {status}"
        );
        assert_eq!(libc::WEXITSTATUS(status), 0, "child failed: {status}");
    }

    fn stop_child(pid: libc::pid_t) {
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    }

    fn recovery_paths(root: &Path, name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let directory = root.join(name);
        private_directory(&directory);
        let database = directory.join("central.db");
        let audit = directory.join("audit.db");
        let lock = audit.with_extension("recovery-lock");
        (database, audit, lock)
    }

    fn recovery_child_drop(root: &Path) {
        let (database_path, audit_path, lock_path) = recovery_paths(root, "child-drop");
        let database =
            Database::open_protected(&database_path, &audit_path).expect("open recovery");
        expect_blocked(&lock_path);
        let child = fork();
        if child == 0 {
            drop(database);
            unsafe { libc::_exit(0) };
        }
        wait_for(child);
        expect_blocked(&lock_path);
        drop(database);
        expect_released(&lock_path);
    }

    fn recovery_parent_drop(root: &Path) {
        let (database_path, audit_path, lock_path) = recovery_paths(root, "parent-drop");
        let database =
            Database::open_protected(&database_path, &audit_path).expect("open recovery");
        expect_blocked(&lock_path);
        let child = fork();
        if child == 0 {
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        drop(database);
        expect_released(&lock_path);
        stop_child(child);
    }

    fn recovery_clone_and_exec(root: &Path) {
        let (database_path, audit_path, lock_path) = recovery_paths(root, "clone");
        let database =
            Database::open_protected(&database_path, &audit_path).expect("open recovery");
        let clone = database.clone();
        drop(database);
        expect_blocked(&lock_path);
        drop(clone);
        expect_released(&lock_path);

        let (database_path, audit_path, lock_path) = recovery_paths(root, "exec");
        let database =
            Database::open_protected(&database_path, &audit_path).expect("open recovery");
        let mut child = Command::new(env::current_exe().expect("fixture executable"))
            .arg("sleep")
            .spawn()
            .expect("exec fixture child");
        drop(database);
        expect_released(&lock_path);
        child.kill().expect("kill exec child");
        child.wait().expect("wait exec child");
    }

    fn recovery_constructor_error(root: &Path) {
        let directory = root.join("constructor-error");
        private_directory(&directory);
        let database_path = directory.join("central.db");
        let lock_path = database_path.with_extension("recovery-lock");
        assert!(Database::open_protected(&database_path, &database_path).is_err());
        expect_released(&lock_path);
    }

    fn central_hardlink_alias(root: &Path) {
        let (central, audit, _) = recovery_paths(root, "central-alias-nonconcurrent");
        drop(Database::open_protected(&central, &audit).expect("initialize protected central"));
        let alias = central.with_file_name("alias.db");
        let alias_audit = audit.with_file_name("alias-audit.db");
        std::fs::hard_link(&central, &alias).expect("create central hardlink alias");
        std::fs::copy(&audit, &alias_audit).expect("copy audit for central alias");
        std::fs::set_permissions(&alias_audit, std::fs::Permissions::from_mode(0o600))
            .expect("protect copied audit");
        let before = std::fs::read(&central).expect("read central before refusal");
        assert!(
            Database::open_protected(&alias, &alias_audit).is_err(),
            "non-concurrent same-inode alias opened"
        );
        assert_eq!(
            std::fs::read(&central).expect("read central after refusal"),
            before
        );
        assert!(
            !alias_audit.with_extension("recovery-lock").exists(),
            "non-concurrent alias refusal created an audit lock"
        );

        let (central, audit, _) = recovery_paths(root, "central-alias-live-owner");
        let owner = Database::open_protected(&central, &audit).expect("open live central owner");
        let alias = central.with_file_name("alias.db");
        let alias_audit = audit.with_file_name("alias-audit.db");
        std::fs::hard_link(&central, &alias).expect("create live central hardlink alias");
        std::fs::copy(&audit, &alias_audit).expect("copy audit for live central alias");
        std::fs::set_permissions(&alias_audit, std::fs::Permissions::from_mode(0o600))
            .expect("protect copied live audit");
        let before = std::fs::read(&central).expect("read live central before child refusal");
        let status = Command::new(env::current_exe().expect("fixture executable"))
            .arg("central-open")
            .arg(&alias)
            .arg(&alias_audit)
            .status()
            .expect("run concurrent central alias opener");
        assert!(
            !status.success(),
            "same-inode central alias opened in another process"
        );
        assert_eq!(
            std::fs::read(&central).expect("read central after child refusal"),
            before
        );
        drop(owner);
    }
}

#[cfg(unix)]
fn main() {
    unix::run();
}
