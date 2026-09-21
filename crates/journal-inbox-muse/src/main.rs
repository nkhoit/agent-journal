fn main() {
    let code = match journal_inbox_muse::Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => match journal_inbox_muse::run(config) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("journal-inbox-muse: {error}");
                1
            }
        },
        Err(journal_inbox_muse::ConfigError::HelpRequested) => {
            println!("{}", journal_inbox_muse::Config::usage());
            0
        }
        Err(journal_inbox_muse::ConfigError::Invalid(message)) => {
            eprintln!(
                "journal-inbox-muse: {message}\n{}",
                journal_inbox_muse::Config::usage()
            );
            2
        }
    };
    std::process::exit(code);
}
