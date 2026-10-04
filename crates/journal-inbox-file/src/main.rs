fn main() {
    let code = match journal_inbox_file::Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => match journal_inbox_file::run(config) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("journal-inbox-file: {error}");
                1
            }
        },
        Err(journal_inbox_file::ConfigError::HelpRequested) => {
            println!("{}", journal_inbox_file::Config::usage());
            0
        }
        Err(journal_inbox_file::ConfigError::Invalid(message)) => {
            eprintln!(
                "journal-inbox-file: {message}\n{}",
                journal_inbox_file::Config::usage()
            );
            2
        }
    };
    std::process::exit(code);
}
