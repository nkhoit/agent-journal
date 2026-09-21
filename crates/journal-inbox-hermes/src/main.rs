use std::process::ExitCode;

fn main() -> ExitCode {
    let config = match journal_inbox_hermes::Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => config,
        Err(journal_inbox_hermes::ConfigError::HelpRequested) => {
            println!("{}", journal_inbox_hermes::Config::usage());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!(
                "journal-inbox-hermes: {error}\n\n{}",
                journal_inbox_hermes::Config::usage()
            );
            return ExitCode::from(2);
        }
    };

    match journal_inbox_hermes::run(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("journal-inbox-hermes: {error}");
            ExitCode::FAILURE
        }
    }
}
