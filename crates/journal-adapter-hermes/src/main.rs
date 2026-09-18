use std::process::ExitCode;

fn main() -> ExitCode {
    let config = match journal_adapter_hermes::Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => config,
        Err(journal_adapter_hermes::ConfigError::HelpRequested) => {
            println!("{}", journal_adapter_hermes::Config::usage());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!(
                "journal-adapter-hermes: {error}\n\n{}",
                journal_adapter_hermes::Config::usage()
            );
            return ExitCode::from(2);
        }
    };

    match journal_adapter_hermes::run(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("journal-adapter-hermes: {error}");
            ExitCode::FAILURE
        }
    }
}
