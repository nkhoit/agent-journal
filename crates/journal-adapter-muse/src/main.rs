fn main() {
    let code = match journal_adapter_muse::Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => match journal_adapter_muse::run(config) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("journal-adapter-muse: {error}");
                1
            }
        },
        Err(journal_adapter_muse::ConfigError::HelpRequested) => {
            println!("{}", journal_adapter_muse::Config::usage());
            0
        }
        Err(journal_adapter_muse::ConfigError::Invalid(message)) => {
            eprintln!(
                "journal-adapter-muse: {message}\n{}",
                journal_adapter_muse::Config::usage()
            );
            2
        }
    };
    std::process::exit(code);
}
