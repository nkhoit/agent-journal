fn main() {
    let code = journal_adapter_muse::run_stub(std::io::stderr(), "journal-adapter-muse");
    std::process::exit(code);
}
