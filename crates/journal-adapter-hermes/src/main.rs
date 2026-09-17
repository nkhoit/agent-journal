fn main() {
    let code = journal_adapter_hermes::run_stub(std::io::stderr(), "journal-adapter-hermes");
    std::process::exit(code);
}
