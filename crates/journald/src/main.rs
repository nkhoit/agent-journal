fn main() {
    let code = journald::run_stub(std::io::stderr(), "journald");
    std::process::exit(code);
}
