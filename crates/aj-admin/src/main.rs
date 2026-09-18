fn main() {
    let code = aj_admin::run(
        &std::env::args().skip(1).collect::<Vec<_>>(),
        std::io::stdout(),
        std::io::stderr(),
    );
    std::process::exit(code);
}
