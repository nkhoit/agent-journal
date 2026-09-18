fn main() {
    let code = aj::run(
        &std::env::args().skip(1).collect::<Vec<_>>(),
        std::io::stderr(),
    );
    std::process::exit(code);
}
