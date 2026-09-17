use std::io::Write;

pub fn run_stub<W: Write>(mut output: W, name: &str) -> i32 {
    match writeln!(
        output,
        "{name}: not implemented (Agent Journal Rust scaffold only; runtime injection unresolved)"
    ) {
        Ok(()) => 2,
        Err(_) => 1,
    }
}

pub fn runtime_status() -> &'static str {
    journal_runtime_hermes::STATUS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_is_explicit_and_runtime_status_is_unresolved() {
        let mut output = Vec::new();
        assert_eq!(run_stub(&mut output, "journal-adapter-hermes"), 2);
        assert!(
            String::from_utf8(output)
                .expect("UTF-8")
                .contains("not implemented")
        );
        assert_eq!(runtime_status(), "unresolved");
    }
}
