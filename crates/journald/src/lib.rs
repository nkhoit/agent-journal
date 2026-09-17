use std::io::Write;

pub fn run_stub<W: Write>(mut output: W, name: &str) -> i32 {
    match writeln!(
        output,
        "{name}: not implemented (Agent Journal Rust scaffold only)"
    ) {
        Ok(()) => 2,
        Err(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_is_explicit_and_nonzero() {
        let mut output = Vec::new();
        assert_eq!(run_stub(&mut output, "journald"), 2);
        assert_eq!(
            String::from_utf8(output).expect("UTF-8"),
            "journald: not implemented (Agent Journal Rust scaffold only)\n"
        );
    }
}
