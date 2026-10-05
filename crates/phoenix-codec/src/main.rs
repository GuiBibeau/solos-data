//! Stdio codec: one JSON array of groups per input line, one JSON response per output line.
//! Kept for the TypeScript decoder during the migration; the Rust decoder links the library.

use std::io::{self, BufRead, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut out = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let groups: Vec<phoenix_codec::Group> = serde_json::from_str(&line?)?;
        writeln!(out, "{}", phoenix_codec::stdio_response(groups))?;
        out.flush()?;
    }
    Ok(())
}
