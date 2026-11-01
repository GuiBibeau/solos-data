//! Behavior of `augment hl-l2book-manifest`: the MD5 gate, sha256 of local bytes, atomic
//! resume, `.part` skipping, the coverage counts, and that the converter reads the output.

use super::checksum;
use super::manifest::{self, Request};
use crate::store::StoreError;
use std::io::Write;
use std::path::{Path, PathBuf};

struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hl-l2-manifest-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One finished book file on disk, arbitrary bytes (the builder never parses lz4).
fn write_book(input: &Path, date: &str, hour: u32, coin: &str, body: &[u8]) -> String {
    let dir = input.join(date).join(hour.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{coin}.lz4")), body).unwrap();
    format!("{date}/{hour}/{coin}.lz4")
}

/// The downloader listing rows: `(key, size, etag)`.
fn tsv(rows: &[(String, u64, String)]) -> String {
    let mut text = String::from("order\tkey\tsize\tetag\tcoin\tdate\tsplit\tpresent\n");
    for (index, (key, size, etag)) in rows.iter().enumerate() {
        text.push_str(&format!("{index}\t{key}\t{size}\t{etag}\tX\t20261022\ttrain\t1\n"));
    }
    text
}

fn row(path: &str, body: &[u8]) -> (String, u64, String) {
    (
        path.to_owned(),
        body.len() as u64,
        super::md5::md5_hex(body),
    )
}

fn build(input: &Path, tsv: &Path, out: &Path, jobs: u32) -> Result<manifest::Report, StoreError> {
    manifest::run(&Request { input, tsv, out, jobs })
}

fn body_a() -> Vec<u8> {
    b"the archive sends books, not promises".to_vec()
}

fn body_b() -> Vec<u8> {
    b"a second distinct object body".to_vec()
}

#[test]
fn builds_the_manifest_and_a_rerun_reuses_every_entry() {
    let root = Tmp::new("build");
    let input = root.0.join("in");
    let good = write_book(&input, "20261022", 0, "BTC", &body_a());
    let second = write_book(&input, "20261022", 0, "ETH", &body_b());
    // A finished file the TSV does not know.
    write_book(&input, "20261022", 1, "SOL", b"not in the listing");
    // A downloader still appending: the size gate fails before any hash.
    write_book(&input, "20261022", 2, "ARB", b"short");
    // A .part sibling, never read.
    std::fs::create_dir_all(input.join("20261022/3")).unwrap();
    std::fs::write(input.join("20261022/3/kPEPE.lz4.part"), b"partial").unwrap();

    let rows = vec![
        row(&good, &body_a()),
        row(&second, &body_b()),
        (
            "20261022/2/ARB.lz4".to_owned(),
            (body_a().len() as u64) * 10,
            "0".repeat(32),
        ),
    ];
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");
    let first = build(&input, &tsv_path, &out, 2).unwrap();
    assert_eq!(first.present, 4, "finished .lz4 books on disk");
    assert_eq!(first.covered, 2);
    assert_eq!(first.newly_hashed, 2);
    assert_eq!(first.reused, 0);
    assert_eq!(first.skipped_part, 1);
    assert_eq!(first.mismatched, 1, "the short file fails the size gate");
    assert_eq!(first.not_in_tsv, 1);

    let text = std::fs::read_to_string(&out).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(text.contains(&format!(
        "{} {} 20261022/0/BTC.lz4",
        crate::fsutil::sha256_hex(&body_a()),
        body_a().len()
    )));
    assert!(text.contains(&format!(
        "{} {} 20261022/0/ETH.lz4",
        crate::fsutil::sha256_hex(&body_b()),
        body_b().len()
    )));

    // The converter reads this exact format.
    let entries = checksum::load_entries(&out).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].coin, "BTC");
    assert_eq!(entries[0].sha256, crate::fsutil::sha256_hex(&body_a()));

    // A rerun reuses every entry and does not rewrite a byte.
    let before = std::fs::read(&out).unwrap();
    let second_run = build(&input, &tsv_path, &out, 2).unwrap();
    assert_eq!(second_run.reused, 2);
    assert_eq!(second_run.newly_hashed, 0);
    assert_eq!(second_run.covered, 2);
    assert_eq!(std::fs::read(&out).unwrap(), before, "the manifest moved");

    // A new finished file appends only itself.
    let added = write_book(&input, "20261023", 0, "LINK", &body_b());
    let mut rows2 = rows.clone();
    rows2.push(row(&added, &body_b()));
    std::fs::write(&tsv_path, tsv(&rows2)).unwrap();
    let third = build(&input, &tsv_path, &out, 2).unwrap();
    assert_eq!(third.reused, 2);
    assert_eq!(third.newly_hashed, 1);
    assert_eq!(third.covered, 3);
    assert_eq!(checksum::load_entries(&out).unwrap().len(), 3);
}

/// Mutation: drop the etag comparison in `classify`, or trust a non-MD5 etag.
#[test]
fn an_md5_mismatch_is_excluded_and_drops_a_stale_entry() {
    let root = Tmp::new("md5");
    let input = root.0.join("in");
    let path = write_book(&input, "20261022", 0, "BTC", &body_a());
    let rows = vec![(path.clone(), body_a().len() as u64, "f".repeat(32))];
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");
    // A stale entry from an earlier world, before this run notices the bytes changed.
    std::fs::write(
        &out,
        format!("{} {} {path}\n", "a".repeat(64), body_a().len()),
    )
    .unwrap();
    let report = build(&input, &tsv_path, &out, 1).unwrap();
    assert_eq!(report.mismatched, 1);
    assert_eq!(report.covered, 0);
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "",
        "a file that fails the gate cannot keep an entry"
    );
    // A multipart-style etag (`md5-N`) can never be gated either.
    let rows = vec![(path, body_a().len() as u64, "abc123-2".to_owned())];
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let report = build(&input, &tsv_path, &out, 1).unwrap();
    assert_eq!(report.mismatched, 1);
    assert_eq!(report.covered, 0);
}

/// Mutation: write the etag (or the size) as the first manifest field instead of the sha256 of
/// the local bytes.
#[test]
fn the_checksum_is_sha256_of_the_local_bytes() {
    let root = Tmp::new("sha-local");
    let input = root.0.join("in");
    let path = write_book(&input, "20261022", 0, "BTC", &body_a());
    let rows = vec![row(&path, &body_a())];
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");
    build(&input, &tsv_path, &out, 1).unwrap();
    let line = std::fs::read_to_string(&out).unwrap();
    assert!(
        line.starts_with(&crate::fsutil::sha256_hex(&body_a())),
        "the manifest records sha256 of the local bytes, not the md5 etag: {line}"
    );
    assert!(!line.contains(&super::md5::md5_hex(&body_a())));
}

#[test]
fn an_interrupted_run_resumes_and_never_leaves_a_torn_manifest() {
    let root = Tmp::new("resume");
    let input = root.0.join("in");
    let mut rows = Vec::new();
    for hour in 0..5u32 {
        let body = format!("object body number {hour}").into_bytes();
        let path = write_book(&input, "20261022", hour, "BTC", &body);
        rows.push(row(&path, &body));
    }
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");

    // A kill in the middle of a batch, before any write of it.
    manifest::fail_after_hashing(2);
    let error = build(&input, &tsv_path, &out, 3).unwrap_err();
    assert!(error.to_string().contains("injected"), "{error}");
    assert!(!out.exists(), "a stopped run wrote a partial manifest");

    // The resume completes and the manifest is well formed.
    let report = build(&input, &tsv_path, &out, 3).unwrap();
    assert_eq!(report.covered, 5);
    assert_eq!(report.newly_hashed, 5);
    assert_eq!(checksum::load_entries(&out).unwrap().len(), 5);

    // A manifest that lost lines (a crash between durable writes, or an older file) is
    // completed again: the kept entries are reused, the lost ones are re-hashed.
    let full = std::fs::read_to_string(&out).unwrap();
    let mut lines: Vec<&str> = full.lines().collect();
    lines.truncate(3);
    std::fs::write(&out, lines.join("\n") + "\n").unwrap();
    let resumed = build(&input, &tsv_path, &out, 3).unwrap();
    assert_eq!(resumed.reused, 3);
    assert_eq!(resumed.newly_hashed, 2);
    assert_eq!(resumed.covered, 5);
    assert_eq!(std::fs::read_to_string(&out).unwrap(), full);
}

/// Mutation: drop the mtime half of the reuse rule, so a rewritten file is never re-hashed.
#[test]
fn a_rewritten_file_with_the_same_size_is_rehashed() {
    let root = Tmp::new("mtime");
    let input = root.0.join("in");
    let path = write_book(&input, "20261022", 0, "BTC", &body_a());
    let rows = vec![row(&path, &body_a())];
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");
    build(&input, &tsv_path, &out, 1).unwrap();
    // Same bytes, same size, new mtime: the reuse rule must not fire. The sleep crosses the
    // coarsest mtime clock a filesystem reports.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(input.join(&path), &body_a()).unwrap();
    let report = build(&input, &tsv_path, &out, 1).unwrap();
    assert_eq!(report.newly_hashed, 1);
    assert_eq!(report.reused, 0);
    assert_eq!(report.covered, 1);
}

#[test]
fn a_gzip_tsv_and_the_cli_both_work() {
    let root = Tmp::new("gz-cli");
    let input = root.0.join("in");
    let path = write_book(&input, "20261022", 0, "BTC", &body_a());
    let rows = vec![row(&path, &body_a())];
    let plain = tsv(&rows);
    let gz_path = root.0.join("listing.tsv.gz");
    let mut encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(&gz_path).unwrap(),
        flate2::Compression::default(),
    );
    encoder.write_all(plain.as_bytes()).unwrap();
    encoder.finish().unwrap();
    let out = root.0.join("MANIFEST");
    let report = build(&input, &gz_path, &out, 4).unwrap();
    assert_eq!(report.covered, 1);
    assert!(out.is_file());
    assert!(root.0.join("MANIFEST.state.json").is_file());

    let code = crate::cli::main(&[
        "augment".to_owned(),
        "hl-l2book-manifest".to_owned(),
        "--input".to_owned(),
        input.to_string_lossy().into_owned(),
        "--tsv".to_owned(),
        gz_path.to_string_lossy().into_owned(),
        "--out".to_owned(),
        out.to_string_lossy().into_owned(),
        "--jobs".to_owned(),
        "2".to_owned(),
    ]);
    assert_eq!(code, 0);
    assert_eq!(checksum::load_entries(&out).unwrap().len(), 1);
}

/// The end-to-end pair: the builder's manifest drives the converter over real lz4 frames.
#[test]
fn the_converter_publishes_from_a_built_manifest() {
    let root = Tmp::new("end-to-end");
    let input = root.0.join("in");
    let btc = lz4_frame(&book_line("BTC"));
    let eth = lz4_frame(&book_line("ETH"));
    let btc_path = write_book(&input, "20261022", 0, "BTC", &btc);
    let eth_path = write_book(&input, "20261022", 0, "ETH", &eth);
    let rows = vec![row(&btc_path, &btc), row(&eth_path, &eth)];
    let tsv_path = root.0.join("listing.tsv");
    std::fs::write(&tsv_path, tsv(&rows)).unwrap();
    let out = root.0.join("MANIFEST");
    let built = build(&input, &tsv_path, &out, 2).unwrap();
    assert_eq!(built.covered, 2);

    let convert_out = root.0.join("parquet");
    let request = super::Request {
        input: &input,
        out: &convert_out,
        checksums: &out,
        min_free_bytes: 0,
        threads: 1,
        memory_limit: "512MB".to_owned(),
        git_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        coins: None,
        since: None,
        until: None,
        drop_raw: false,
        max_ratio: 1e12,
    };
    let report = super::run(&request, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(report.hours_verified, 1);
    assert_eq!(report.rows, 2);
}

fn book_line(coin: &str) -> String {
    serde_json::json!({
        "time": "2026-10-22T00:00:00.5",
        "raw": {"channel": "l2Book", "data": {
            "coin": coin,
            "time": 1_792_627_200_000_i64,
            "levels": [[{"px": "1", "sz": "1", "n": 1}], [{"px": "2", "sz": "1", "n": 1}]]
        }}
    })
    .to_string()
        + "\n"
}

fn lz4_frame(body: &str) -> Vec<u8> {
    let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
    encoder.write_all(body.as_bytes()).unwrap();
    let (frame, result) = encoder.finish();
    result.unwrap();
    frame
}
