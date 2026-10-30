//! Nightly random-kill loop (issue #61): catch crash orderings nobody named.
//!
//! The harness spawns the real binary (`dev publish-fixture`) against a deterministic fixture
//! source, SIGKILLs it at a seeded random point — after a seeded number of stdout bytes, or after
//! a seeded delay, whichever comes first — then restarts and asserts the restart predicate
//! (ADR-0016) and the byte-identical re-run against an uncrashed golden run.
//!
//! This test is `#[ignore]`d: it never runs in per-PR CI (`cargo test --workspace`), only in the
//! scheduled `nightly-random-kill` workflow or by hand with `-- --ignored`. Offline, on temporary
//! directories. Each iteration logs its seed; a failure reports the seed for a one-command replay:
//!
//! ```sh
//! SOLOS_DATA_NIGHTLY_SEED=<seed> cargo test --test nightly_random_kill -- --ignored --nocapture
//! ```

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::writer::recover_files;
use solos_data::restart::{restart_predicate, tail_watermark};
use solos_data::store::Store;
use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The fixture publishes one row per raw table at one slot per epoch (an epoch is 432000 slots),
/// so a four-epoch range writes four files per table — enough distinct kill points.
const FROM: i64 = 100;

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("solos-nightly-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `(table, epoch, row_count, sha256, slot_from, slot_to)` of every active file, ordered. The
/// same shape as the crash-safety matrix (`tests/failpoints.rs`): the file name (a random UUID)
/// is deliberately not compared — byte-identity is the SHA-256 of the bytes on disk.
type Manifest = Vec<(String, i64, i64, String, i64, i64)>;

fn open(root: &Path) -> Store {
    Store::open(root, SCHEMA).unwrap()
}

fn manifest(store: &Store) -> Manifest {
    store
        .rows(
            "SELECT f.table_name, f.epoch, f.row_count, f.sha256, b.slot_from, b.slot_to
             FROM files f LEFT JOIN file_bounds b ON b.path = f.path
             WHERE f.status = 'active'
             ORDER BY f.table_name, f.epoch",
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.str("table_name").unwrap_or("").to_owned(),
                row.int("epoch").unwrap_or(0),
                row.int("row_count").unwrap_or(0),
                row.str("sha256").unwrap_or("").to_owned(),
                row.int("slot_from").unwrap_or(0),
                row.int("slot_to").unwrap_or(0),
            )
        })
        .collect()
}

/// Spawn the real binary to publish the deterministic fixture, printing one progress line per
/// file. stdout is piped so the harness can count bytes; stderr is dropped.
fn spawn(root: &Path, from: i64, to: i64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_solos-data"))
        .args(["dev", "publish-fixture", "--root"])
        .arg(root)
        .arg("--from")
        .arg(from.to_string())
        .arg("--to")
        .arg(to.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

/// One finished child run.
struct Run {
    status: std::process::ExitStatus,
    /// Everything the child wrote to stdout, in order.
    stdout: Vec<u8>,
}

/// Spawn and run to completion, or kill the child once `bytes >= n_bytes` (byte mode, sent from
/// the reader thread the moment the threshold byte arrives, so a seed maps deterministically to a
/// point in the output stream) or once `delay` has elapsed (delay mode, from the polling thread).
/// Both thresholds unreachable (`usize::MAX` / `None`) means run to completion.
fn run(root: &Path, from: i64, to: i64, n_bytes: usize, delay: Option<Duration>) -> Run {
    let mut child = spawn(root, from, to);
    let pid = child.id() as i32;
    let stdout = child.stdout.take().unwrap();
    let threshold = u64::try_from(n_bytes).unwrap_or(u64::MAX);
    let count = Arc::new(AtomicU64::new(0));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let reader_count = Arc::clone(&count);
    let reader_captured = Arc::clone(&captured);
    let reader = std::thread::spawn(move || {
        let mut stdout = stdout;
        let mut buf = [0u8; 4096];
        loop {
            match stdout.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let n64 = u64::try_from(n).unwrap_or(u64::MAX);
                    let total = reader_count.fetch_add(n64, Ordering::SeqCst) + n64;
                    reader_captured.lock().unwrap().extend_from_slice(&buf[..n]);
                    if total >= threshold {
                        // SIGKILL: process death, not a returned error (ADR-0016).
                        let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                    }
                }
                Err(_) => break,
            }
        }
    });

    let started = Instant::now();
    let deadline = started + Duration::from_secs(120);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if let Some(delay) = delay
            && started.elapsed() >= delay
        {
            let _ = child.kill();
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture did not exit within 120s");
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    reader.join().unwrap();
    Run {
        status,
        stdout: captured.lock().unwrap().clone(),
    }
}

/// The golden uncrashed run: its manifest and its total stdout byte length (the seed maps to a
/// byte offset inside this stream).
struct Golden {
    manifest: Manifest,
    stdout_len: usize,
}

fn golden(from: i64, to: i64) -> Golden {
    let root = TempDir::new("golden");
    let run = run(&root.0, from, to, usize::MAX, None);
    assert!(run.status.success(), "golden run failed: {}", run.status);
    let store = open(&root.0);
    let manifest = manifest(&store);
    assert!(
        !manifest.is_empty(),
        "the fixture must publish at least one file"
    );
    assert_eq!(
        tail_watermark(&store).unwrap(),
        Some(to),
        "the golden run must commit the tail watermark"
    );
    restart_predicate(&store, tail_watermark(&store).unwrap()).unwrap();
    store.close().unwrap();
    Golden {
        manifest,
        stdout_len: run.stdout.len(),
    }
}

/// splitmix64: a small deterministic PRNG so a seed always maps to the same kill point.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The byte threshold for a seed: `[1, stdout_len]`, so the kill lands inside the publication
/// stream rather than after the process has already finished.
fn byte_threshold(seed: u64, stdout_len: usize) -> usize {
    let mut state = seed;
    let span = stdout_len.max(1);
    1 + (splitmix64(&mut state) as usize % span)
}

/// The delay threshold for a seed in `delay` mode: `[0, SOLOS_DATA_NIGHTLY_MAX_DELAY_MS)`.
fn delay_millis(seed: u64) -> u64 {
    let mut state = seed.wrapping_add(0x5EED_0000);
    let max = std::env::var("SOLOS_DATA_NIGHTLY_MAX_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3_000)
        .max(1);
    splitmix64(&mut state) % max
}

/// After a kill, `recover_files` removes the interrupted publication's orphans and the restart
/// predicate must pass. The commit is atomic (ADR-0016 step 7): either nothing is committed
/// (watermark `None`, no active files) or everything is (watermark `Some(to)`, golden manifest).
/// Returns whether the watermark was already committed, i.e. no re-run is needed.
fn assert_after_kill(root: &Path, to: i64, golden: &Golden, seed: u64) -> bool {
    let mut store = open(root);
    recover_files(&mut store, root).unwrap();
    let watermark = tail_watermark(&store).unwrap();
    restart_predicate(&store, watermark).unwrap();
    let got = manifest(&store);
    let committed = match watermark {
        None => {
            assert!(
                got.is_empty(),
                "seed {seed}: watermark not committed but active files are registered"
            );
            false
        }
        Some(w) => {
            assert_eq!(w, to, "seed {seed}: tail watermark must be {to}");
            assert_eq!(
                &got, &golden.manifest,
                "seed {seed}: committed watermark without the full golden manifest"
            );
            true
        }
    };
    store.close().unwrap();
    committed
}

/// Re-run to completion from the watermark and require the byte-identical result: the same
/// manifest as golden and no duplicated primary key.
fn complete_and_compare(root: &Path, from: i64, to: i64, golden: &Golden, seed: u64) {
    let rerun = run(root, from, to, usize::MAX, None);
    assert!(
        rerun.status.success(),
        "seed {seed}: completion run failed: {}",
        rerun.status
    );
    let store = open(root);
    assert_eq!(tail_watermark(&store).unwrap(), Some(to));
    let got = manifest(&store);
    assert_eq!(
        &got, &golden.manifest,
        "seed {seed}: re-run is not byte-identical to the golden run"
    );
    assert_eq!(
        store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].int("n"),
        Some(i64::try_from(golden.manifest.len()).unwrap_or(0)),
        "seed {seed}: duplicate or missing file rows"
    );
    assert_eq!(
        store
            .rows("SELECT count(*) AS n FROM transactions", &[])
            .unwrap()[0]
            .int("n"),
        store
            .rows(
                "SELECT count(DISTINCT signature) AS n FROM transactions",
                &[]
            )
            .unwrap()[0]
            .int("n"),
        "seed {seed}: a transaction primary key was duplicated"
    );
    restart_predicate(&store, tail_watermark(&store).unwrap()).unwrap();
    store.close().unwrap();
}

fn run_iteration(seed: u64, from: i64, to: i64, golden: &Golden, mode: &str) {
    let root = TempDir::new(&format!("{seed}"));
    let (n_bytes, delay) = match mode {
        "delay" => (usize::MAX, Some(Duration::from_millis(delay_millis(seed)))),
        _ => (byte_threshold(seed, golden.stdout_len), None),
    };
    eprintln!(
        "seed={seed} kill={mode} bytes={n_bytes} stdout_len={}",
        golden.stdout_len
    );
    let run = run(&root.0, from, to, n_bytes, delay);
    if !run.status.success() && run.status.signal().is_none() {
        panic!(
            "seed {seed}: fixture exited with an error, not a kill: {}",
            run.status
        );
    }
    let committed = assert_after_kill(&root.0, to, golden, seed);
    if !committed {
        complete_and_compare(&root.0, from, to, golden, seed);
    }
}

#[test]
#[ignore = "nightly; run with -- --ignored (SOLOS_DATA_NIGHTLY_ITERATIONS / SOLOS_DATA_NIGHTLY_SEED)"]
fn nightly_random_kill_loop() {
    let iterations: u64 = std::env::var("SOLOS_DATA_NIGHTLY_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let seed_env = std::env::var("SOLOS_DATA_NIGHTLY_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let mode = std::env::var("SOLOS_DATA_NIGHTLY_KILL").unwrap_or_else(|_| "bytes".into());
    let epochs: i64 = std::env::var("SOLOS_DATA_NIGHTLY_EPOCHS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let from = FROM;
    let to = from + (epochs - 1) * 432_000;

    let golden = golden(from, to);
    eprintln!(
        "golden: {} files, {} stdout bytes",
        golden.manifest.len(),
        golden.stdout_len
    );

    let seeds: Vec<u64> = match seed_env {
        Some(seed) => vec![seed],
        None => (0..iterations).collect(),
    };
    for seed in seeds {
        run_iteration(seed, from, to, &golden, &mode);
    }
    eprintln!("PASS: {} iteration(s)", iterations.max(1));
}
