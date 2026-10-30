//! Nightly random-kill loop (issue #61): catch crash orderings nobody named.
//!
//! The harness spawns the real binary (`dev publish-fixture`) against a deterministic fixture
//! source. The fixture prints one progress line per publication step; a seed picks a publication
//! stage, then a progress line inside it, and SIGKILLs the process after that line's bytes arrive
//! plus a seeded jitter, so kills spread across the `tmp-write`, `tmp-fsync`, `rename`,
//! `dir-fsync`, `readback` and `commit` windows instead of clustering at file hashes.
//!
//! After a kill the harness classifies the iteration (killed or completed, the last progress line
//! seen, and whether the watermark was committed), then restarts the binary so its production
//! startup recovery removes the orphans (the harness does not call `recover_files` itself) and
//! asserts the restart predicate (ADR-0016), the byte-identical re-run and no orphan files.
//!
//! This test is `#[ignore]`d: it never runs in per-PR CI (`cargo test --workspace`), only in the
//! scheduled `nightly-random-kill` workflow or by hand with `-- --ignored`. Offline, on temporary
//! directories. A failure reports the seed and a one-command replay that pins every input that
//! changes the kill point:
//!
//! ```sh
//! SOLOS_DATA_NIGHTLY_SEED=<seed> SOLOS_DATA_NIGHTLY_KILL=bytes SOLOS_DATA_NIGHTLY_EPOCHS=4 \
//!   SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS=<ms> SOLOS_DATA_NIGHTLY_MAX_DELAY_MS=<ms> \
//!   cargo test --test nightly_random_kill -- --ignored --nocapture
//! ```

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::writer::TABLES;
use solos_data::restart::{restart_predicate, tail_watermark};
use solos_data::store::Store;
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The fixture publishes one row per raw table at one slot per epoch (an epoch is 432000 slots),
/// so a four-epoch range writes four files per table, enough distinct kill points per stage.
const FROM: i64 = 100;

/// The publication steps the fixture prints, in order. Each line is printed immediately before
/// the step runs, so a kill whose last seen line is `stage` landed inside that step's window.
/// `committed` is printed after the watermark transaction commits, before `published`.
const STAGES: [&str; 7] = [
    "tmp-write",
    "tmp-fsync",
    "rename",
    "dir-fsync",
    "readback",
    "commit",
    "committed",
];

fn stage_index(stage: &str) -> usize {
    STAGES.iter().position(|s| *s == stage).unwrap()
}

/// The stage named by one progress line, if any. File stages are `"<stage> <table> epoch=<n>"`;
/// `commit`, `committed` and `published` are single words.
fn stage_of_line(line: &str) -> Option<&'static str> {
    for stage in STAGES {
        if line == stage {
            return Some(stage);
        }
        if let Some(rest) = line.strip_prefix(stage)
            && rest.starts_with(' ')
        {
            return Some(stage);
        }
    }
    if line == "published" {
        return Some("published");
    }
    None
}

/// Byte offset and stage of every recognized progress line in a captured stdout.
fn progress_offsets(stdout: &[u8]) -> Vec<(usize, &'static str)> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    for line in stdout.split(|&b| b == b'\n') {
        let text = std::str::from_utf8(line)
            .unwrap_or("")
            .trim_end_matches('\r')
            .trim();
        if let Some(stage) = stage_of_line(text) {
            out.push((offset, stage));
        }
        offset += line.len() + 1;
    }
    out
}

/// The last progress line in a captured stdout (for classification), if any.
fn last_progress_stage(stdout: &[u8]) -> Option<&'static str> {
    stdout.split(|&b| b == b'\n').rev().find_map(|line| {
        let text = std::str::from_utf8(line)
            .unwrap_or("")
            .trim_end_matches('\r')
            .trim();
        stage_of_line(text)
    })
}

/// Keeps the root while the thread is panicking so the workflow can upload it as a failure
/// artifact; drops it otherwise.
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
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// The root and replay command of the iteration in flight, so the panic hook can log them and the
/// workflow can upload the root.
static CURRENT_ROOT: Mutex<Option<PathBuf>> = Mutex::new(None);
static REPLAY: Mutex<Option<String>> = Mutex::new(None);

fn set_current_root(root: &Path) {
    *CURRENT_ROOT.lock().unwrap() = Some(root.to_path_buf());
}

fn append_log(root: &Path, line: &str) {
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("harness.log"))
    {
        let _ = writeln!(file, "{line}");
    }
}

/// Every progress line goes to stderr and to the current root's `harness.log`, which is uploaded
/// with the root on failure.
fn log_line(line: &str) {
    eprintln!("{line}");
    if let Ok(root) = CURRENT_ROOT.lock()
        && let Some(root) = root.as_ref()
    {
        append_log(root, line);
    }
}

fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Ok(replay) = REPLAY.lock()
            && let Some(replay) = replay.as_ref()
        {
            eprintln!("replay: {replay}");
        }
        if let Ok(root) = CURRENT_ROOT.lock()
            && let Some(root) = root.as_ref()
        {
            append_log(root, &format!("PANIC: {}", info));
            if let Ok(replay) = REPLAY.lock()
                && let Some(replay) = replay.as_ref()
            {
                append_log(root, &format!("replay: {replay}"));
            }
        }
        default(info);
    }));
}

/// `(table, epoch, row_count, sha256, slot_from, slot_to)` of every active file, ordered. The
/// same shape as the crash-safety matrix (`tests/failpoints.rs`): the file name (a random UUID)
/// is deliberately not compared; byte-identity is the SHA-256 of the bytes on disk.
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

/// Spawn the real binary. With `recover_only` it only runs the production startup recovery;
/// otherwise it publishes the deterministic fixture `[from, to]`.
fn spawn(root: &Path, from: i64, to: i64, recover_only: bool) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_solos-data"));
    command.args(["dev", "publish-fixture", "--root"]).arg(root);
    if recover_only {
        command.arg("--recover-only");
    } else {
        command
            .arg("--from")
            .arg(from.to_string())
            .arg("--to")
            .arg(to.to_string());
    }
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// One finished child run.
struct Run {
    status: std::process::ExitStatus,
    /// Everything the child wrote to stdout, in order.
    stdout: Vec<u8>,
    /// Everything the child wrote to stderr, kept for the failure artifact.
    stderr: Vec<u8>,
    /// Wall-clock duration of the child run.
    duration: Duration,
}

/// Spawn and run to completion, or kill the child once `bytes >= n_bytes` plus `jitter` (byte
/// mode: the reader thread reads byte-by-byte and SIGKILLs the moment the threshold byte arrives
/// plus the jitter, so the kill lands with microsecond precision inside the step's window) or
/// once `delay` has elapsed (delay mode, from spawn). `usize::MAX` / `None` / `None` runs to
/// completion.
fn run(
    root: &Path,
    from: i64,
    to: i64,
    n_bytes: usize,
    delay: Option<Duration>,
    jitter: Option<Duration>,
    recover_only: bool,
) -> Run {
    let started = Instant::now();
    let mut child = spawn(root, from, to, recover_only);
    let pid = child.id() as i32;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let threshold = u64::try_from(n_bytes).unwrap_or(u64::MAX);
    let count = Arc::new(AtomicU64::new(0));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let stderr_captured = Arc::new(Mutex::new(Vec::new()));

    let stderr_reader = {
        let stderr_captured = Arc::clone(&stderr_captured);
        std::thread::spawn(move || {
            let mut stderr = stderr;
            let mut buf = Vec::new();
            let _ = stderr.read_to_end(&mut buf);
            *stderr_captured.lock().unwrap() = buf;
        })
    };

    let reader_count = Arc::clone(&count);
    let reader_captured = Arc::clone(&captured);
    let reader = std::thread::spawn(move || {
        let mut stdout = stdout;
        let mut byte = [0u8; 1];
        let mut fired = false;
        loop {
            match stdout.read(&mut byte) {
                Ok(0) => break,
                Ok(1) => {
                    let total = reader_count.fetch_add(1, Ordering::SeqCst) + 1;
                    reader_captured.lock().unwrap().push(byte[0]);
                    if !fired && total >= threshold {
                        fired = true;
                        if let Some(jitter) = jitter {
                            std::thread::sleep(jitter);
                        }
                        // SIGKILL from the reader thread: no polling latency, so the kill lands
                        // within the step whose line just crossed the threshold.
                        let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let deadline = started + Duration::from_secs(120);
    let delay_deadline = delay.map(|d| started + d);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        let now = Instant::now();
        if let Some(deadline) = delay_deadline
            && now >= deadline
        {
            let _ = child.kill();
        }
        if now > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture did not exit within 120s");
        }
        std::thread::sleep(Duration::from_micros(100));
    };
    reader.join().unwrap();
    stderr_reader.join().unwrap();
    Run {
        status,
        stdout: captured.lock().unwrap().clone(),
        stderr: stderr_captured.lock().unwrap().clone(),
        duration: started.elapsed(),
    }
}

/// The golden uncrashed run: its manifest, its stdout length, the number of progress lines, its
/// measured duration, and the byte offset of every progress line per stage (the seed maps to one
/// of these offsets).
struct Golden {
    manifest: Manifest,
    stdout_len: usize,
    progress_count: usize,
    duration: Duration,
    line_starts: Vec<Vec<usize>>,
}

fn golden(from: i64, to: i64) -> Golden {
    let root = TempDir::new("golden");
    set_current_root(&root.0);
    let run = run(&root.0, from, to, usize::MAX, None, None, false);
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
    restart_predicate(&store, tail_watermark(&store).unwrap())
        .unwrap_or_else(|e| panic!("golden run restart predicate failed: {e}"));
    store.close().unwrap();

    let offsets = progress_offsets(&run.stdout);
    let progress_count = offsets.iter().filter(|(_, s)| *s != "published").count();
    assert!(
        progress_count > 0,
        "the fixture must print at least one progress line"
    );
    let mut line_starts: Vec<Vec<usize>> = vec![Vec::new(); STAGES.len()];
    for (offset, stage) in offsets {
        if stage != "published" {
            line_starts[stage_index(stage)].push(offset);
        }
    }
    for starts in &line_starts {
        assert!(
            !starts.is_empty(),
            "every stage must have at least one progress line"
        );
    }
    Golden {
        manifest,
        stdout_len: run.stdout.len(),
        progress_count,
        duration: run.duration,
        line_starts,
    }
}

/// splitmix64: a small deterministic PRNG so a seed always maps to the same trigger point.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The byte trigger for a seed: a stage drawn uniformly first, then a progress line of that
/// stage, then the byte offset just inside that line. Drawing the stage first keeps the single
/// `commit` and `committed` lines from being starved by the per-file stages' many lines.
fn trigger_offset(seed: u64, golden: &Golden) -> (usize, &'static str) {
    let mut state = seed;
    let stage = STAGES[(splitmix64(&mut state) as usize) % STAGES.len()];
    let starts = &golden.line_starts[stage_index(stage)];
    let offset = starts[(splitmix64(&mut state) as usize) % starts.len()];
    (offset + 1, stage)
}

/// Seeded jitter after the trigger byte arrives: at least 1ms and up to `budget_ms` for the slow
/// steps (COPY, readback, commit), so the kill lands inside the step's work instead of exactly at
/// its first instruction. Zero for the sub-millisecond fsync/rename windows and for `committed`
/// (already after the atomic commit): the inline kill's microsecond precision is what spreads
/// those. The budget is pinned by `SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS` when set, otherwise it is
/// the golden run's measured duration over 8, clamped to [2, 25].
fn jitter_millis(seed: u64, budget_ms: u64, stage: &str) -> u64 {
    if matches!(stage, "tmp-fsync" | "rename" | "dir-fsync" | "committed") {
        return 0;
    }
    let mut state = seed.wrapping_add(0x6A09_E667_F3BC_C909);
    1 + (splitmix64(&mut state) % budget_ms.max(1))
}

/// The delay for a seed in `delay` mode: `[0, max_delay_ms)`, scaled to the golden run's measured
/// duration by default (the env var overrides the cap for manual experiments).
fn delay_millis(seed: u64, max_delay_ms: u64) -> u64 {
    let mut state = seed.wrapping_add(0x5EED_0000);
    splitmix64(&mut state) % max_delay_ms.max(1)
}

/// After recovery, no `.parquet` or `.tmp` may remain under the root: every file of the
/// interrupted publication was an orphan and the binary's startup recovery removed it.
fn assert_no_orphans(root: &Path) {
    fn walk(root: &Path, dir: &Path) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path);
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                assert!(
                    !name.ends_with(".parquet") && !name.ends_with(".tmp"),
                    "orphan left after recovery: {}",
                    path.strip_prefix(root).unwrap_or(&path).display()
                );
            }
        }
    }
    walk(root, root);
}

/// After a re-run (or a kill that landed after the commit), no `.tmp` may remain and every
/// `.parquet` under the root must be registered in `files`.
fn assert_no_unregistered(root: &Path, store: &Store) {
    let registered: HashSet<PathBuf> = store
        .rows("SELECT path FROM files", &[])
        .unwrap()
        .iter()
        .filter_map(|r| r.str("path"))
        .map(|p| solos_data::fsutil::manifest_path(root, Path::new(p)))
        .collect();
    fn walk(root: &Path, dir: &Path, registered: &HashSet<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, registered);
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.ends_with(".tmp") {
                    panic!("tmp file left after re-run: {}", path.display());
                }
                if name.ends_with(".parquet")
                    && !registered.contains(&solos_data::fsutil::manifest_path(root, &path))
                {
                    panic!(
                        "unregistered parquet left after re-run: {}",
                        path.strip_prefix(root).unwrap_or(&path).display()
                    );
                }
            }
        }
    }
    walk(root, root, &registered);
}

/// After a kill, the commit is atomic (ADR-0016 step 7): either nothing is committed (watermark
/// `None`, no active files) or everything is (watermark `Some(to)`, the golden manifest). Opening
/// the store replays the WAL, so this reads the true commit state. Returns whether the watermark
/// was committed. This assertion is what catches a planted "write `W` outside the file
/// transaction" ordering: files registered with no watermark violates the `None` arm.
fn committed_after_kill(root: &Path, to: i64, golden: &Golden, seed: u64) -> bool {
    let store = open(root);
    let watermark = tail_watermark(&store).unwrap();
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

/// After a kill that landed after the atomic commit: restart the binary's recovery (a no-op here)
/// and require no unregistered files and the restart predicate.
fn assert_committed_after_kill(root: &Path, from: i64, to: i64, seed: u64) {
    let recover = run(root, from, to, usize::MAX, None, None, true);
    assert!(
        recover.status.success(),
        "seed {seed}: recovery restart failed: {}",
        recover.status
    );
    let store = open(root);
    assert_no_unregistered(root, &store);
    let watermark = tail_watermark(&store).unwrap();
    restart_predicate(&store, watermark)
        .unwrap_or_else(|e| panic!("seed {seed}: restart predicate failed: {e}"));
    store.close().unwrap();
}

/// After a kill that landed before the commit: the binary restart recovers the orphans, then the
/// re-run to completion is byte-identical to the golden run and registers every file once.
fn assert_uncommitted_recovered(
    root: &Path,
    from: i64,
    to: i64,
    epochs: i64,
    golden: &Golden,
    seed: u64,
) {
    // 1. The binary restarts and runs the production startup recovery, removing the orphans.
    let recover = run(root, from, to, usize::MAX, None, None, true);
    assert!(
        recover.status.success(),
        "seed {seed}: recovery restart failed: {}",
        recover.status
    );
    assert_no_orphans(root);

    // 2. Re-run the publication to completion.
    let rerun = run(root, from, to, usize::MAX, None, None, false);
    assert!(
        rerun.status.success(),
        "seed {seed}: completion run failed: {}",
        rerun.status
    );

    let store = open(root);
    assert_no_unregistered(root, &store);
    assert_eq!(
        tail_watermark(&store).unwrap(),
        Some(to),
        "seed {seed}: completion run must commit the tail watermark"
    );
    let got = manifest(&store);
    assert_eq!(
        &got, &golden.manifest,
        "seed {seed}: re-run is not byte-identical to the golden run"
    );
    // One `files`/`file_bounds` row per published file, and exactly the fixture's rows per raw
    // table (no duplicated or missing rows; the PRIMARY KEY alone is not asserted here).
    assert_eq!(
        store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].int("n"),
        Some(i64::try_from(golden.manifest.len()).unwrap_or(0)),
        "seed {seed}: duplicate or missing file rows"
    );
    assert_eq!(
        store
            .rows("SELECT count(*) AS n FROM file_bounds", &[])
            .unwrap()[0]
            .int("n"),
        Some(i64::try_from(golden.manifest.len()).unwrap_or(0)),
        "seed {seed}: duplicate or missing file_bounds rows"
    );
    let expected = epochs;
    for table in TABLES {
        let n = store
            .rows(&format!("SELECT count(*) AS n FROM {table}"), &[])
            .unwrap()[0]
            .int("n");
        assert_eq!(
            n,
            Some(expected),
            "seed {seed}: {table} row count must match the fixture"
        );
    }
    restart_predicate(&store, tail_watermark(&store).unwrap())
        .unwrap_or_else(|e| panic!("seed {seed}: restart predicate failed: {e}"));
    store.close().unwrap();
}

/// Histogram of the iteration classes.
#[derive(Default)]
struct Tally {
    /// Iterations by the deterministic trigger stage (byte mode), reported alongside where the
    /// kill actually landed.
    triggered: BTreeMap<&'static str, usize>,
    /// Killed iterations by the last progress line actually seen before the kill landed.
    killed_by_stage: BTreeMap<&'static str, usize>,
    /// Killed iterations whose watermark was committed.
    killed_committed: usize,
    /// Killed iterations whose watermark was not committed.
    killed_uncommitted: usize,
    /// Iterations where the child exited 0 before the kill landed (vacuous).
    completed: usize,
}

fn record(
    tally: &mut Tally,
    seed: u64,
    status: &std::process::ExitStatus,
    trigger_stage: Option<&'static str>,
    last_stage: Option<&'static str>,
    committed: bool,
) {
    if let Some(stage) = trigger_stage {
        *tally.triggered.entry(stage).or_default() += 1;
    }
    if status.success() {
        tally.completed += 1;
        log_line(&format!(
            "hist: seed={seed} completed-before-kill committed={committed}"
        ));
    } else if status.signal().is_some() {
        let stage = last_stage.unwrap_or("before-first-line");
        *tally.killed_by_stage.entry(stage).or_default() += 1;
        if committed {
            tally.killed_committed += 1;
        } else {
            tally.killed_uncommitted += 1;
        }
        log_line(&format!(
            "hist: seed={seed} killed last={stage} committed={committed}"
        ));
    } else {
        panic!("seed {seed}: fixture exited with an error, not a kill: {status}");
    }
}

fn print_histogram(tally: &Tally) {
    for stage in STAGES {
        log_line(&format!(
            "histogram: triggered {stage}: {}",
            tally.triggered.get(stage).copied().unwrap_or(0)
        ));
    }
    for stage in STAGES {
        log_line(&format!(
            "histogram: killed after {stage}: {}",
            tally.killed_by_stage.get(stage).copied().unwrap_or(0)
        ));
    }
    if let Some(n) = tally.killed_by_stage.get("before-first-line") {
        log_line(&format!("histogram: killed before-first-line: {n}"));
    }
    log_line(&format!(
        "histogram: killed committed: {}",
        tally.killed_committed
    ));
    log_line(&format!(
        "histogram: killed uncommitted: {}",
        tally.killed_uncommitted
    ));
    log_line(&format!(
        "histogram: completed-before-kill: {}",
        tally.completed
    ));
}

/// The fixed loop configuration shared across iterations.
struct Loop<'a> {
    from: i64,
    to: i64,
    epochs: i64,
    mode: &'a str,
    max_delay_ms: u64,
    jitter_budget_ms: u64,
    golden: &'a Golden,
}

fn run_iteration(seed: u64, loop_: &Loop<'_>, tally: &mut Tally) -> TempDir {
    let root = TempDir::new(&format!("{seed}"));
    set_current_root(&root.0);
    let (n_bytes, delay, jitter, trigger_stage) = match loop_.mode {
        "delay" => {
            let delay = Duration::from_millis(delay_millis(seed, loop_.max_delay_ms));
            (usize::MAX, Some(delay), None, None)
        }
        _ => {
            let (offset, stage) = trigger_offset(seed, loop_.golden);
            let jitter = Duration::from_millis(jitter_millis(seed, loop_.jitter_budget_ms, stage));
            (offset, None, Some(jitter), Some(stage))
        }
    };
    log_line(&format!(
        "seed={seed} kill={} bytes={n_bytes} delay={delay:?} jitter={jitter:?} trigger={}",
        loop_.mode,
        trigger_stage.unwrap_or("delay")
    ));
    let run = run(&root.0, loop_.from, loop_.to, n_bytes, delay, jitter, false);
    if !run.stderr.is_empty() {
        append_log(
            &root.0,
            &format!("fixture stderr:\n{}", String::from_utf8_lossy(&run.stderr)),
        );
    }
    if !run.status.success() && run.status.signal().is_none() {
        panic!(
            "seed {seed}: fixture exited with an error, not a kill: {}",
            run.status
        );
    }
    let committed = committed_after_kill(&root.0, loop_.to, loop_.golden, seed);
    let last_stage = last_progress_stage(&run.stdout);
    record(
        tally,
        seed,
        &run.status,
        trigger_stage,
        last_stage,
        committed,
    );
    log_line(&format!(
        "seed={seed} outcome: killed={} completed={} last={:?} committed={committed}",
        run.status.signal().is_some(),
        run.status.success(),
        last_stage
    ));

    if run.status.success() {
        // Completed before the kill landed: `committed_after_kill` already validated the
        // committed manifest; there is nothing to recover.
    } else if committed {
        assert_committed_after_kill(&root.0, loop_.from, loop_.to, seed);
    } else {
        assert_uncommitted_recovered(
            &root.0,
            loop_.from,
            loop_.to,
            loop_.epochs,
            loop_.golden,
            seed,
        );
    }
    root
}

#[test]
#[ignore = "nightly; run with -- --ignored (SOLOS_DATA_NIGHTLY_ITERATIONS / SOLOS_DATA_NIGHTLY_SEED)"]
fn nightly_random_kill_loop() {
    install_panic_hook();
    let iterations: u64 = std::env::var("SOLOS_DATA_NIGHTLY_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let seed_env = match std::env::var("SOLOS_DATA_NIGHTLY_SEED") {
        Ok(v) if !v.trim().is_empty() => match v.trim().parse::<u64>() {
            Ok(seed) => Some(seed),
            Err(_) => panic!("SOLOS_DATA_NIGHTLY_SEED is not a valid u64: {v:?}"),
        },
        _ => None,
    };
    let mode = std::env::var("SOLOS_DATA_NIGHTLY_KILL").unwrap_or_else(|_| "bytes".into());
    assert!(
        mode == "bytes" || mode == "delay",
        "SOLOS_DATA_NIGHTLY_KILL must be 'bytes' or 'delay', got {mode:?}"
    );
    let epochs: i64 = std::env::var("SOLOS_DATA_NIGHTLY_EPOCHS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let from = FROM;
    let to = from + (epochs - 1) * 432_000;

    let golden = golden(from, to);
    let max_delay_ms = std::env::var("SOLOS_DATA_NIGHTLY_MAX_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(|| golden.duration.as_millis().max(1) as u64);
    let jitter_budget_ms = match std::env::var("SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS") {
        Ok(v) if !v.trim().is_empty() => match v.trim().parse::<u64>() {
            Ok(budget) => budget,
            Err(_) => panic!("SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS is not a valid u64: {v:?}"),
        },
        _ => (golden.duration.as_millis() as u64 / 8).clamp(2, 25),
    };
    log_line(&format!(
        "golden: {} files, {} progress lines, {} stdout bytes, {} ms, jitter budget {} ms",
        golden.manifest.len(),
        golden.progress_count,
        golden.stdout_len,
        golden.duration.as_millis(),
        jitter_budget_ms
    ));

    let seeds: Vec<u64> = match seed_env {
        Some(seed) => vec![seed],
        None => (0..iterations).collect(),
    };
    let loop_ = Loop {
        from,
        to,
        epochs,
        mode: &mode,
        max_delay_ms,
        jitter_budget_ms,
        golden: &golden,
    };
    let mut tally = Tally::default();
    // The last iteration's root stays alive across the histogram assertions, so a failure there
    // keeps the root (with its harness.log) for the workflow artifact upload.
    let mut _last_root: Option<TempDir> = None;
    for seed in &seeds {
        let replay = format!(
            "SOLOS_DATA_NIGHTLY_SEED={seed} SOLOS_DATA_NIGHTLY_KILL={mode} \
             SOLOS_DATA_NIGHTLY_EPOCHS={epochs} \
             SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS={jitter_budget_ms} \
             SOLOS_DATA_NIGHTLY_MAX_DELAY_MS={max_delay_ms} \
             cargo test --test nightly_random_kill -- --ignored --nocapture"
        );
        *REPLAY.lock().unwrap() = Some(replay);
        _last_root = Some(run_iteration(*seed, &loop_, &mut tally));
    }

    print_histogram(&tally);

    // Replay mode (a single seed) has no spread to assert; the full loop must land a kill in
    // every publication stage, exercise both commit states, and keep the vacuous
    // completed-before-kill class under a cap.
    if seed_env.is_none() {
        // The vacuous class comes first: if too many iterations completed before the kill
        // landed, the loop is not exercising the crash windows at all.
        let cap = (seeds.len() / 20).max(1);
        assert!(
            tally.completed <= cap,
            "{} iterations completed before the kill landed (cap {cap}); the kill windows are vacuous",
            tally.completed
        );
        // Byte mode draws the trigger stage deterministically, but the assertion is on where the
        // kill actually landed: every publication stage must have at least one kill land in its
        // window. Delay mode spreads by wall-clock time instead and skips this check.
        if mode != "delay" {
            for stage in STAGES {
                let n = tally.killed_by_stage.get(stage).copied().unwrap_or(0);
                assert!(
                    n > 0,
                    "no kill landed in the {stage} stage; the kill windows are not spread"
                );
            }
        }
        assert!(
            tally.killed_committed > 0,
            "no kill landed after the watermark commit"
        );
        assert!(
            tally.killed_uncommitted > 0,
            "no kill landed before the watermark commit"
        );
    }

    eprintln!("PASS: {} iteration(s)", seeds.len());
}
