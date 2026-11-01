//! Move sealed raw epochs to R2: verify the object, record it, then optionally delete the
//! local file. The catalog update that records `both` is fsync-durable before any delete, so a
//! crash between those steps leaves the local file in place and the catalog consistent.

use super::catalog::write_catalog;
use super::object_store::ObjectStore;
use super::seal::sealed_epoch_ids;
use super::writer::{RemoteSample, verify_files_with};
use crate::fsutil::{file_hash, sync_dir_after_rename, tmp_path};
use crate::jsonout::{Obj, log, now};
use crate::store::{Store, StoreError};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// `maintain offload`.
pub struct OffloadRequest<'a> {
    /// Epochs to visit.
    pub epochs: &'a [i64],
    /// Require a matching R2 size and SHA-256. The command refuses to run without it.
    pub verify: bool,
    /// Delete local copies after the catalog records `both`. Refused for an unsealed epoch.
    pub drop_local: bool,
    /// Held for the whole drop so the decoder cannot start. Required when `drop_local` is set.
    pub decoded_root: Option<&'a Path>,
}

/// `phoenix_raw/` plus the path relative to the raw root. That is the backup object key.
pub fn object_key(root: &Path, path: &str) -> Result<String, StoreError> {
    let full = Path::new(path);
    let rel = full
        .strip_prefix(root)
        .map_err(|_| StoreError::Check("registered file is outside the raw root".into()))?;
    let rel = rel.to_string_lossy().replace('\\', "/");
    if rel.is_empty() || rel.split('/').any(|part| part == ".." || part.is_empty()) {
        return Err(StoreError::Check(
            "registered file is outside the raw root".into(),
        ));
    }
    Ok(format!("phoenix_raw/{rel}"))
}

/// True when a catalog entry's durable copy is R2 alone.
#[must_use]
pub fn is_r2_file(file: &Value) -> bool {
    file.get("location").and_then(Value::as_str) == Some("r2")
}

/// Refuse to open a catalog that still contains an r2-only file the caller would read.
pub fn refuse_r2_only(files: &[Value]) -> Result<(), StoreError> {
    if let Some(file) = files.iter().find(|file| is_r2_file(file)) {
        let key = file.get("r2_key").and_then(Value::as_str).unwrap_or("");
        return Err(StoreError::Check(format!(
            "r2-only file; restore the epoch before reading ({key})"
        )));
    }
    Ok(())
}

/// Confirm `key` exists with `size` bytes and SHA-256 `sha`.
///
/// A stored checksum is compared directly. Without one the body is streamed and hashed.
///
/// # Errors
///
/// The object is missing, or the size or SHA-256 does not match.
pub fn confirm_remote(
    objects: &dyn ObjectStore,
    key: &str,
    size: u64,
    sha: &str,
) -> Result<(), StoreError> {
    let Some(head) = objects.head(key)? else {
        return Err(StoreError::Check(format!("R2 object missing: {key}")));
    };
    if head.size != size {
        return Err(StoreError::Check(format!("R2 size mismatch for {key}")));
    }
    let remote = match &head.sha256 {
        Some(stored) => stored.clone(),
        None => stream_sha256(objects, key)?,
    };
    if remote != sha {
        return Err(StoreError::Check(format!("R2 sha256 mismatch for {key}")));
    }
    Ok(())
}

fn stream_sha256(objects: &dyn ObjectStore, key: &str) -> Result<String, StoreError> {
    let mut hasher = Sha256::new();
    let mut sink = HashSink {
        hasher: &mut hasher,
    };
    objects.write_to(key, &mut sink)?;
    Ok(hex::encode(hasher.finalize()))
}

struct HashSink<'a> {
    hasher: &'a mut Sha256,
}

impl Write for HashSink<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Verify R2 copies of `epochs` and, with [`OffloadRequest::drop_local`], delete local files.
///
/// # Errors
///
/// `--verify` is missing, the decoder is still running during a drop, or any object fails
/// the size or SHA-256 check. A failure stops the run; files already recorded stay recorded.
pub fn offload(
    store: &mut Store,
    objects: &dyn ObjectStore,
    request: &OffloadRequest<'_>,
) -> Result<Obj, StoreError> {
    if !request.verify {
        return Err(StoreError::Check(
            "offload requires --verify; refusing to record or delete without it".into(),
        ));
    }
    if request.drop_local && request.decoded_root.is_none() {
        return Err(StoreError::Check(
            "drop-local requires the decoded checkpoint path; stop the decoder first".into(),
        ));
    }
    // Hold the decoder lock for the whole drop. Releasing it early would let the decoder
    // start while files are being unlinked.
    let decoder = match (request.drop_local, request.decoded_root) {
        (true, Some(root)) => Some(open_decoder(root)?),
        _ => None,
    };
    let wanted: HashSet<i64> = request.epochs.iter().copied().collect();
    let sealed = sealed_epoch_ids(store)?;
    let files = store.rows(
        "SELECT path, table_name, epoch, row_count, sha256, byte_size, location, r2_key FROM files WHERE status='active' ORDER BY epoch, path",
        &[],
    )?;
    let mut verified = 0i64;
    let mut dropped = 0i64;
    let mut caught_up = 0i64;
    let mut kept_local = 0i64;
    let mut already = 0i64;
    for file in &files {
        let epoch = file.int("epoch").unwrap_or(-1);
        if !wanted.contains(&epoch) {
            continue;
        }
        let path_text = file.str("path").unwrap_or("").to_owned();
        let sha = file.str("sha256").unwrap_or("").to_owned();
        let location = file.str("location").unwrap_or("local").to_owned();
        let path = PathBuf::from(&path_text);
        let local_exists = path.is_file();
        if location == "r2" && !local_exists {
            already += 1;
            continue;
        }
        let key = object_key(&store.root, &path_text)?;
        let size = expected_size(&path, local_exists, file, &sha)?;
        confirm_remote(objects, &key, size, &sha)?;
        if location != "both" && location != "r2" {
            set_location(store, &path_text, "both", &key, size)?;
            let root = store.root.clone();
            // The `?` is the guard. A failed catalog fsync must abort before any unlink.
            // Ignoring this error deletes the local file with no durable catalog.
            write_catalog(store, &root)?;
        } else if location == "r2" && local_exists {
            // A restored file is local again. Record both before a second drop.
            set_location(store, &path_text, "both", &key, size)?;
            let root = store.root.clone();
            write_catalog(store, &root)?;
        }
        verified += 1;
        log(
            "offload_verified",
            Obj::new()
                .with("r2Key", key.clone())
                .with("sha256", sha.clone())
                .with("bytes", i64::try_from(size).unwrap_or(i64::MAX))
                .with("epoch", epoch),
        );
        if halt_before_drop() && request.drop_local {
            return Err(StoreError::Check(
                "interrupted after verify before delete".into(),
            ));
        }
        if !request.drop_local {
            kept_local += 1;
            continue;
        }
        if !sealed.contains(&epoch) {
            kept_local += 1;
            continue;
        }
        if local_exists {
            std::fs::remove_file(&path)?;
            if let Some(parent) = path.parent() {
                sync_dir_after_rename(parent)?;
            }
            log(
                "offload_deleted",
                Obj::new()
                    .with("r2Key", key.clone())
                    .with("sha256", sha.clone())
                    .with("bytes", i64::try_from(size).unwrap_or(i64::MAX))
                    .with("epoch", epoch),
            );
            dropped += 1;
        } else {
            caught_up += 1;
        }
        set_location(store, &path_text, "r2", &key, size)?;
        let root = store.root.clone();
        write_catalog(store, &root)?;
    }
    drop(decoder);
    Ok(Obj::new()
        .with("at", now())
        .with("ok", true)
        .with("verified", verified)
        .with("dropped", dropped)
        .with("caughtUp", caught_up)
        .with("keptLocal", kept_local)
        .with("alreadyR2", already))
}

fn expected_size(
    path: &Path,
    local_exists: bool,
    file: &Obj,
    sha: &str,
) -> Result<u64, StoreError> {
    if local_exists {
        let len = std::fs::metadata(path)?.len();
        if file_hash(path)? != sha {
            return Err(StoreError::Check(format!(
                "local sha256 mismatch for {}",
                path.file_name().and_then(|n| n.to_str()).unwrap_or("file")
            )));
        }
        return Ok(len);
    }
    let recorded = file.int("byte_size").unwrap_or(-1);
    if recorded < 0 {
        return Err(StoreError::Check(
            "local file is missing and the catalog has no size".into(),
        ));
    }
    Ok(u64::try_from(recorded).unwrap_or(0))
}

fn set_location(
    store: &mut Store,
    path: &str,
    location: &str,
    key: &str,
    size: u64,
) -> Result<(), StoreError> {
    let bytes = i64::try_from(size).unwrap_or(i64::MAX);
    store.exec(
        "UPDATE files SET location=?, r2_key=?, byte_size=? WHERE path=?",
        &[&location, &key, &bytes, &path],
    )?;
    Ok(())
}

/// The stored path must normalize strictly inside `root`. A `..` segment that escapes, or a
/// path in another directory, is refused before restore writes it.
fn require_inside_root(root: &Path, path: &Path) -> Result<(), StoreError> {
    let root = crate::fsutil::normalize(root);
    let full = crate::fsutil::normalize(path);
    if full == root || !full.starts_with(&root) {
        return Err(StoreError::Check(
            "registered file is outside the raw root".into(),
        ));
    }
    Ok(())
}

fn open_decoder(root: &Path) -> Result<Store, StoreError> {
    Store::open(root, crate::decoder::schema::schema_static()).map_err(|error| match error {
        StoreError::AlreadyOwned { .. } => StoreError::Check(
            "decoder checkpoint is owned; stop solos-data-phoenix-decoder.service before --drop-local"
                .into(),
        ),
        other => other,
    })
}

/// Pull `epochs` back from R2 and require each SHA-256 to match the catalog.
///
/// # Errors
///
/// A download's SHA-256 does not match, or the local file is present and already differs.
/// A mismatch leaves no local file and does not change `location`.
pub fn restore(
    store: &mut Store,
    objects: &dyn ObjectStore,
    epochs: &[i64],
) -> Result<Obj, StoreError> {
    let wanted: HashSet<i64> = epochs.iter().copied().collect();
    let files = store.rows(
        "SELECT path, epoch, sha256, byte_size, location, r2_key FROM files WHERE status='active' ORDER BY epoch, path",
        &[],
    )?;
    let mut restored = 0i64;
    let mut already = 0i64;
    for file in &files {
        let epoch = file.int("epoch").unwrap_or(-1);
        if !wanted.contains(&epoch) {
            continue;
        }
        let path_text = file.str("path").unwrap_or("").to_owned();
        let sha = file.str("sha256").unwrap_or("").to_owned();
        let path = PathBuf::from(&path_text);
        // The catalog path is whatever was stored. Refuse one that normalizes outside the raw root
        // before any download or overwrite.
        require_inside_root(&store.root, &path)?;
        let key = file
            .str("r2_key")
            .map(str::to_owned)
            .filter(|key| !key.is_empty())
            .map_or_else(|| object_key(&store.root, &path_text), Ok)?;
        if path.is_file() {
            if file_hash(&path)? != sha {
                return Err(StoreError::Check(format!(
                    "local file does not match the catalog hash; restore will not overwrite {key}"
                )));
            }
            if file.str("location") == Some("r2") {
                let size = std::fs::metadata(&path)?.len();
                set_location(store, &path_text, "both", &key, size)?;
                let root = store.root.clone();
                write_catalog(store, &root)?;
            }
            already += 1;
            continue;
        }
        if let Some(parent) = path.parent() {
            crate::fsutil::create_dir_all_durable(parent)?;
        }
        let tmp = tmp_path(&path);
        let downloaded = match download_hashed(objects, &key, &tmp) {
            Ok(hash) => hash,
            Err(error) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(error);
            }
        };
        if downloaded != sha {
            let _ = std::fs::remove_file(&tmp);
            return Err(StoreError::Check(format!(
                "restored sha256 mismatch for {key}"
            )));
        }
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            sync_dir_after_rename(parent)?;
        }
        let size = std::fs::metadata(&path)?.len();
        set_location(store, &path_text, "both", &key, size)?;
        let root = store.root.clone();
        write_catalog(store, &root)?;
        restored += 1;
        log(
            "offload_restored",
            Obj::new()
                .with("r2Key", key)
                .with("sha256", sha)
                .with("bytes", i64::try_from(size).unwrap_or(i64::MAX))
                .with("epoch", epoch),
        );
    }
    Ok(Obj::new()
        .with("at", now())
        .with("ok", true)
        .with("restored", restored)
        .with("alreadyLocal", already))
}

fn download_hashed(objects: &dyn ObjectStore, key: &str, tmp: &Path) -> Result<String, StoreError> {
    let mut hasher = Sha256::new();
    let mut file = std::fs::File::create(tmp)?;
    struct Tee<'a> {
        file: &'a mut std::fs::File,
        hasher: &'a mut Sha256,
    }
    impl Write for Tee<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.hasher.update(buf);
            self.file.write_all(buf)?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    objects.write_to(
        key,
        &mut Tee {
            file: &mut file,
            hasher: &mut hasher,
        },
    )?;
    file.sync_all()?;
    Ok(hex::encode(hasher.finalize()))
}

/// Local files older than `older_than` whose R2 object is missing.
///
/// Catalogued files use `created_at`. Parquet on disk that the catalog does not list uses
/// mtime. This does not open DuckDB.
///
/// # Errors
///
/// The catalog cannot be read, or the object store listing fails.
pub fn backup_gap(
    root: &Path,
    objects: &dyn ObjectStore,
    older_than: Duration,
    now_at: chrono::DateTime<chrono::Utc>,
) -> Result<Obj, StoreError> {
    let catalog_path = root.join("catalog.json");
    let catalog: Value = if catalog_path.is_file() {
        serde_json::from_str(&std::fs::read_to_string(&catalog_path)?)
            .map_err(|error| StoreError::Check(error.to_string()))?
    } else {
        Value::Null
    };
    let files = catalog
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut known = HashSet::new();
    let mut gaps = Vec::new();
    for file in &files {
        let path = file.get("path").and_then(Value::as_str).unwrap_or("");
        if let Ok(rel) = path_rel(root, path) {
            known.insert(rel);
        }
        let location = file
            .get("location")
            .and_then(Value::as_str)
            .unwrap_or("local");
        if location == "r2" {
            continue;
        }
        let created = file
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(parse_time);
        let old = created.is_none_or(|at| is_old(at, now_at, older_than));
        if !old {
            continue;
        }
        let key = file
            .get("r2_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .or_else(|| object_key(root, path).ok());
        let Some(key) = key else {
            // A catalogued local file with no object key cannot be proved present in R2.
            // Skipping it hid the gap.
            gaps.push(Obj::new().with("path", path).with("reason", "no-key"));
            continue;
        };
        if objects.head(&key)?.is_none() {
            gaps.push(
                Obj::new()
                    .with("r2Key", key)
                    .with("reason", "catalog-local"),
            );
        }
    }
    walk_uncatalogued(root, &known, objects, older_than, now_at, &mut gaps)?;
    Ok(Obj::new()
        .with("at", now())
        .with("ok", gaps.is_empty())
        .with("gaps", gaps.len())
        .with_rows("files", gaps))
}

fn walk_uncatalogued(
    root: &Path,
    known: &HashSet<String>,
    objects: &dyn ObjectStore,
    older_than: Duration,
    now_at: chrono::DateTime<chrono::Utc>,
    gaps: &mut Vec<Obj>,
) -> Result<(), StoreError> {
    fn visit(
        root: &Path,
        dir: &Path,
        known: &HashSet<String>,
        objects: &dyn ObjectStore,
        older_than: Duration,
        now_at: chrono::DateTime<chrono::Utc>,
        gaps: &mut Vec<Obj>,
    ) -> Result<(), StoreError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            }
            let path = entry.path();
            if file_type.is_dir() {
                visit(root, &path, known, objects, older_than, now_at, gaps)?;
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".parquet") {
                continue;
            }
            let rel = path_rel(root, &path.to_string_lossy())?;
            if known.contains(&rel) {
                continue;
            }
            let modified = entry
                .metadata()?
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let at = chrono::DateTime::<chrono::Utc>::from(modified);
            if !is_old(at, now_at, older_than) {
                continue;
            }
            let key = format!("phoenix_raw/{rel}");
            if objects.head(&key)?.is_none() {
                gaps.push(Obj::new().with("r2Key", key).with("reason", "uncatalogued"));
            }
        }
        Ok(())
    }
    visit(root, root, known, objects, older_than, now_at, gaps)
}

fn path_rel(root: &Path, path: &str) -> Result<String, StoreError> {
    let full = Path::new(path);
    let rel = if full.is_absolute() {
        full.strip_prefix(root)
            .map_err(|_| StoreError::Check("path is outside the raw root".into()))?
            .to_path_buf()
    } else {
        full.to_path_buf()
    };
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

fn parse_time(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.with_timezone(&chrono::Utc))
}

/// Exit code for `maintain backup-gap`: `0` when every old local file is in `objects`, `2` when
/// one is not. The report JSON is written to stdout.
///
/// # Errors
///
/// The catalog cannot be read, or the object store listing fails.
pub fn backup_gap_exit(
    root: &Path,
    objects: &dyn ObjectStore,
    older_than: Duration,
    now_at: chrono::DateTime<chrono::Utc>,
) -> Result<i32, StoreError> {
    let report = backup_gap(root, objects, older_than, now_at)?;
    println!("{}", report.to_json());
    let ok = report.get("ok") == Some(&Value::Bool(true));
    Ok(if ok { 0 } else { 2 })
}

fn is_old(
    at: chrono::DateTime<chrono::Utc>,
    now_at: chrono::DateTime<chrono::Utc>,
    older_than: Duration,
) -> bool {
    now_at
        .signed_duration_since(at)
        .to_std()
        .is_ok_and(|age| age >= older_than)
}

/// Parse `sealed`, `N`, or `N-M` (inclusive). A range wider than 10_000 epochs is refused.
pub fn parse_epochs(spec: &str, sealed: &[i64]) -> Result<Vec<i64>, StoreError> {
    if spec == "sealed" {
        return Ok(sealed.to_vec());
    }
    if let Some((start, end)) = spec.split_once('-') {
        let start: i64 = start
            .parse()
            .map_err(|_| StoreError::Check("invalid epoch range".into()))?;
        let end: i64 = end
            .parse()
            .map_err(|_| StoreError::Check("invalid epoch range".into()))?;
        if start < 0 || start > end || end - start > 10_000 {
            return Err(StoreError::Check("invalid epoch range".into()));
        }
        return Ok((start..=end).collect());
    }
    let epoch: i64 = spec
        .parse()
        .map_err(|_| StoreError::Check("invalid epoch".into()))?;
    if epoch < 0 {
        return Err(StoreError::Check("invalid epoch".into()));
    }
    Ok(vec![epoch])
}

/// `solos-data maintain ...`.
pub fn command(args: &[String]) -> i32 {
    match command_result(args) {
        Ok(code) => code,
        Err(error) => {
            log(
                "maintain_fatal",
                Obj::new().with("error", crate::jsonout::safe_error(&error.to_string())),
            );
            1
        }
    }
}

fn command_result(args: &[String]) -> Result<i32, StoreError> {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if command == "help" || command == "--help" {
        println!("{}", help_text().to_json());
        return Ok(0);
    }
    let config = crate::collector::config::load_config(flag(args, "--config"))?;
    match command {
        "seal" => {
            let lag = match flag(args, "--lag") {
                Some(value) => value
                    .parse::<i64>()
                    .map_err(|_| StoreError::Check("invalid --lag".into()))?,
                None => config.seal_lag_epochs,
            };
            let decoded = decoded_root(args)?;
            let mut store = open_raw(&config)?;
            let report = seal_report(&mut store, lag, &decoded)?;
            store.close()?;
            println!("{}", report.to_json());
            Ok(0)
        }
        "offload" => {
            let spec = flag(args, "--epochs").ok_or_else(|| {
                StoreError::Check("offload requires --epochs <n|n-m|sealed>".into())
            })?;
            let objects = super::object_store::RcloneStore::open_default()?;
            let mut store = open_raw(&config)?;
            let sealed = sealed_list(&store)?;
            let epochs = parse_epochs(spec, &sealed)?;
            let drop_local = has(args, "--drop-local");
            let decoded = if drop_local {
                Some(decoded_root(args)?)
            } else {
                None
            };
            let report = offload(
                &mut store,
                &objects,
                &OffloadRequest {
                    epochs: &epochs,
                    verify: has(args, "--verify"),
                    drop_local,
                    decoded_root: decoded.as_deref(),
                },
            )?;
            store.close()?;
            println!("{}", report.to_json());
            Ok(0)
        }
        "restore" => {
            let spec = flag(args, "--epochs")
                .ok_or_else(|| StoreError::Check("restore requires --epochs <n|n-m>".into()))?;
            let objects = super::object_store::RcloneStore::open_default()?;
            let mut store = open_raw(&config)?;
            let sealed = sealed_list(&store)?;
            let epochs = parse_epochs(spec, &sealed)?;
            let report = restore(&mut store, &objects, &epochs)?;
            store.close()?;
            println!("{}", report.to_json());
            Ok(0)
        }
        "backup-gap" => {
            let hours = flag(args, "--older-than").map_or(Ok(30u64), str::parse::<u64>);
            let Ok(hours) = hours else {
                return Err(StoreError::Check("invalid --older-than".into()));
            };
            let objects = super::object_store::RcloneStore::open_default()?;
            backup_gap_exit(
                &config.data_dir,
                &objects,
                Duration::from_secs(hours.saturating_mul(3600)),
                chrono::Utc::now(),
            )
        }
        "verify-storage" => {
            let mut store = open_raw(&config)?;
            let report = if has(args, "--remote") {
                let objects = super::object_store::RcloneStore::open_default()?;
                verify_files_with(
                    &mut store,
                    Some(&mut |sample: RemoteSample| {
                        confirm_remote(&objects, &sample.key, sample.size, &sample.sha256)
                    }),
                )?
            } else {
                super::writer::verify_files(&mut store)?
            };
            store.close()?;
            println!("{}", report.to_json());
            Ok(0)
        }
        _ => Err(StoreError::Check("Unknown maintain command".into())),
    }
}

fn seal_report(store: &mut Store, lag: i64, decoded: &Path) -> Result<Obj, StoreError> {
    super::seal::seal_epochs(
        store,
        &super::seal::SealRequest {
            lag,
            decoded_root: decoded,
        },
    )
}

fn sealed_list(store: &Store) -> Result<Vec<i64>, StoreError> {
    let mut ids: Vec<i64> = sealed_epoch_ids(store)?.into_iter().collect();
    ids.sort_unstable();
    Ok(ids)
}

fn open_raw(config: &crate::collector::config::Config) -> Result<Store, StoreError> {
    Store::open(&config.data_dir, crate::collector::schema::SCHEMA).map_err(|error| match error {
        StoreError::AlreadyOwned { .. } => StoreError::Check(
            "collector checkpoint is owned; stop solos-data-phoenix.service first".into(),
        ),
        other => other,
    })
}

fn decoded_root(args: &[String]) -> Result<PathBuf, StoreError> {
    if let Some(path) = flag(args, "--decoded-root") {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("SOLOS_DATA_DECODED_DIR")
        && !path.is_empty()
    {
        return Ok(PathBuf::from(path));
    }
    Err(StoreError::Check(
        "seal and drop-local require --decoded-root or SOLOS_DATA_DECODED_DIR".into(),
    ))
}

fn help_text() -> Obj {
    Obj::new().with_obj(
        "commands",
        Obj::new()
            .with(
                "seal",
                "Mark settled epochs sealed after coverage, hash, row count and decoder consumption checks. Stop the collector and the decoder first. --lag N (default sealLagEpochs, 2). --decoded-root PATH",
            )
            .with(
                "offload",
                "--epochs <n|n-m|sealed> --verify [--drop-local]. Confirms each R2 object's size and sha256, records location both, then deletes the local file only with --drop-local and only when the epoch is sealed. Stop the collector. --drop-local also holds the decoder checkpoint for the whole run. Idempotent and resumable.",
            )
            .with(
                "restore",
                "--epochs <n|n-m|sealed>. Download the epoch and require each sha256 to match the catalog. Stop the collector.",
            )
            .with(
                "backup-gap",
                "--older-than HOURS (default 30). Exit 2 when a local file older than that has no R2 object. Does not open DuckDB, so it can run with the collector up.",
            )
            .with(
                "verify-storage",
                "Hash local active files. r2-only files are skipped. --remote checks one r2-only file per epoch by streaming its sha256.",
            ),
    )
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .map(String::as_str)
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

/// Stop between the durable `both` record and the local delete. Tests only.
#[cfg(feature = "failpoints")]
pub fn halt_offload_before_drop(halt: bool) {
    HALT_BEFORE_DROP.with(|slot| slot.set(halt));
}

#[cfg(feature = "failpoints")]
std::thread_local! {
    static HALT_BEFORE_DROP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn halt_before_drop() -> bool {
    #[cfg(feature = "failpoints")]
    {
        HALT_BEFORE_DROP.with(std::cell::Cell::get)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        false
    }
}
