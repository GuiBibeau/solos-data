//! Sealed-epoch offload. Each test names the mutation that must turn it red.

use serde_json::{Value, json};
use solos_data::catalog::read_catalog;
use solos_data::collector::compactor::compact;
use solos_data::collector::object_store::MemoryStore;
use solos_data::collector::offload::{
    OffloadRequest, backup_gap, backup_gap_exit, confirm_remote, halt_offload_before_drop,
    is_r2_file, object_key, offload, restore,
};
use solos_data::collector::reader::query_dataset;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::seal::{EPOCH_SLOTS, SealRequest, seal_epochs};
use solos_data::collector::writer::{publish_range, verify_files, verify_files_with};
use solos_data::decoder::schema::schema_static;
use solos_data::decoder::source::pending_sources;
use solos_data::fsutil::{FsyncStep, clear_fsync_failure, inject_fsync_failure};
use solos_data::fsutil::{sha256_hex, write_durable};
use solos_data::store::Store;
use std::path::{Path, PathBuf};
use std::process::Command;

struct Temp(PathBuf);

impl Temp {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "solos-offload-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn publish_slot(store: &mut Store, root: &Path, slot: i64, signature: &str) {
    store
        .exec_batch(&format!(
            "INSERT INTO transactions VALUES ('{signature}', {slot}, {slot}, NULL, NULL, 'null', 5000, 10, 'AA==', '{{}}', '{{}}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)"
        ))
        .unwrap();
    publish_range(store, root, slot, slot, None).unwrap();
}

fn cover(store: &mut Store, epoch: i64) {
    let start = epoch * EPOCH_SLOTS;
    let end = start + EPOCH_SLOTS - 1;
    store
        .exec(
            "INSERT OR IGNORE INTO published_ranges VALUES (?, ?)",
            &[&start, &end],
        )
        .unwrap();
}

fn consume(decoded: &Path, store: &Store, epochs: &[i64]) {
    let mut dec = Store::open(decoded, schema_static()).unwrap();
    for file in store
        .rows(
            "SELECT sha256, path, row_count, epoch, table_name FROM files WHERE status='active'",
            &[],
        )
        .unwrap()
    {
        if file.str("table_name") != Some("transactions") {
            continue;
        }
        let epoch = file.int("epoch").unwrap();
        if !epochs.contains(&epoch) {
            continue;
        }
        let count = file.int("row_count").unwrap();
        dec.exec(
            "INSERT OR REPLACE INTO sources VALUES (?, ?, ?)",
            &[
                &file.str("sha256").unwrap(),
                &file.str("path").unwrap(),
                &count,
            ],
        )
        .unwrap();
    }
    dec.close().unwrap();
}

fn must_err<T, E: std::fmt::Display>(result: Result<T, E>) -> String {
    match result {
        Err(error) => error.to_string(),
        Ok(_) => panic!("expected an error"),
    }
}

fn sealed_epochs(report: &solos_data::jsonout::Obj) -> Vec<i64> {
    report
        .get("sealedEpochs")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .filter_map(|value| value.as_i64())
        .collect()
}

fn parquet_names(root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    fn visit(dir: &Path, root: &Path, names: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, root, names);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                names.push(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    visit(root, root, &mut names);
    names.sort();
    names
}

fn tx_path(store: &Store, epoch: i64) -> String {
    store
        .rows(
            "SELECT path FROM files WHERE status='active' AND table_name='transactions' AND epoch=? ORDER BY path",
            &[&epoch],
        )
        .unwrap()[0]
        .str("path")
        .unwrap()
        .to_owned()
}

fn stage_remote(scene: &FileScene, bytes: &[u8]) {
    scene.objects.insert(
        scene.key.clone(),
        bytes.to_vec(),
        Some(scene.sha.clone()),
        None,
    );
}

fn parquet_count(root: &Path) -> usize {
    let mut count = 0;
    fn visit(dir: &Path, count: &mut usize) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                visit(&path, count);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                *count += 1;
            }
        }
    }
    visit(root, &mut count);
    count
}

/// Mutation: remove the `epoch >= cutoff` settled check in `seal_epochs`.
#[test]
fn the_current_epoch_is_not_sealed() {
    let tmp = Temp::new("seal-current");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "old");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS, "live");
    cover(&mut store, 0);
    cover(&mut store, 1);
    let decoded = tmp.0.join("decoded");
    consume(&decoded, &store, &[0, 1]);
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), vec![0]);
    store.close().unwrap();
}

/// Mutation: remove the published-range coverage requirement in `seal_epochs`.
#[test]
fn an_uncovered_epoch_is_not_sealed() {
    let tmp = Temp::new("seal-gap");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "gap");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS, "full");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS * 2, "live");
    cover(&mut store, 1);
    cover(&mut store, 2);
    let decoded = tmp.0.join("decoded");
    consume(&decoded, &store, &[0, 1, 2]);
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), vec![1]);
    store.close().unwrap();
}

/// Mutation: remove the decoder consumption check in `seal_epochs`.
#[test]
fn an_undecoded_epoch_is_not_sealed() {
    let tmp = Temp::new("seal-decoded");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "partial");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS, "done");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS * 2, "live");
    cover(&mut store, 0);
    cover(&mut store, 1);
    cover(&mut store, 2);
    let decoded = tmp.0.join("decoded");
    consume(&decoded, &store, &[1, 2]);
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), vec![1]);
    store.close().unwrap();
}

/// Mutation: remove the sealed-epoch refusal in `publish_range`.
#[test]
fn publish_refuses_a_sealed_epoch() {
    let tmp = Temp::new("seal-write");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "old");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS, "live");
    cover(&mut store, 0);
    cover(&mut store, 1);
    let decoded = tmp.0.join("decoded");
    consume(&decoded, &store, &[0, 1]);
    seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    let before = parquet_count(&tmp.0);
    store
        .exec_batch(
            "INSERT INTO transactions VALUES ('extra', 1, 1, NULL, NULL, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)",
        )
        .unwrap();
    let err = publish_range(&mut store, &tmp.0, 1, 1, None).unwrap_err();
    assert!(err.to_string().contains("sealed"), "{err}");
    assert_eq!(parquet_count(&tmp.0), before);
    store.close().unwrap();
}

struct FileScene {
    store: Option<Store>,
    tmp: Temp,
    path: PathBuf,
    key: String,
    sha: String,
    decoded: PathBuf,
    objects: MemoryStore,
}

impl FileScene {
    fn new(bytes: &[u8]) -> Self {
        let tmp = Temp::new("file");
        let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
        let dir = tmp.0.join("staging/transactions/epoch=4");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("0-10-a.parquet");
        std::fs::write(&path, bytes).unwrap();
        let sha = sha256_hex(bytes);
        let path_text = path.to_string_lossy().into_owned();
        let len = i64::try_from(bytes.len()).unwrap();
        let epoch = 4i64;
        store
            .exec(
                "INSERT INTO files (path, table_name, epoch, row_count, sha256, created_at, status, byte_size, location) VALUES (?, 'transactions', ?, 1, ?, '2026-01-01T00:00:00.000Z', 'active', ?, 'local')",
                &[&path_text, &epoch, &sha, &len],
            )
            .unwrap();
        store
            .exec(
                "INSERT INTO sealed_epochs VALUES (?, 't', ?::JSON)",
                &[&epoch, &"{\"settled\":true}"],
            )
            .unwrap();
        let key = object_key(&tmp.0, &path_text).unwrap();
        let decoded = tmp.0.join("decoded-hold");
        Self {
            decoded,
            path,
            key,
            sha,
            objects: MemoryStore::new(),
            store: Some(store),
            tmp,
        }
    }

    fn offload(
        &mut self,
        drop_local: bool,
    ) -> Result<solos_data::jsonout::Obj, solos_data::store::StoreError> {
        let epoch = [4i64];
        offload(
            self.store.as_mut().unwrap(),
            &self.objects,
            &OffloadRequest {
                epochs: &epoch,
                verify: true,
                drop_local,
                decoded_root: Some(&self.decoded),
            },
        )
    }

    fn close(mut self) {
        self.store.take().unwrap().close().unwrap();
    }
}

fn catalog_location(root: &Path) -> String {
    let value: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json")).unwrap()).unwrap();
    value["files"][0]["location"].as_str().unwrap().to_owned()
}

/// Mutation: delete the R2 existence check before `unlink`.
#[test]
fn no_drop_without_a_verified_r2_copy() {
    let bytes = b"raw-bytes-for-drop";
    let mut scene = FileScene::new(bytes);
    let err = must_err(scene.offload(true));
    assert!(err.contains("missing"), "{err}");
    assert!(scene.path.exists());
    let key = scene.key.clone();
    let sha = scene.sha.clone();
    scene.objects.insert(key, bytes.to_vec(), Some(sha), None);
    scene.offload(true).unwrap();
    assert!(!scene.path.exists());
    assert_eq!(catalog_location(&scene.tmp.0), "r2");
    scene.close();
}

/// Mutation: accept an R2 object whose size differs from the local file.
#[test]
fn a_size_mismatch_blocks_the_drop() {
    let bytes = b"raw-bytes-for-size";
    let mut scene = FileScene::new(bytes);
    let key = scene.key.clone();
    let sha = scene.sha.clone();
    scene.objects.insert(
        key,
        bytes.to_vec(),
        Some(sha),
        Some(u64::try_from(bytes.len()).unwrap() + 1),
    );
    let err = must_err(scene.offload(true));
    assert!(err.contains("size"), "{err}");
    assert!(scene.path.exists());
    assert!(!scene.tmp.0.join("catalog.json").exists() || catalog_location(&scene.tmp.0) != "r2");
    scene.close();
}

/// Mutation: accept a stored R2 checksum that differs from the catalog hash, or accept
/// streamed bytes that differ.
#[test]
fn a_sha_mismatch_blocks_the_drop() {
    let bytes = b"raw-bytes-for-sha";
    let mut stored = FileScene::new(bytes);
    let writes_before = stored.objects.write_to_calls();
    let key = stored.key.clone();
    stored
        .objects
        .insert(key, bytes.to_vec(), Some("0".repeat(64)), None);
    let err = must_err(stored.offload(true));
    assert!(err.contains("sha256"), "{err}");
    assert!(stored.path.exists());
    assert_eq!(stored.objects.write_to_calls(), writes_before);
    stored.close();

    let mut streamed = FileScene::new(bytes);
    let mut bad = bytes.to_vec();
    bad[0] ^= 0xff;
    let key = streamed.key.clone();
    streamed.objects.insert(key, bad, None, None);
    let err = must_err(streamed.offload(true));
    assert!(err.contains("sha256"), "{err}");
    assert!(streamed.path.exists());
    assert!(streamed.objects.write_to_calls() >= 1);
    streamed.close();
}

/// Mutation: unlink the local file before the durable location=both catalog write.
#[test]
fn a_crash_between_verify_and_delete_leaves_a_consistent_catalog() {
    let bytes = b"raw-bytes-for-crash";
    let mut scene = FileScene::new(bytes);
    let key = scene.key.clone();
    let sha = scene.sha.clone();
    scene.objects.insert(key, bytes.to_vec(), Some(sha), None);
    halt_offload_before_drop(true);
    let err = must_err(scene.offload(true));
    halt_offload_before_drop(false);
    assert!(err.contains("interrupted"), "{err}");
    assert!(scene.path.exists(), "local file must still be there");
    assert_eq!(catalog_location(&scene.tmp.0), "both");
    scene.offload(true).unwrap();
    assert!(!scene.path.exists());
    assert_eq!(catalog_location(&scene.tmp.0), "r2");
    let again = scene.offload(true).unwrap();
    assert_eq!(again.int("alreadyR2"), Some(1));
    assert_eq!(catalog_location(&scene.tmp.0), "r2");
    scene.close();
}

/// Mutation: remove the `location == "r2"` skip in `verify_files`, or skip the remote sample
/// hash comparison.
#[test]
fn verify_skips_r2_only_files_and_a_remote_sample_can_reject_them() {
    let tmp = Temp::new("verify-r2");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    let dir = tmp.0.join("staging/transactions/epoch=2");
    std::fs::create_dir_all(&dir).unwrap();
    let missing = dir.join("0-10-cold.parquet");
    let missing_text = missing.to_string_lossy().into_owned();
    let cold_sha = sha256_hex(b"cold-bytes");
    let cold_len = i64::try_from(b"cold-bytes".len()).unwrap();
    let key = object_key(&tmp.0, &missing_text).unwrap();
    store
        .exec(
            "INSERT INTO files (path, table_name, epoch, row_count, sha256, created_at, status, byte_size, location, r2_key) VALUES (?, 'transactions', 2, 1, ?, 't', 'active', ?, 'r2', ?)",
            &[&missing_text, &cold_sha, &cold_len, &key],
        )
        .unwrap();
    let report = verify_files(&mut store).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(true)));
    assert_eq!(report.int("skippedR2"), Some(1));
    assert!(!missing.exists());
    let objects = MemoryStore::new();
    objects.insert(key, b"cold-bytes".to_vec(), Some("f".repeat(64)), None);
    let err = must_err(verify_files_with(
        &mut store,
        Some(&mut |sample| confirm_remote(&objects, &sample.key, sample.size, &sample.sha256)),
    ));
    assert!(err.contains("sha256"), "{err}");
    store.close().unwrap();
}

/// Mutation: skip the sha256 comparison after the restore download.
#[test]
fn restore_round_trip_checks_sha256() {
    let good = b"round-trip-bytes";
    let mut scene = FileScene::new(good);
    std::fs::remove_file(&scene.path).unwrap();
    let key = scene.key.clone();
    scene
        .store
        .as_mut()
        .unwrap()
        .exec(
            "UPDATE files SET location='r2', r2_key=? WHERE epoch=4",
            &[&key],
        )
        .unwrap();
    let mut bad = good.to_vec();
    bad[0] ^= 0xff;
    scene.objects.insert(key.clone(), bad, None, None);
    let err = must_err(restore(scene.store.as_mut().unwrap(), &scene.objects, &[4]));
    assert!(err.contains("sha256"), "{err}");
    assert!(!scene.path.exists());
    let location = scene
        .store
        .as_mut()
        .unwrap()
        .rows("SELECT location FROM files", &[])
        .unwrap()[0]
        .str("location")
        .unwrap()
        .to_owned();
    assert_eq!(location, "r2");
    scene.objects.insert(key, good.to_vec(), None, None);
    restore(scene.store.as_mut().unwrap(), &scene.objects, &[4]).unwrap();
    assert_eq!(std::fs::read(&scene.path).unwrap(), good);
    assert_eq!(sha256_hex(&std::fs::read(&scene.path).unwrap()), scene.sha);
    assert_eq!(catalog_location(&scene.tmp.0), "both");
    scene.close();
}

/// Mutation: drop the consumed-r2 `continue`, or `continue` past an unconsumed r2-only file.
#[test]
fn the_decoder_skips_a_consumed_r2_file_and_refuses_an_unconsumed_one() {
    let consumed_root = Temp::new("decode-consumed");
    write_durable(
        &consumed_root.0.join("catalog.json"),
        br#"{"at":"t","files":[{"path":"staging/transactions/epoch=1/0-10-a.parquet","table_name":"transactions","row_count":2,"sha256":"h","location":"r2","r2_key":"phoenix_raw/staging/transactions/epoch=1/0-10-a.parquet"}]}"#,
    )
    .unwrap();
    let mut decoded = Store::open(&consumed_root.0.join("decoded"), schema_static()).unwrap();
    decoded
        .exec(
            "INSERT INTO sources VALUES ('h', 'staging/transactions/epoch=1/0-10-a.parquet', 2)",
            &[],
        )
        .unwrap();
    let pending = pending_sources(&mut decoded, &consumed_root.0).unwrap();
    assert!(
        pending.is_empty(),
        "consumed r2 file was offered to the decoder"
    );
    decoded.close().unwrap();

    let fresh = Temp::new("decode-fresh");
    write_durable(
        &fresh.0.join("catalog.json"),
        br#"{"at":"t","files":[{"path":"staging/transactions/epoch=1/0-10-a.parquet","table_name":"transactions","row_count":2,"sha256":"h","location":"r2","r2_key":"phoenix_raw/staging/transactions/epoch=1/0-10-a.parquet"}]}"#,
    )
    .unwrap();
    let mut decoded = Store::open(&fresh.0.join("decoded"), schema_static()).unwrap();
    let err = must_err(pending_sources(&mut decoded, &fresh.0));
    assert!(err.contains("restore"), "{err}");
    decoded.close().unwrap();
}

/// Mutation: remove `refuse_r2_only` from the raw query path.
#[test]
fn a_query_refuses_an_r2_only_file() {
    let tmp = Temp::new("query-r2");
    std::fs::write(
        tmp.0.join("catalog.json"),
        r#"{"at":"t","files":[{"path":"staging/transactions/epoch=1/0-10-a.parquet","table_name":"transactions","row_count":1,"sha256":"h","created_at":"t","location":"r2","r2_key":"phoenix_raw/a.parquet"}],"coverage":[]}"#,
    )
    .unwrap();
    let err = must_err(query_dataset(
        &tmp.0,
        "SELECT count(*) AS n FROM transactions",
    ));
    assert!(err.contains("restore"), "{err}");
    assert!(is_r2_file(
        &json!({"location": "r2", "r2_key": "phoenix_raw/a.parquet"})
    ));
}

/// Mutation: treat a missing R2 object as present, or drop the age threshold.
#[test]
fn backup_gap_alerts_when_an_old_local_file_is_not_in_r2() {
    let tmp = Temp::new("gap");
    let old_rel = "staging/transactions/epoch=1/old.parquet";
    let young_rel = "staging/transactions/epoch=1/young.parquet";
    let loose_old = "staging/transactions/epoch=2/loose.parquet";
    let loose_young = "staging/transactions/epoch=2/fresh.parquet";
    for rel in [old_rel, young_rel, loose_old, loose_young] {
        let path = tmp.0.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"x").unwrap();
    }
    Command::new("touch")
        .args([
            "-d",
            "2020-01-01 00:00:00",
            &tmp.0.join(loose_old).to_string_lossy(),
        ])
        .status()
        .unwrap();
    Command::new("touch")
        .args([
            "-d",
            "2026-11-01 12:00:00",
            &tmp.0.join(loose_young).to_string_lossy(),
        ])
        .status()
        .unwrap();
    let old_key = format!("phoenix_raw/{old_rel}");
    let young_key = format!("phoenix_raw/{young_rel}");
    let present_key = "phoenix_raw/staging/transactions/epoch=1/present.parquet";
    let catalog = json!({
        "files": [
            {"path": tmp.0.join(old_rel).to_string_lossy(), "table_name": "transactions", "created_at": "2020-01-01T00:00:00Z", "location": "local", "r2_key": old_key},
            {"path": tmp.0.join(young_rel).to_string_lossy(), "table_name": "transactions", "created_at": "2026-11-01T22:00:00Z", "location": "local", "r2_key": young_key},
            {"path": tmp.0.join("staging/transactions/epoch=1/present.parquet").to_string_lossy(), "table_name": "transactions", "created_at": "2020-01-01T00:00:00Z", "location": "both", "r2_key": present_key}
        ]
    });
    std::fs::write(tmp.0.join("catalog.json"), catalog.to_string()).unwrap();
    std::fs::write(
        tmp.0.join("staging/transactions/epoch=1/present.parquet"),
        b"y",
    )
    .unwrap();
    let objects = MemoryStore::new();
    objects.insert(present_key, b"y".to_vec(), None, None);
    let now = chrono::DateTime::parse_from_rfc3339("2026-11-01T23:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let report = backup_gap(
        &tmp.0,
        &objects,
        std::time::Duration::from_secs(30 * 3600),
        now,
    )
    .unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
    let keys: Vec<String> = report
        .rows("files")
        .unwrap()
        .iter()
        .map(|row| row.str("r2Key").unwrap().to_owned())
        .collect();
    assert!(keys.iter().any(|key| key == &old_key), "{keys:?}");
    assert!(
        keys.iter().any(|key| key.ends_with("loose.parquet")),
        "{keys:?}"
    );
    assert!(!keys.iter().any(|key| key == &young_key), "{keys:?}");
    assert!(
        !keys.iter().any(|key| key.ends_with("fresh.parquet")),
        "{keys:?}"
    );
    assert!(!keys.iter().any(|key| key == present_key), "{keys:?}");
}

#[test]
fn read_catalog_still_accepts_a_file_without_a_location() {
    let tmp = Temp::new("catalog-old");
    std::fs::write(
        tmp.0.join("catalog.json"),
        r#"{"at":"t","files":[{"path":"a","table_name":"transactions","row_count":"2","sha256":"h"}]}"#,
    )
    .unwrap();
    let catalog = read_catalog(&tmp.0).unwrap();
    assert_eq!(catalog.files[0].location, "local");
    assert!(catalog.files[0].r2_key.is_none());
}

struct ClearFsync;

impl Drop for ClearFsync {
    fn drop(&mut self) {
        clear_fsync_failure();
    }
}

fn publish_covered(store: &mut Store, root: &Path, epochs: &[i64]) -> PathBuf {
    for epoch in epochs {
        publish_slot(store, root, epoch * EPOCH_SLOTS, &format!("epoch-{epoch}"));
        cover(store, *epoch);
    }
    let decoded = root.join("decoded");
    consume(&decoded, store, epochs);
    decoded
}

/// Mutation: ignore `lag` in the seal cutoff (`current.saturating_sub(lag)`).
#[test]
fn lag_two_seals_only_the_oldest_of_four_epochs() {
    let tmp = Temp::new("lag-two");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    let decoded = publish_covered(&mut store, &tmp.0, &[0, 1, 2, 3]);
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 2,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), vec![0]);
    for epoch in [1, 2, 3] {
        let reason = report
            .rows("refused")
            .unwrap()
            .iter()
            .find(|row| row.int("epoch") == Some(epoch))
            .and_then(|row| row.str("reason"));
        assert_eq!(reason, Some("not settled"), "epoch {epoch}");
    }
    store.close().unwrap();
}

/// Mutation: remove the `lag < 0` refusal in `seal_epochs`.
#[test]
fn a_negative_lag_is_refused() {
    let tmp = Temp::new("lag-negative");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    let err = must_err(seal_epochs(
        &mut store,
        &SealRequest {
            lag: -1,
            decoded_root: &tmp.0.join("decoded"),
        },
    ));
    assert!(err.contains("lag"), "{err}");
    store.close().unwrap();
}

/// Mutation: remove the SHA-256 comparison in `seal_epochs`.
#[test]
fn a_flipped_byte_is_not_sealed() {
    let tmp = Temp::new("seal-hash");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    let decoded = publish_covered(&mut store, &tmp.0, &[0, 1]);
    let path = tx_path(&store, 0);
    let mut bytes = std::fs::read(&path).unwrap();
    let index = bytes.len() / 2;
    bytes[index] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), Vec::<i64>::new());
    let reason = report
        .rows("refused")
        .unwrap()
        .iter()
        .find(|row| row.int("epoch") == Some(0))
        .and_then(|row| row.str("reason"));
    assert_eq!(reason, Some("hash mismatch"));
    store.close().unwrap();
}

/// Mutation: remove the Parquet row-count comparison in `seal_epochs`.
#[test]
fn a_bumped_row_count_is_not_sealed() {
    let tmp = Temp::new("seal-count");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "counted");
    publish_slot(&mut store, &tmp.0, EPOCH_SLOTS, "live");
    cover(&mut store, 0);
    cover(&mut store, 1);
    // The decoder offset matches the bumped catalog count, so only the parquet count can refuse.
    store
        .exec(
            "UPDATE files SET row_count = row_count + 1 WHERE epoch = 0 AND table_name = 'transactions'",
            &[],
        )
        .unwrap();
    let decoded = tmp.0.join("decoded");
    consume(&decoded, &store, &[0, 1]);
    let report = seal_epochs(
        &mut store,
        &SealRequest {
            lag: 0,
            decoded_root: &decoded,
        },
    )
    .unwrap();
    assert_eq!(sealed_epochs(&report), Vec::<i64>::new());
    let reason = report
        .rows("refused")
        .unwrap()
        .iter()
        .find(|row| row.int("epoch") == Some(0))
        .and_then(|row| row.str("reason"));
    assert_eq!(reason, Some("count mismatch"));
    store.close().unwrap();
}

/// Mutation: remove the sealed-epoch `continue` in `compact`.
#[test]
fn compact_leaves_a_sealed_epoch_unchanged() {
    let tmp = Temp::new("compact-sealed");
    let mut store = Store::open(&tmp.0, SCHEMA).unwrap();
    publish_slot(&mut store, &tmp.0, 0, "base");
    let original = tx_path(&store, 0);
    let count = store
        .rows(
            "SELECT row_count, sha256 FROM files WHERE path=?",
            &[&original],
        )
        .unwrap();
    let rows = count[0].int("row_count").unwrap();
    let sha = count[0].str("sha256").unwrap().to_owned();
    let bytes = i64::try_from(std::fs::metadata(&original).unwrap().len()).unwrap();
    let dir = Path::new(&original).parent().unwrap();
    for i in 1..12 {
        let path = dir.join(format!("copy-{i}.parquet"));
        std::fs::copy(&original, &path).unwrap();
        let text = path.to_string_lossy().into_owned();
        let created = format!("2026-06-01T00:{i:02}:00.000Z");
        store
            .exec(
                "INSERT INTO files (path, table_name, epoch, row_count, sha256, created_at, status, byte_size, location) VALUES (?, 'transactions', 0, ?, ?, ?, 'active', ?, 'local')",
                &[&text, &rows, &sha, &created, &bytes],
            )
            .unwrap();
    }
    store
        .exec(
            "INSERT INTO sealed_epochs VALUES (0, 't', ?::JSON)",
            &[&"{}"],
        )
        .unwrap();
    let before = parquet_names(&tmp.0);
    assert_eq!(before.len(), 12);
    compact(&mut store, &tmp.0).unwrap();
    assert_eq!(parquet_names(&tmp.0), before);
    let active = store
        .rows("SELECT count(*) AS n FROM files WHERE status='active'", &[])
        .unwrap();
    assert_eq!(active[0].int("n"), Some(12));
    store.close().unwrap();
}

/// Mutation: remove the `!request.verify` refusal in `offload`.
#[test]
fn offload_refuses_to_run_without_verify() {
    let bytes = b"needs-verify";
    let mut scene = FileScene::new(bytes);
    stage_remote(&scene, bytes);
    let epoch = [4i64];
    let err = must_err(offload(
        scene.store.as_mut().unwrap(),
        &scene.objects,
        &OffloadRequest {
            epochs: &epoch,
            verify: false,
            drop_local: true,
            decoded_root: Some(&scene.decoded),
        },
    ));
    assert!(err.contains("verify"), "{err}");
    assert!(scene.path.exists());
    scene.close();
}

/// Mutation: remove the `decoded_root.is_none()` refusal in `offload`.
#[test]
fn drop_local_requires_a_decoded_root() {
    let bytes = b"needs-decoded-root";
    let mut scene = FileScene::new(bytes);
    stage_remote(&scene, bytes);
    let epoch = [4i64];
    let err = must_err(offload(
        scene.store.as_mut().unwrap(),
        &scene.objects,
        &OffloadRequest {
            epochs: &epoch,
            verify: true,
            drop_local: true,
            decoded_root: None,
        },
    ));
    assert!(err.contains("decoded"), "{err}");
    assert!(scene.path.exists());
    scene.close();
}

/// Mutation: skip opening the decoder checkpoint for the whole `--drop-local` run.
#[test]
fn drop_local_fails_while_the_decoder_checkpoint_is_held() {
    let bytes = b"decoder-held";
    let mut scene = FileScene::new(bytes);
    stage_remote(&scene, bytes);
    let held = Store::open(&scene.decoded, schema_static()).unwrap();
    let err = must_err(scene.offload(true));
    assert!(err.contains("owned"), "{err}");
    assert!(
        scene.path.exists(),
        "drop ran while the decoder checkpoint was open"
    );
    held.close().unwrap();
    scene.close();
}

/// Mutation: remove the `!sealed.contains` skip before the local unlink.
#[test]
fn an_unsealed_epoch_keeps_its_local_file() {
    let bytes = b"unsealed-stays-local";
    let mut scene = FileScene::new(bytes);
    scene
        .store
        .as_mut()
        .unwrap()
        .exec("DELETE FROM sealed_epochs", &[])
        .unwrap();
    stage_remote(&scene, bytes);
    let report = scene.offload(true).unwrap();
    assert!(scene.path.exists());
    assert_eq!(report.int("dropped"), Some(0));
    assert_eq!(report.int("keptLocal"), Some(1));
    assert_eq!(catalog_location(&scene.tmp.0), "both");
    scene.close();
}

/// Mutation: ignore the `write_catalog` error after recording `both` (`let _ = write_catalog`).
#[test]
fn a_failed_catalog_fsync_keeps_the_local_file() {
    let bytes = b"fsync-keeps-the-local-file";
    let mut scene = FileScene::new(bytes);
    stage_remote(&scene, bytes);
    let _guard = ClearFsync;
    inject_fsync_failure(FsyncStep::TempFile);
    let err = must_err(scene.offload(true));
    assert!(err.contains("fsync"), "{err}");
    assert!(
        scene.path.exists(),
        "local file was deleted after a failed catalog fsync"
    );
    scene.close();
}

/// Mutation: remove the `part == ".."` rejection in `object_key`.
#[test]
fn object_key_rejects_parent_directory_traversal() {
    let tmp = Temp::new("dotdot");
    let path = format!("{}/epoch=1/../../outside.parquet", tmp.0.display());
    assert!(
        Path::new(&path).strip_prefix(&tmp.0).is_ok(),
        "the fixture must stay lexically under the root so only the .. check refuses it"
    );
    let err = must_err(object_key(&tmp.0, &path));
    assert!(err.contains("outside"), "{err}");
}

/// Mutation: remove the local SHA-256 comparison in `expected_size`.
#[test]
fn a_local_hash_mismatch_blocks_the_drop() {
    let bytes = b"local-hash-must-match";
    let mut scene = FileScene::new(bytes);
    let mut flipped = bytes.to_vec();
    flipped[0] ^= 0xff;
    std::fs::write(&scene.path, &flipped).unwrap();
    stage_remote(&scene, bytes);
    let err = must_err(scene.offload(true));
    assert!(err.contains("sha256"), "{err}");
    assert!(scene.path.exists());
    scene.close();
}

/// Mutation: remove the local hash comparison before restore leaves an existing file in place.
#[test]
fn restore_refuses_to_overwrite_a_mismatched_file() {
    let good = b"restore-do-not-overwrite";
    let mut scene = FileScene::new(good);
    let mut bad = good.to_vec();
    bad[0] ^= 0xff;
    std::fs::write(&scene.path, &bad).unwrap();
    stage_remote(&scene, good);
    let err = must_err(restore(scene.store.as_mut().unwrap(), &scene.objects, &[4]));
    assert!(err.contains("overwrite"), "{err}");
    assert_eq!(std::fs::read(&scene.path).unwrap(), bad);
    scene.close();
}

/// Mutation: remove `require_inside_root` from `restore`.
#[test]
fn restore_refuses_a_path_outside_the_raw_root() {
    let good = b"stay-inside-the-root";
    let mut scene = FileScene::new(good);
    let outside = std::env::temp_dir().join(format!(
        "solos-offload-escape-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let outside_text = outside.to_string_lossy().into_owned();
    let key = scene.key.clone();
    scene
        .store
        .as_mut()
        .unwrap()
        .exec(
            "UPDATE files SET path=?, location='r2', r2_key=? WHERE epoch=4",
            &[&outside_text, &key],
        )
        .unwrap();
    scene.objects.insert(key, good.to_vec(), None, None);
    let err = must_err(restore(scene.store.as_mut().unwrap(), &scene.objects, &[4]));
    assert!(err.contains("outside"), "{err}");
    assert!(!outside.exists(), "restore wrote outside the raw root");
    scene.close();
}

/// Mutation: change the backup-gap gap status from 2 to 0.
#[test]
fn backup_gap_exits_2_on_a_gap_and_0_when_clean() {
    let tmp = Temp::new("gap-exit");
    let rel = "staging/transactions/epoch=1/old.parquet";
    let path = tmp.0.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"x").unwrap();
    let key = format!("phoenix_raw/{rel}");
    let catalog = json!({
        "files": [{
            "path": path.to_string_lossy(),
            "created_at": "2020-01-01T00:00:00Z",
            "location": "local",
            "r2_key": key,
        }]
    });
    std::fs::write(tmp.0.join("catalog.json"), catalog.to_string()).unwrap();
    let objects = MemoryStore::new();
    let now = chrono::DateTime::parse_from_rfc3339("2026-11-01T23:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let age = std::time::Duration::from_secs(30 * 3600);
    assert_eq!(backup_gap_exit(&tmp.0, &objects, age, now).unwrap(), 2);
    objects.insert(key, b"x".to_vec(), None, None);
    assert_eq!(backup_gap_exit(&tmp.0, &objects, age, now).unwrap(), 0);
}

/// Mutation: `continue` past a keyless catalog row, or treat a missing `created_at` as young.
#[test]
fn backup_gap_reports_a_keyless_entry_and_a_missing_timestamp() {
    let tmp = Temp::new("gap-keyless");
    let nostamp = tmp.0.join("staging/transactions/epoch=3/nostamp.parquet");
    let stamp_key = "phoenix_raw/staging/transactions/epoch=3/nostamp.parquet";
    let catalog = json!({
        "files": [
            {
                "path": "staging/transactions/epoch=9/nokey.parquet",
                "created_at": "2020-01-01T00:00:00Z",
                "location": "local"
            },
            {
                "path": "staging/transactions/epoch=9/young-nokey.parquet",
                "created_at": "2026-11-01T22:30:00Z",
                "location": "local"
            },
            {
                "path": nostamp.to_string_lossy(),
                "location": "local",
                "r2_key": stamp_key
            }
        ]
    });
    std::fs::write(tmp.0.join("catalog.json"), catalog.to_string()).unwrap();
    let now = chrono::DateTime::parse_from_rfc3339("2026-11-01T23:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let report = backup_gap(
        &tmp.0,
        &MemoryStore::new(),
        std::time::Duration::from_secs(30 * 3600),
        now,
    )
    .unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
    let files = report.rows("files").unwrap();
    assert!(
        files.iter().any(|row| row.str("reason") == Some("no-key")
            && row.str("path") == Some("staging/transactions/epoch=9/nokey.parquet")),
        "{files:?}"
    );
    assert!(
        !files.iter().any(|row| row
            .str("path")
            .is_some_and(|path| path.contains("young-nokey"))),
        "{files:?}"
    );
    assert!(
        files
            .iter()
            .any(|row| row.str("r2Key") == Some(stamp_key)
                && row.str("reason") == Some("catalog-local")),
        "{files:?}"
    );
}
