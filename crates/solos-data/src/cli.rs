//! Command-line surface. Arguments are parsed the way the TypeScript entry points parsed them:
//! a positional command, `--flag value` pairs anywhere, and JSON on stdout.

use crate::collector::config::{Config, load_config, provider_url};
use crate::collector::limiter::Limiter;
use crate::collector::rpc::Provider;
use crate::db::Db;
use crate::decoder::service::{DecoderConfig, run_decoder, stop_flag};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::{Store, StoreError};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Run the CLI and return the process exit code.
#[must_use]
pub fn main(args: &[String]) -> i32 {
    let group = args.first().map(String::as_str).unwrap_or("help");
    match group {
        "decoder" => decoder(&args[1..]),
        "collector" => collector(&args[1..]),
        "augment" => augment(&args[1..]),
        "dev" => dev(&args[1..]),
        _ => {
            println!(
                "{}",
                Obj::new()
                    .with("groups", json!(["collector", "decoder", "augment", "dev"]))
                    .to_json()
            );
            0
        }
    }
}

/// `--name value` from anywhere in the arguments.
#[must_use]
pub fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

// ---------------------------------------------------------------------------------------------
// decoder

fn decoder(args: &[String]) -> i32 {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if command == "help" || command == "--help" {
        println!(
            "{}",
            Obj::new()
                .with(
                    "commands",
                    json!([
                        "once",
                        "watch",
                        "status",
                        "query --sql SELECT ... [--slots <from>-<to>]",
                        "verify-storage (offline)",
                        "repack (offline)"
                    ])
                )
                .with(
                    "config",
                    "--config config/decoded.json; no RPC credential required"
                )
                .to_json()
        );
        return 0;
    }
    match decoder_command(command, args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!(
                "{}",
                Obj::new()
                    .with("at", now())
                    .with("event", "decoder_fatal")
                    .with("error", safe_error(&error.to_string()))
                    .to_json()
            );
            1
        }
    }
}

/// Load `config/decoded.json` (or `--config`), apply the environment and validate.
pub fn load_decoder_config(args: &[String]) -> Result<DecoderConfig, StoreError> {
    let path = flag(args, "--config").unwrap_or("config/decoded.json");
    let text = std::fs::read_to_string(path)?;
    let config: Value =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let field = |name: &str| config.get(name).and_then(Value::as_str).map(str::to_owned);
    let raw_dir = std::env::var("SOLOS_DATA_RAW_DIR")
        .ok()
        .or_else(|| field("rawDir"))
        .ok_or_else(|| StoreError::Check("rawDir is required".into()))?;
    let data_dir = std::env::var("SOLOS_DATA_DECODED_DIR")
        .ok()
        .or_else(|| field("dataDir"))
        .ok_or_else(|| StoreError::Check("dataDir is required".into()))?;
    let batch_size = config.get("batchSize").and_then(Value::as_i64).unwrap_or(0);
    let poll_ms = config.get("pollMs").and_then(Value::as_i64).unwrap_or(0);
    let checkpoint_interval = config
        .get("checkpointIntervalSeconds")
        .and_then(Value::as_i64)
        .unwrap_or(900);
    if checkpoint_interval < 0 {
        return Err(StoreError::Check(
            "invalid checkpointIntervalSeconds".into(),
        ));
    }
    if !(1..=10_000).contains(&batch_size) {
        return Err(StoreError::Check("invalid batchSize".into()));
    }
    if poll_ms < 100 {
        return Err(StoreError::Check("invalid pollMs".into()));
    }
    let cwd = std::env::current_dir()?;
    let raw_dir = crate::fsutil::resolve(&cwd, Path::new(&raw_dir));
    let data_dir = crate::fsutil::resolve(&cwd, Path::new(&data_dir));
    if raw_dir == data_dir {
        return Err(StoreError::Check(
            "raw and decoded roots must differ".into(),
        ));
    }
    Ok(DecoderConfig {
        raw_dir,
        data_dir,
        batch_size: batch_size as u64,
        poll_ms: poll_ms as u64,
        checkpoint_interval_seconds: checkpoint_interval as u64,
    })
}

/// `--slots <from>-<to>`, inclusive, for a query scoped to the epochs covering that range.
fn parse_slots(text: &str) -> Result<(i64, i64), StoreError> {
    let invalid = || StoreError::Check("--slots expects <from>-<to>, both slots".into());
    let (from, to) = text.split_once('-').ok_or_else(invalid)?;
    let from = from.trim().parse::<i64>().map_err(|_| invalid())?;
    let to = to.trim().parse::<i64>().map_err(|_| invalid())?;
    if from < 0 || from > to {
        return Err(invalid());
    }
    Ok((from, to))
}

fn decoder_command(command: &str, args: &[String]) -> Result<(), StoreError> {
    let config = load_decoder_config(args)?;
    match command {
        "status" => {
            print!(
                "{}",
                std::fs::read_to_string(config.data_dir.join("status.json"))?
            );
            Ok(())
        }
        "query" => {
            let sql = flag(args, "--sql")
                .ok_or_else(|| StoreError::Check("query requires --sql".into()))?;
            let slots = flag(args, "--slots").map(parse_slots).transpose()?;
            println!(
                "{}",
                crate::decoder::reader::query_decoded_slots(&config.data_dir, sql, slots)?
                    .to_json()
            );
            Ok(())
        }
        "repack" => {
            println!(
                "{}",
                crate::repack::repack_checkpoint(&config.data_dir)?.to_json()
            );
            Ok(())
        }
        "verify-storage" => {
            let mut store = Store::open(&config.data_dir, crate::decoder::schema::schema_static())?;
            let result = crate::decoder::verify::verify_decoded(&mut store);
            store.close()?;
            println!("{}", result?.to_json());
            Ok(())
        }
        "once" | "watch" => run_decoder(&config, command == "once", stop_flag()?),
        _ => Err(StoreError::Check("unknown decoder command".into())),
    }
}

// ---------------------------------------------------------------------------------------------
// collector

const COLLECTOR_COMMANDS: [(&str, &str); 16] = [
    (
        "run",
        "Supervised finalized-only collector (tail + backfill)",
    ),
    ("probe", "Bounded live sizing; isolated in dataDir/probe"),
    (
        "capabilities",
        "Probe Alchemy extensions and oldest indexed program transactions",
    ),
    (
        "status",
        "Read supervisor status snapshot without opening the live writer",
    ),
    (
        "catalog",
        "Read published file/coverage catalog while collection runs",
    ),
    (
        "query",
        "Query published Parquet data while collection runs: --sql SELECT ... or --sql-file path",
    ),
    (
        "verify-storage",
        "Offline hash/count verification; stop the service first",
    ),
    (
        "maintain",
        "Offline compaction/verified checkpoint trimming/GC; --all drains checkpoint copies; --legacy also scans old files; stop writer first",
    ),
    (
        "repair-next-backfill",
        "Offline ordering repair using finalized cached blocks only; stop writer first",
    ),
    (
        "validate-next-backfill",
        "Offline validation report for the pending historical chunk; stop writer first",
    ),
    (
        "storage-stats",
        "Offline checkpoint allocation and table sizes; stop writer first",
    ),
    (
        "repack",
        "Offline verified atomic checkpoint rewrite to reclaim space; stop writer first",
    ),
    (
        "relocate",
        "Offline rebase and verify raw file registrations after moving the data root; stop the service first",
    ),
    (
        "benchmark-decoder",
        "Offline published-file resume benchmark: --mode legacy|bounded",
    ),
    (
        "benchmark-ordering",
        "Offline checkpoint benchmark with rolled-back writes: --mode legacy|bounded; stop writer first",
    ),
    (
        "benchmark-ingest",
        "Offline duplicate-page insert with rolled-back writes; stop writer first",
    ),
];

fn collector(args: &[String]) -> i32 {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if command == "help" || command == "--help" {
        let mut commands = Obj::new();
        for (name, text) in COLLECTOR_COMMANDS {
            commands.set(name, text);
        }
        println!(
            "{}",
            Obj::new()
                .with_obj("commands", commands)
                .with(
                    "config",
                    "--config path or SOLOS_DATA_CONFIG; credentials: SOLANA_RPC_URL only"
                )
                .to_json()
        );
        return 0;
    }
    match collector_command(command, args) {
        Ok(()) => 0,
        Err(error) => {
            log(
                "fatal",
                Obj::new().with("error", safe_error(&error.to_string())),
            );
            1
        }
    }
}

/// A provider that refuses every call; `repair-next-backfill` must use cached blocks only.
struct NoRpc;

impl crate::collector::rpc::Rpc for NoRpc {
    fn call<'a>(
        &'a self,
        method: &'a str,
        _params: Value,
        _lane: crate::collector::config::Lane,
    ) -> crate::collector::rpc::RpcFuture<'a> {
        Box::pin(async move {
            Err(crate::collector::rpc::RpcFailure {
                method: format!(
                    "{method}: missing finalized block cache; resume collector to fetch"
                ),
                code: -1,
                throttled: false,
            })
        })
    }

    fn provider_name(&self) -> &str {
        "none"
    }
}

fn collector_command(command: &str, args: &[String]) -> Result<(), StoreError> {
    let config_path = flag(args, "--config")
        .map(str::to_owned)
        .or_else(|| std::env::var("SOLOS_DATA_CONFIG").ok());
    let mut config: Config = load_config(config_path.as_deref())?;
    let mode = flag(args, "--mode").unwrap_or("bounded");
    if command.starts_with("benchmark-") && !["legacy", "bounded"].contains(&mode) {
        return Err(StoreError::Check("invalid benchmark mode".into()));
    }
    if command == "benchmark-decoder" {
        println!(
            "{}",
            crate::collector::diagnostics::benchmark_decoder(&config.data_dir, mode)?.to_json()
        );
        return Ok(());
    }
    if command == "status" || command == "catalog" {
        print!(
            "{}",
            std::fs::read_to_string(config.data_dir.join(format!("{command}.json")))?
        );
        return Ok(());
    }
    if command == "query" {
        let sql = match (flag(args, "--sql-file"), flag(args, "--sql")) {
            (Some(file), _) => std::fs::read_to_string(file)?,
            (None, Some(sql)) => sql.to_owned(),
            (None, None) => {
                return Err(StoreError::Check(
                    "query requires --sql or --sql-file".into(),
                ));
            }
        };
        println!(
            "{}",
            crate::collector::reader::query_dataset(&config.data_dir, &sql)?.to_json()
        );
        return Ok(());
    }
    if command == "probe" {
        config.data_dir = config.data_dir.join("probe");
    }
    if command == "repack" {
        println!(
            "{}",
            crate::repack::repack_checkpoint(&config.data_dir)?.to_json()
        );
        return Ok(());
    }
    if !COLLECTOR_COMMANDS.iter().any(|(name, _)| *name == command) {
        return Err(StoreError::Check("Unknown command".into()));
    }
    let mut store = Store::open(&config.data_dir, crate::collector::schema::SCHEMA)?;
    let result = offline_or_live(command, args, &mut store, config, mode);
    store.close()?;
    result
}

/// Offline commands run on the store directly; live commands move it to the writer thread.
fn offline_or_live(
    command: &str,
    args: &[String],
    store: &mut Store,
    config: Config,
    mode: &str,
) -> Result<(), StoreError> {
    let outcome: Result<(), StoreError> = (|| {
        match command {
            "benchmark-ordering" => println!(
                "{}",
                crate::collector::diagnostics::benchmark_ordering(store, mode)?.to_json()
            ),
            "benchmark-ingest" => println!(
                "{}",
                crate::collector::diagnostics::benchmark_ingest(store, mode)?.to_json()
            ),
            "validate-next-backfill" | "repair-next-backfill" => {
                let progress = store
                    .get("backfill")?
                    .ok_or_else(|| StoreError::Check("no backfill cursor".into()))?;
                let next = progress.get("next").and_then(Value::as_i64).unwrap_or(0);
                let (from, to) = (next - config.backfill_chunk_slots + 1, next);
                if command == "repair-next-backfill" {
                    let store_moved = std::mem::replace(
                        store,
                        Store::open(&config.data_dir.join(".placeholder"), "")?,
                    );
                    let (db, thread) = Db::spawn(store_moved);
                    let runtime = tokio::runtime::Runtime::new()?;
                    let repair = runtime.block_on(crate::collector::ordering::order_range(
                        Arc::new(NoRpc),
                        &db,
                        crate::collector::config::Lane::Backfill,
                        from,
                        to,
                        256,
                    ));
                    *store = thread.join(db);
                    let _ = std::fs::remove_dir_all(config.data_dir.join(".placeholder"));
                    repair?;
                }
                let report = crate::collector::validation::validate_range(store, from, to)?;
                let anomalies = store.rows(
                    "SELECT s.slot,count(*) AS n,count(t.tx_index) AS indexed,
        count(DISTINCT t.tx_index) AS distinct_index,count(*) FILTER(WHERE t.single_in_slot) AS singles,
        count(*) FILTER(WHERE t.single_in_slot IS NULL) AS unknown,min(t.slot) AS transaction_slot,
        count(*) FILTER(WHERE t.slot IS DISTINCT FROM s.slot) AS slot_mismatch
        FROM signatures s LEFT JOIN transactions t USING(signature) WHERE s.slot BETWEEN ? AND ?
        GROUP BY s.slot HAVING (n>1 AND (indexed<>n OR distinct_index<>n OR singles>0)) OR (n=1 AND singles<>1) LIMIT 10",
                    &[&from, &to],
                )?;
                let mut out = report;
                out.set_rows("anomalies", anomalies);
                println!("{}", out.to_json());
            }
            "storage-stats" => println!(
                "{}",
                crate::collector::diagnostics::storage_stats(store)?.to_json()
            ),
            "verify-storage" => println!(
                "{}",
                crate::collector::writer::verify_files(store)?.to_json()
            ),
            "maintain" => println!(
                "{}",
                crate::collector::maintenance::maintain(
                    store,
                    &config,
                    has(args, "--all"),
                    has(args, "--legacy")
                )?
                .to_json()
            ),
            "relocate" => println!(
                "{}",
                crate::collector::maintenance::relocate(store)?.to_json()
            ),
            "capabilities" | "probe" | "run" => {
                let limiter = Arc::new(Limiter::new(
                    config.cu_per_second * config.utilization,
                    config.tail_share,
                    config.concurrency.clone(),
                ));
                let provider = Arc::new(Provider::new(provider_url()?, config.clone(), limiter));
                let runtime = tokio::runtime::Runtime::new()?;
                if command == "capabilities" {
                    let rpc: Arc<dyn crate::collector::rpc::Rpc> = provider.clone();
                    let report =
                        runtime.block_on(crate::collector::probe::capabilities(rpc, &config))?;
                    println!("{}", report.to_json());
                    return Ok(());
                }
                let store_moved = std::mem::replace(
                    store,
                    Store::open(&config.data_dir.join(".placeholder"), "")?,
                );
                let (db, thread) = Db::spawn(store_moved);
                let outcome = if command == "probe" {
                    let rpc: Arc<dyn crate::collector::rpc::Rpc> = provider.clone();
                    runtime
                        .block_on(crate::collector::probe::probe(rpc, &db, &config))
                        .map(|report| println!("{}", report.to_json()))
                } else {
                    runtime.block_on(crate::collector::service::run(
                        provider,
                        db.clone(),
                        config.clone(),
                    ))
                };
                *store = thread.join(db);
                let _ = std::fs::remove_dir_all(config.data_dir.join(".placeholder"));
                outcome?;
            }
            _ => return Err(StoreError::Check("Unknown command".into())),
        }
        Ok(())
    })();
    outcome
}

// ---------------------------------------------------------------------------------------------
// augment

const AUGMENT_COMMANDS: [(&str, &str); 6] = [
    (
        "sync",
        "Backfill and catch up dated files and paged histories, then exit: [--source name] [--symbol SYM]",
    ),
    (
        "capture",
        "Long-running capture of streams that cannot be fetched later: Hyperliquid candles and asset contexts, Elfa",
    ),
    ("status", "Read both lanes' status snapshots"),
    ("catalog", "Read the sync lane's file catalog"),
    (
        "sec-map",
        "Look the equity symbols up in EDGAR's company_tickers.json and report their CIKs: [--write] stores them in the config",
    ),
    (
        "query",
        "Read-only SQL over the catalogued files: --sql SELECT ... FROM <source>_<dataset>",
    ),
];

fn augment(args: &[String]) -> i32 {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if command == "help" || command == "--help" {
        let mut commands = Obj::new();
        for (name, text) in AUGMENT_COMMANDS {
            commands.set(name, text);
        }
        println!(
            "{}",
            Obj::new()
                .with_obj("commands", commands)
                .with(
                    "config",
                    "--config config/augment.json; SOLOS_DATA_AUGMENT_DIR overrides dataDir; ELFA_API_KEY enables Elfa"
                )
                .to_json()
        );
        return 0;
    }
    match augment_command(command, args) {
        Ok(code) => code,
        Err(error) => {
            log(
                "augment_fatal",
                Obj::new().with("error", safe_error(&error.to_string())),
            );
            1
        }
    }
}

fn augment_command(command: &str, args: &[String]) -> Result<i32, StoreError> {
    let config = crate::augment::config::load_augment_config(flag(args, "--config"))?;
    match command {
        "status" => {
            println!(
                "{}",
                crate::augment::ledger::read_statuses(&config.data_dir).to_json()
            );
            Ok(0)
        }
        "catalog" => {
            print!(
                "{}",
                std::fs::read_to_string(config.data_dir.join("catalog.json"))?
            );
            Ok(0)
        }
        "capture" => {
            let status = crate::augment::capture::run_capture(&config, stop_flag()?)?;
            println!("{}", status.to_json());
            Ok(0)
        }
        "sec-map" => {
            let path = flag(args, "--config").unwrap_or("config/augment.json");
            let write = args.iter().any(|a| a == "--write");
            let runtime = tokio::runtime::Runtime::new()?;
            let report = runtime.block_on(crate::augment::sec::map_ciks(path, &config, write))?;
            println!("{}", report.to_json());
            Ok(0)
        }
        "query" => {
            let sql = flag(args, "--sql")
                .ok_or_else(|| StoreError::Check("query requires --sql".into()))?;
            println!(
                "{}",
                crate::augment::query::query_augment(&config.data_dir, sql)?.to_json()
            );
            Ok(0)
        }
        "sync" => {
            let filter = crate::augment::sync::Filter {
                source: flag(args, "--source").map(str::to_owned),
                symbol: flag(args, "--symbol").map(str::to_owned),
            };
            let status = crate::augment::sync::run_sync(
                &config,
                chrono::Utc::now().timestamp_millis(),
                &filter,
                stop_flag()?,
            )?;
            println!("{}", status.to_json());
            let errors = status_errors(&status);
            Ok(i32::from(errors > 0))
        }
        _ => Err(StoreError::Check("unknown augment command".into())),
    }
}

/// `totals.errors` of a sync status.
fn status_errors(status: &Obj) -> i64 {
    status
        .to_value()
        .get("totals")
        .and_then(|t| t.get("errors"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------------------------
// dev

/// Re-collect a published range through the archive lane and compare it with the raw archive.
fn archive_check(args: &[String], from: i64, to: i64) -> Result<Obj, StoreError> {
    let config_path = flag(args, "--config")
        .map(str::to_owned)
        .or_else(|| std::env::var("SOLOS_DATA_CONFIG").ok());
    let mut config: Config = load_config(config_path.as_deref())?;
    config.archive_backfill_enabled = true;
    let limiter = Arc::new(Limiter::new(
        config.cu_per_second * config.utilization,
        config.tail_share,
        config.concurrency.clone(),
    ));
    let provider = Arc::new(Provider::new(provider_url()?, config.clone(), limiter));
    let runtime = tokio::runtime::Runtime::new()?;
    let rpc: Arc<dyn crate::collector::rpc::Rpc> = provider.clone();
    runtime.block_on(async {
        let http = reqwest::Client::new();
        let scratch = config
            .data_dir
            .join(format!(".archive-check-exchange-{}", uuid::Uuid::new_v4()));
        let store = Store::open(&scratch, crate::collector::schema::SCHEMA)?;
        let (db, thread) = Db::spawn(store);
        let exchange =
            crate::collector::exchange::refresh_exchange(rpc.as_ref(), &db, &config, &http).await;
        let store = thread.join(db);
        store.close()?;
        let _ = std::fs::remove_dir_all(&scratch);
        let exchange = exchange?;
        crate::collector::archive::check_range(rpc, &config, &exchange.program_data, from, to).await
    })
}

fn dev(args: &[String]) -> i32 {
    let command = args.first().map(String::as_str).unwrap_or("help");
    let result = match command {
        "compare-decoded" => {
            let (Some(left), Some(right)) = (flag(args, "--left"), flag(args, "--right")) else {
                eprintln!(
                    "{}",
                    Obj::new()
                        .with(
                            "error",
                            "compare-decoded requires --left and --right decoded roots"
                        )
                        .to_json()
                );
                return 1;
            };
            crate::dev::compare_decoded(&PathBuf::from(left), &PathBuf::from(right))
        }
        "archive-check" => {
            let (Some(from), Some(to)) = (
                flag(args, "--from").and_then(|v| v.parse::<i64>().ok()),
                flag(args, "--to").and_then(|v| v.parse::<i64>().ok()),
            ) else {
                eprintln!(
                    "{}",
                    Obj::new()
                        .with("error", "archive-check requires --from and --to slots")
                        .to_json()
                );
                return 1;
            };
            archive_check(args, from, to)
        }
        "crash-writer" => {
            // Test fixture: commit one row, leave a transaction open, wait to be killed.
            let Some(root) = flag(args, "--root") else {
                return 1;
            };
            return match crate::dev::crash_writer(&PathBuf::from(root)) {
                Ok(()) => 0,
                Err(_) => 1,
            };
        }
        _ => {
            println!(
                "{}",
                Obj::new()
                    .with(
                        "commands",
                        json!([
                            "compare-decoded --left <root> --right <root>",
                            "archive-check --from <slot> --to <slot> [--config path]",
                            "crash-writer --root <root> (test fixture)"
                        ])
                    )
                    .to_json()
            );
            return 0;
        }
    };
    match result {
        Ok(report) => {
            println!("{}", report.to_json());
            i32::from(report.get("identical") != Some(&Value::Bool(true)))
        }
        Err(error) => {
            eprintln!(
                "{}",
                Obj::new()
                    .with("at", now())
                    .with("event", "fatal")
                    .with("error", safe_error(&error.to_string()))
                    .to_json()
            );
            1
        }
    }
}
