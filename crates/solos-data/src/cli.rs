//! Command-line surface. Arguments are parsed the way the TypeScript entry points parsed them:
//! a positional command, `--flag value` pairs anywhere, and JSON on stdout.

use crate::decoder::service::{DecoderConfig, run_decoder, stop_flag};
use crate::jsonout::{Obj, now, safe_error};
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Run the CLI and return the process exit code.
#[must_use]
pub fn main(args: &[String]) -> i32 {
    let group = args.first().map(String::as_str).unwrap_or("help");
    match group {
        "decoder" => decoder(&args[1..]),
        "dev" => dev(&args[1..]),
        "collector" => {
            eprintln!(
                "{}",
                Obj::new()
                    .with("at", now())
                    .with("event", "fatal")
                    .with(
                        "error",
                        "the collector is not in this binary yet; run the TypeScript collector"
                    )
                    .to_json()
            );
            1
        }
        _ => {
            println!(
                "{}",
                Obj::new()
                    .with(
                        "groups",
                        Value::Array(
                            ["collector", "decoder", "dev"]
                                .iter()
                                .map(|g| Value::String((*g).into()))
                                .collect()
                        )
                    )
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

fn decoder(args: &[String]) -> i32 {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if command == "help" || command == "--help" {
        println!(
            "{}",
            Obj::new()
                .with(
                    "commands",
                    serde_json::json!([
                        "once",
                        "watch",
                        "status",
                        "query --sql SELECT ...",
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
    })
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
            println!(
                "{}",
                crate::decoder::reader::query_decoded(&config.data_dir, sql)?.to_json()
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
        _ => {
            println!(
                "{}",
                Obj::new()
                    .with(
                        "commands",
                        serde_json::json!(["compare-decoded --left <root> --right <root>"])
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
