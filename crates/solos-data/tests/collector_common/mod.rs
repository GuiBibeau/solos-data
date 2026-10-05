//! Shared fixtures for the collector tests: a temporary raw root with the schema, the writer
//! thread, and the scripted RPC of `tests/fixtures.ts`.

#![allow(dead_code)]

use serde_json::{Value, json};
use solos_data::collector::config::{Config, Lane, load_config};
use solos_data::collector::rpc::{Rpc, RpcFailure, RpcFuture, RpcResponse};
use solos_data::db::{Db, DbThread};
use solos_data::store::Store;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// A test root with a store on the writer thread and the repository config pointed at it.
pub struct Fixture {
    pub root: PathBuf,
    pub db: Db,
    pub thread: Option<DbThread>,
    pub config: Config,
}

impl Fixture {
    pub fn new() -> Fixture {
        let root = std::env::temp_dir().join(format!("solos-data-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = Store::open(&root, solos_data::collector::schema::SCHEMA).unwrap();
        let (db, thread) = Db::spawn(store);
        let path = solos_data::collector::config::repository_config_path();
        let mut config = load_config(Some(path.to_str().unwrap())).unwrap();
        config.data_dir = root.clone();
        Fixture {
            root,
            db,
            thread: Some(thread),
            config,
        }
    }

    /// Close the store (joining the writer) and reopen it on a fresh writer thread.
    pub fn reopen(&mut self) {
        let store = self.thread.take().unwrap().join(self.db.clone());
        store.close().unwrap();
        let store = Store::open(&self.root, solos_data::collector::schema::SCHEMA).unwrap();
        let (db, thread) = Db::spawn(store);
        self.db = db;
        self.thread = Some(thread);
    }

    pub fn close(mut self) {
        let store = self.thread.take().unwrap().join(self.db.clone());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&self.root);
    }

    pub fn rows(&self, sql: &str) -> Vec<solos_data::jsonout::Obj> {
        let sql = sql.to_owned();
        self.db
            .run_blocking(move |store| store.rows(&sql, &[]))
            .unwrap()
    }

    pub fn exec(&self, sql: &str) {
        let sql = sql.to_owned();
        self.db
            .run_blocking(move |store| store.exec_batch(&sql))
            .unwrap();
    }

    pub fn get(&self, name: &str) -> Option<Value> {
        let name = name.to_owned();
        self.db.run_blocking(move |store| store.get(&name)).unwrap()
    }

    pub fn set(&self, name: &str, value: Value) {
        let name = name.to_owned();
        self.db
            .run_blocking(move |store| store.set(&name, &value))
            .unwrap();
    }

    pub fn scalar(&self, sql: &str) -> String {
        let rows = self.rows(sql);
        let row = &rows[0];
        let (_, value) = row.iter().next().unwrap();
        match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }
}

/// `insertTx(store, signature, slot)`.
pub fn insert_tx(f: &Fixture, signature: &str, slot: i64) {
    let sql = format!(
        "INSERT INTO transactions VALUES ('{signature}', {slot}, {slot}, NULL, NULL, 'null', 5000, 10, 'AA==', '{{}}', '{{}}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)"
    );
    f.exec(&sql);
}

/// `sig(signature, slot)`.
pub fn sig(signature: &str, slot: i64) -> Value {
    json!({ "signature": signature, "slot": slot, "blockTime": slot, "err": null })
}

/// The scripted provider: pages for `getSignaturesForAddress`, blocks for `getBlock`.
pub struct FixtureRpc {
    pub pages: Mutex<VecDeque<Vec<Value>>>,
    pub blocks: HashMap<i64, Vec<String>>,
    pub fail_block: AtomicBool,
    pub calls: Mutex<Vec<(String, Value)>>,
}

impl FixtureRpc {
    pub fn new(pages: Vec<Vec<Value>>, blocks: HashMap<i64, Vec<&str>>) -> FixtureRpc {
        FixtureRpc {
            pages: Mutex::new(pages.into()),
            blocks: blocks
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().map(str::to_owned).collect()))
                .collect(),
            fail_block: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn calls_of(&self, method: &str) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

fn failure(method: &str, text: &str) -> RpcFailure {
    RpcFailure {
        method: format!("{method}: {text}"),
        code: -1,
        throttled: false,
    }
}

impl Rpc for FixtureRpc {
    fn call<'a>(&'a self, method: &'a str, params: Value, _lane: Lane) -> RpcFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_owned(), params.clone()));
            let result = match method {
                "getSlot" => Value::from(102),
                "getSignaturesForAddress" => {
                    Value::Array(self.pages.lock().unwrap().pop_front().unwrap_or_default())
                }
                "getBlock" => {
                    if self.fail_block.load(Ordering::Relaxed) {
                        return Err(failure(method, "fixture block failure"));
                    }
                    let slot = params[0].as_i64().unwrap_or(0);
                    match self.blocks.get(&slot) {
                        Some(signatures) => json!({ "signatures": signatures }),
                        None => json!({ "signatures": null }),
                    }
                }
                other => return Err(failure(other, "Unexpected fixture method")),
            };
            Ok(RpcResponse {
                raw: result.to_string(),
                result,
            })
        })
    }

    fn provider_name(&self) -> &str {
        "fixture"
    }
}

/// A scripted call handler: method and params in, result or error text out.
pub type Handler = Box<dyn Fn(&str, &Value) -> Result<Value, String> + Send + Sync>;

/// A provider driven by a closure, for the bulk tests.
pub struct ClosureRpc {
    pub handler: Handler,
    pub calls: Mutex<usize>,
}

impl ClosureRpc {
    pub fn new(
        handler: impl Fn(&str, &Value) -> Result<Value, String> + Send + Sync + 'static,
    ) -> ClosureRpc {
        ClosureRpc {
            handler: Box::new(handler),
            calls: Mutex::new(0),
        }
    }
}

impl Rpc for ClosureRpc {
    fn call<'a>(&'a self, method: &'a str, params: Value, _lane: Lane) -> RpcFuture<'a> {
        Box::pin(async move {
            *self.calls.lock().unwrap() += 1;
            tokio::task::yield_now().await;
            match (self.handler)(method, &params) {
                Ok(result) => Ok(RpcResponse {
                    raw: result.to_string(),
                    result,
                }),
                Err(text) => Err(failure(method, &text)),
            }
        })
    }

    fn provider_name(&self) -> &str {
        "fixture"
    }
}

/// The TypeScript `wire(version, byte)` helper from `tests/bulk.test.ts`.
pub fn wire(version: u8, byte: u8) -> String {
    use base64::Engine;
    let signature = vec![byte; 64];
    let mut legacy = vec![1, 0, 0, 1];
    legacy.extend([0u8; 64]);
    legacy.push(0);
    let bytes = match version {
        0 => [vec![1], signature, vec![128], legacy, vec![0]].concat(),
        1 => [
            vec![129, 1, 0, 0],
            vec![0; 36],
            vec![0, 1],
            vec![0; 32],
            signature,
        ]
        .concat(),
        _ => [vec![1], signature, legacy].concat(),
    };
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn wire_signature(b64: &str) -> String {
    solana_wire::signature_of_base64(b64).unwrap()
}
