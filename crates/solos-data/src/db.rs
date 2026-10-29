//! One writer thread owns the DuckDB checkpoint; asynchronous lanes send it closures and await the
//! result. This is the TypeScript `Store.exclusive` promise queue, with the database work kept
//! off the async executor.

use crate::store::{Store, StoreError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce(&mut Store) + Send + 'static>;

enum Message {
    Job(Job),
    Stop,
}

/// Error classes after which a DuckDB connection is not trusted
/// (`docs/issues/002-poisoned-connection.md`): a failed rollback or "cannot continue"
/// (`rollback_failed`), a failed commit (`commit_failed`) and DuckDB out of its buffer (`oom`). In
/// the 2026-10-14 and 2026-10-16 incidents none of them cleared on the same connection.
#[must_use]
pub fn fatal_class(message: &str) -> Option<&'static str> {
    // A job refused by a poisoned writer carries the original error: classify that one.
    if let Some(reason) = refused_reason(message) {
        return Some(fatal_class(reason).unwrap_or("rollback_failed"));
    }
    let m = message.to_ascii_lowercase();
    if m.contains("failed to rollback") || m.contains("cannot continue operation") {
        Some("rollback_failed")
    } else if m.contains("failed to commit") {
        Some("commit_failed")
    } else if m.contains("out of memory") {
        Some("oom")
    } else if m.contains("fsync failed") {
        // A failed fsync is not retried: the lane exits and the process restarts from W (ADR-0016).
        Some("fsync")
    } else {
        None
    }
}

/// The error every job gets once a guarded writer has seen a fatal error.
const POISONED: &str = "checkpoint connection poisoned";

/// The original error of a job refused by a poisoned writer, or `None` for any other message.
#[must_use]
pub fn refused_reason(message: &str) -> Option<&str> {
    message
        .strip_prefix(POISONED)
        .map(|rest| rest.trim_start_matches(':').trim_start())
}

#[derive(Default)]
struct Guard {
    enabled: AtomicBool,
    poisoned: Mutex<Option<String>>,
}

/// Handle to the writer thread. Cloning shares the same queue.
#[derive(Clone)]
pub struct Db {
    sender: mpsc::Sender<Message>,
    guard: Arc<Guard>,
}

/// The writer thread, joined on [`Db::close`].
pub struct DbThread {
    handle: JoinHandle<Store>,
}

impl Db {
    /// Move `store` to a new thread and return the handle pair.
    #[must_use]
    pub fn spawn(store: Store) -> (Db, DbThread) {
        let (sender, receiver) = mpsc::channel::<Message>();
        let handle = std::thread::Builder::new()
            .name("duckdb-writer".into())
            .spawn(move || {
                let mut store = store;
                while let Ok(message) = receiver.recv() {
                    match message {
                        Message::Job(job) => job(&mut store),
                        Message::Stop => break,
                    }
                }
                store
            })
            .expect("writer thread spawns");
        (
            Db {
                sender,
                guard: Arc::default(),
            },
            DbThread { handle },
        )
    }

    /// From now on, the first job that fails with a [`fatal_class`] error poisons the writer: every
    /// later job, including those already queued, fails with that error without touching the
    /// connection. The collector sets this so nothing runs between a failed rollback and its exit.
    pub fn guard_fatal(&self) {
        self.guard.enabled.store(true, Ordering::Relaxed);
    }

    /// The fatal error that poisoned a guarded writer, if any.
    #[must_use]
    pub fn poisoned(&self) -> Option<String> {
        self.guard.poisoned.lock().expect("guard").clone()
    }

    fn guarded<T>(
        guard: &Guard,
        store: &mut Store,
        job: impl FnOnce(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if let Some(reason) = guard.poisoned.lock().expect("guard").as_ref() {
            return Err(StoreError::Check(format!("{POISONED}: {reason}")));
        }
        let result = job(store);
        if let Err(error) = &result
            && guard.enabled.load(Ordering::Relaxed)
        {
            let message = error.to_string();
            if fatal_class(&message).is_some() {
                *guard.poisoned.lock().expect("guard") = Some(message);
            }
        }
        result
    }

    /// Run `job` on the writer thread and await its result.
    pub async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let (reply, receive) = oneshot::channel();
        let guard = Arc::clone(&self.guard);
        self.sender
            .send(Message::Job(Box::new(move |store| {
                let _ = reply.send(Self::guarded(&guard, store, job));
            })))
            .map_err(|_| StoreError::Check("database writer stopped".into()))?;
        receive
            .await
            .map_err(|_| StoreError::Check("database writer dropped a job".into()))?
    }

    /// Run `job` synchronously from a blocking context (tests, offline commands).
    pub fn run_blocking<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let (reply, receive) = std::sync::mpsc::channel();
        let guard = Arc::clone(&self.guard);
        self.sender
            .send(Message::Job(Box::new(move |store| {
                let _ = reply.send(Self::guarded(&guard, store, job));
            })))
            .map_err(|_| StoreError::Check("database writer stopped".into()))?;
        receive
            .recv()
            .map_err(|_| StoreError::Check("database writer dropped a job".into()))?
    }

    /// `kv` lookup.
    pub async fn get(&self, name: &str) -> Result<Option<serde_json::Value>, StoreError> {
        let name = name.to_owned();
        self.run(move |store| store.get(&name)).await
    }

    /// `kv` upsert.
    pub async fn set(&self, name: &str, value: serde_json::Value) -> Result<(), StoreError> {
        let name = name.to_owned();
        self.run(move |store| store.set(&name, &value)).await
    }

    /// Query rows.
    pub async fn rows(
        &self,
        sql: &str,
        params: Vec<crate::store::Param>,
    ) -> Result<Vec<crate::jsonout::Obj>, StoreError> {
        let sql = sql.to_owned();
        self.run(move |store| {
            store.rows(
                &sql,
                &params
                    .iter()
                    .map(|p| p as &dyn duckdb::ToSql)
                    .collect::<Vec<_>>(),
            )
        })
        .await
    }

    /// Execute one statement.
    pub async fn exec(
        &self,
        sql: &str,
        params: Vec<crate::store::Param>,
    ) -> Result<usize, StoreError> {
        let sql = sql.to_owned();
        self.run(move |store| {
            store.exec(
                &sql,
                &params
                    .iter()
                    .map(|p| p as &dyn duckdb::ToSql)
                    .collect::<Vec<_>>(),
            )
        })
        .await
    }
}

impl DbThread {
    /// Stop the writer after the jobs already queued and return the store. Other handles may
    /// still exist; their later jobs fail with "database writer stopped".
    pub fn join(self, db: Db) -> Store {
        let _ = db.sender.send(Message::Stop);
        drop(db);
        self.handle.join().expect("writer thread joins")
    }
}
