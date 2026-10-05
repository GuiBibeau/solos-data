//! One writer thread owns the DuckDB checkpoint; asynchronous lanes send it closures and await the
//! result. This is the TypeScript `Store.exclusive` promise queue, with the database work kept
//! off the async executor.

use crate::store::{Store, StoreError};
use std::sync::mpsc;
use std::thread::JoinHandle;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce(&mut Store) + Send + 'static>;

enum Message {
    Job(Job),
    Stop,
}

/// Handle to the writer thread. Cloning shares the same queue.
#[derive(Clone)]
pub struct Db {
    sender: mpsc::Sender<Message>,
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
        (Db { sender }, DbThread { handle })
    }

    /// Run `job` on the writer thread and await its result.
    pub async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Message::Job(Box::new(move |store| {
                let _ = reply.send(job(store));
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
        self.sender
            .send(Message::Job(Box::new(move |store| {
                let _ = reply.send(job(store));
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
