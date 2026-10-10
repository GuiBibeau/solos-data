//! Elfa call episodes against an in-process server that behaves like `/v3/calls/episodes`:
//! every episode active in `[from, to]`, ordered by `openedAt`, thirty a page, with an opaque
//! cursor. Hundreds of old episodes stay open, so an ascending page-capped pull would never
//! reach the present; the lane must store the newest episodes in its first cycle, finish the
//! old ones across cycles from its stored cursor, keep up with new openings, and survive a
//! failed page.

mod augment_common;

use augment_common::{Response, Server, tempdir};
use serde_json::{Value, json};
use solos_data::augment::elfa::ElfaLane;
use solos_data::augment::episodes::{self, State};
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::series::Ctx;
use solos_data::db::Db;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// 2026-10-09T12:00:00Z.
const NOW_S: i64 = 1_791_547_200;

fn ctx(server: &Server, root: &std::path::Path) -> (Ctx, solos_data::db::DbThread) {
    let mut store = ledger::open(root, Lane::Capture).unwrap();
    ledger::recover(&mut store, root, Lane::Capture).unwrap();
    let (db, thread) = Db::spawn(store);
    let http = Http::new(200.0).unwrap();
    http.set_host_rate(&server.base, 200.0);
    let ctx = Ctx {
        http,
        db,
        root: root.to_path_buf(),
        lane: Lane::Capture,
        start: solos_data::augment::periods::parse_date("2026-09-01").unwrap(),
        now_ms: NOW_S * 1000,
        stop: Arc::new(AtomicBool::new(false)),
        disk_budget_bytes: 0,
    };
    (ctx, thread)
}

fn count(ctx: &Ctx, root: &std::path::Path) -> (i64, i64) {
    let root_clone = root.to_path_buf();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let rows = solos_data::augment::query::query_augment(
        root,
        "SELECT count(*) AS n, count(DISTINCT id) AS ids FROM elfa_episodes",
    )
    .unwrap()
    .to_value();
    let row = &rows["rows"][0];
    let n = |k: &str| row[k].as_str().unwrap().parse::<i64>().unwrap();
    (n("n"), n("ids"))
}

fn state(ctx: &Ctx) -> State {
    let key = solos_data::augment::elfa::progress_key(solos_data::augment::elfa::Stream::Episodes);
    let value = ctx
        .db
        .run_blocking(move |store| ledger::get_progress(store, &key))
        .unwrap();
    State::from_progress(value.as_ref())
}

/// The fake API: `episodes` are (id, openedAt), all open; `fail_cursor` answers 400 once.
fn serve(server: &Server, episodes: Arc<Mutex<Vec<(String, i64)>>>, fail: Arc<AtomicI64>) {
    server.route("/v3/calls/episodes", move |request| {
        let from: i64 = request.param("from").unwrap().parse().unwrap();
        let to: i64 = request.param("to").unwrap().parse().unwrap();
        let desc = request.param("order").as_deref() == Some("desc");
        let offset: usize = request
            .param("cursor")
            .map_or(0, |c| c.trim_start_matches('o').parse().unwrap());
        if offset > 0 && fail.load(Ordering::SeqCst) == offset as i64 {
            fail.store(-1, Ordering::SeqCst);
            return Response::status(400);
        }
        // Active in the window: opened by `to` and still open (none closes here).
        let mut active: Vec<(String, i64)> = episodes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, opened)| *opened <= to && to >= from)
            .cloned()
            .collect();
        active.sort_by_key(|(id, opened)| (*opened, id.clone()));
        if desc {
            active.reverse();
        }
        let page: Vec<Value> = active
            .iter()
            .skip(offset)
            .take(30)
            .map(|(id, opened)| json!({ "id": id, "openedAt": opened, "observedAt": opened + 15, "status": "open" }))
            .collect();
        let more = offset + 30 < active.len();
        Response::json(&json!({
            "episodes": page,
            "hasMore": more,
            "nextCursor": if more { Some(format!("o{}", offset + 30)) } else { None },
        }))
    });
}

#[test]
fn episodes_reach_the_present_first_then_catch_up_from_the_stored_cursor() {
    let server = Server::start();
    // 300 episodes opened 120 to 90 days ago and still open, 60 opened in the last 30 days.
    let mut all: Vec<(String, i64)> = (0..300)
        .map(|i| (format!("old{i}"), NOW_S - 120 * 86_400 + i * 8_640))
        .collect();
    all.extend((0..60).map(|i| (format!("new{i}"), NOW_S - 30 * 86_400 + i * 43_200)));
    let total = all.len() as i64;
    let newest = all.iter().map(|(_, o)| *o).max().unwrap();
    let episodes = Arc::new(Mutex::new(all));
    let fail = Arc::new(AtomicI64::new(-1));
    serve(&server, Arc::clone(&episodes), Arc::clone(&fail));
    let root = tempdir("solos-episodes");
    let (ctx, thread) = ctx(&server, &root);
    let lane = ElfaLane::new(&server.base, "test-key", NOW_S - 30 * 86_400);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let start_s = lane.start_s;

    // Cycle 1, three pages: the newest ninety, the present included, and a pending catch-up.
    let outcome = runtime
        .block_on(episodes::pull(&ctx, &lane, start_s, NOW_S, 3))
        .unwrap();
    assert_eq!(outcome.requests, 3);
    assert_eq!(count(&ctx, &root), (90, 90));
    let after_first = state(&ctx);
    assert_eq!(after_first.newest_opened_at, Some(newest));
    assert_eq!(after_first.pending.len(), 1);
    assert_eq!(after_first.pending[0].cursor.as_deref(), Some("o90"));
    assert!(
        server
            .hits()
            .iter()
            .all(|h| h.param("order").as_deref() == Some("desc"))
    );

    // Two new openings; cycle 2 pages the head (one page reaches below newest - 1 h) and
    // resumes the catch-up at its cursor; the third catch-up page fails and stays pending.
    {
        let mut list = episodes.lock().unwrap();
        list.push(("fresh1".into(), NOW_S + 600));
        list.push(("fresh2".into(), NOW_S + 1_200));
    }
    fail.store(150, Ordering::SeqCst);
    let hits = server.hit_count();
    let result = runtime.block_on(episodes::pull(&ctx, &lane, start_s, NOW_S + 3_600, 6));
    assert!(result.is_err(), "the failed page is reported");
    let cycle: Vec<_> = server.hits().into_iter().skip(hits).collect();
    assert_eq!(cycle[0].param("cursor"), None, "the head comes first");
    assert_eq!(
        cycle[0].param("from"),
        Some((newest - episodes::HEAD_OVERLAP_S).to_string())
    );
    assert_eq!(cycle[1].param("cursor").as_deref(), Some("o90"));
    assert_eq!(cycle.len(), 4, "head, two catch-up pages, the failed page");
    let after_second = state(&ctx);
    assert_eq!(after_second.newest_opened_at, Some(NOW_S + 1_200));
    assert_eq!(after_second.pending.len(), 1);
    assert_eq!(after_second.pending[0].cursor.as_deref(), Some("o150"));
    // 90 + the two fresh ones + two catch-up pages of 30.
    assert_eq!(count(&ctx, &root), (152, 152));

    // Later cycles finish the catch-up from the stored cursor; nothing is lost or duplicated.
    for hour in 2..10 {
        runtime
            .block_on(episodes::pull(
                &ctx,
                &lane,
                start_s,
                NOW_S + hour * 3_600,
                4,
            ))
            .unwrap();
    }
    let done = state(&ctx);
    assert!(done.pending.is_empty());
    assert_eq!(count(&ctx, &root), (total + 2, total + 2));
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}
