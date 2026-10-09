//! The Elfa capture additions against an in-process HTTP server: the minute events poll with
//! its request budget and `received_at_ms`, and the Auto alerts lane: validate-then-create,
//! idempotence by title, the credit budget, renewal with cancellation of the old query, and
//! notifications from the server-sent event stream recorded with the instant of receipt.

mod augment_common;

use augment_common::{Response, Server, tempdir};
use serde_json::{Value, json};
use solos_data::augment::auto::{self, AutoLane};
use solos_data::augment::config::{AlertDef, ElfaAuto};
use solos_data::augment::elfa::{self, ElfaLane};
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::series::Ctx;
use solos_data::db::Db;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;

fn ctx(server: &Server, root: &std::path::Path) -> (Arc<Ctx>, solos_data::db::DbThread) {
    let mut store = ledger::open(root, Lane::Capture).unwrap();
    ledger::recover(&mut store, root, Lane::Capture).unwrap();
    let (db, thread) = Db::spawn(store);
    let http = Http::new(50.0).unwrap();
    http.set_host_rate(&server.base, 50.0);
    (
        Arc::new(Ctx {
            http,
            db,
            root: root.to_path_buf(),
            lane: Lane::Capture,
            start: solos_data::augment::periods::parse_date("2026-10-01").unwrap(),
            now_ms: NOW_MS,
            stop: Arc::new(AtomicBool::new(false)),
            disk_budget_bytes: 0,
        }),
        thread,
    )
}

fn rows(root: &std::path::Path, sql: &str) -> Vec<Value> {
    solos_data::augment::query::query_augment(root, sql)
        .unwrap()
        .to_value()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

fn catalog(ctx: &Ctx) {
    let root = ctx.root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root, Lane::Capture))
        .unwrap();
}

#[test]
fn the_minute_poll_takes_two_pages_and_records_receipt() {
    let server = Server::start();
    let now_s = NOW_MS / 1000;
    server.route("/v3/events", move |request| {
        let from: i64 = request.param("from").unwrap().parse().unwrap();
        assert_eq!(request.param("order").as_deref(), Some("asc"));
        let page = request.param("cursor").map_or(0, |c| c.len());
        let events: Vec<Value> = (0..30)
            .map(|i| json!({ "id": format!("e{page}-{i}"), "firstSeenAt": from + page as i64 * 30 + i, "eventClass": "news" }))
            .collect();
        Response::json(&json!({ "events": events, "nextCursor": format!("{}c", "c".repeat(page)), "hasMore": true }))
    });
    let root = tempdir("solos-alerts");
    let (ctx, thread) = ctx(&server, &root);
    let mut lane = ElfaLane::new(&server.base, "test-key", now_s - 86_400);
    lane.events_pages = 2;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let outcome = runtime.block_on(elfa::poll_events(&ctx, &lane, now_s));
    assert_eq!(outcome.errors, 0);
    assert_eq!(outcome.requests, 2, "the poll's request budget");
    assert_eq!(outcome.rows, 60);
    assert_eq!(server.hits_of("/v3/events"), 2);
    assert_eq!(
        server.hits_of("/v3/key-status"),
        0,
        "no credit guard on a free poll"
    );
    assert_eq!(lane.poll.polls.load(Ordering::Relaxed), 1);
    assert_eq!(lane.poll.requests.load(Ordering::Relaxed), 2);
    // The second poll resumes one second before the newest event received.
    let second = runtime.block_on(elfa::poll_events(&ctx, &lane, now_s + 60));
    assert_eq!(second.requests, 2);
    let resumed: i64 = server
        .hits()
        .into_iter()
        .filter(|h| h.path == "/v3/events")
        .nth(2)
        .and_then(|h| h.param("from"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(resumed, now_s - 86_400 + 59);
    catalog(&ctx);
    let out = rows(
        &root,
        "SELECT count(*) AS n, count(received_at_ms) AS received, min(received_at_ms) > 1700000000000 AS recent FROM elfa_events",
    );
    assert_eq!(out[0]["n"], out[0]["received"]);
    assert_eq!(out[0]["recent"], true);
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

/// A fake Auto API: queries live in `state`, creations cost five credits, listings one.
struct FakeAuto {
    used: Arc<AtomicI64>,
    queries: Arc<Mutex<Vec<Value>>>,
}

fn mount_auto(server: &Server) -> FakeAuto {
    let used = Arc::new(AtomicI64::new(204));
    let queries: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let (u, q) = (Arc::clone(&used), Arc::clone(&queries));
    server.route("/v3/key-status", move |_| {
        Response::json(
            &json!({ "credits": { "used": u.load(Ordering::SeqCst) }, "historyFrom": 1 }),
        )
    });
    server.route("/v2/auto/queries/validate", |request| {
        let body = request.json();
        assert_eq!(body["query"]["actions"][0]["type"], "notify");
        assert_eq!(body["query"]["expiresIn"], "720h");
        Response::json(&json!({ "valid": true, "errors": [], "estimatedCost": { "credits": 5 } }))
    });
    let (u, q2) = (Arc::clone(&used), Arc::clone(&q));
    server.route("/v2/auto/queries", move |request| {
        if request.method == "GET" {
            u.fetch_add(1, Ordering::SeqCst);
            return Response::json(&json!({ "queries": q2.lock().unwrap().clone(), "total": 1 }))
                .with_header("x-elfa-credits", "1");
        }
        let body = request.json();
        u.fetch_add(5, Ordering::SeqCst);
        let id = format!("q-{}", q2.lock().unwrap().len() + 1);
        q2.lock().unwrap().push(json!({ "id": id, "title": body["title"], "status": "active", "createdAt": "2026-10-09T12:00:00.000Z" }));
        Response {
            status: 201,
            headers: vec![("x-elfa-credits".into(), "5".into())],
            body: json!({ "queryId": id, "status": "active", "estimatedCredits": 5 }).to_string().into_bytes(),
        }
    });
    for id in ["q-1", "q-2"] {
        let q3 = Arc::clone(&q);
        server.route(&format!("/v2/auto/queries/{id}/cancel"), move |_| {
            for query in q3.lock().unwrap().iter_mut() {
                if query["id"] == id {
                    query["status"] = json!("cancelled");
                }
            }
            Response::json(&json!({ "id": id, "status": "cancelled" }))
        });
    }
    FakeAuto { used, queries }
}

fn alert(title: &str) -> AlertDef {
    AlertDef {
        title: title.into(),
        description: "d".into(),
        conditions: json!({ "AND": [{ "source": "funding", "method": "annualized_rate", "args": { "ticker": "SOL:HYPERLIQUID" }, "operator": "crosses_above", "value": 50 }] }),
        repeat: Some(json!({ "cooldown": "1h", "maxTriggers": 5 })),
    }
}

#[test]
fn alerts_are_created_once_renewed_near_expiry_and_bounded_by_the_budget() {
    let server = Server::start();
    let fake = mount_auto(&server);
    let root = tempdir("solos-alerts");
    let (ctx, thread) = ctx(&server, &root);
    let cfg = ElfaAuto {
        enabled: true,
        max_alerts: 8,
        credit_budget_per_month: 12,
        expires_in: "720h".into(),
        renew_within_hours: 48,
        alerts: vec![alert("A"), alert("B"), alert("C")],
    };
    let billing = Arc::new(tokio::sync::Mutex::new(()));
    let lane = AutoLane::new(&server.base, "test-key", cfg, billing);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(auto::reconcile(&ctx, &lane, NOW_MS))
        .unwrap();
    // Two creations fit a budget of twelve (listing: 1, creations: 5 each); C waits.
    let active = lane.active();
    assert_eq!(active.len(), 2);
    assert_eq!(active[0].title, "A");
    assert_eq!(active[0].expires_at_ms, NOW_MS + 720 * 3_600_000);
    assert_eq!(server.hits_of("/v2/auto/queries/validate"), 2);
    assert_eq!(fake.used.load(Ordering::SeqCst), 204 + 1 + 10);
    assert_eq!(lane.counters.spent_month.load(Ordering::Relaxed), 11);
    // A second reconciliation changes nothing but the listing's credit.
    runtime
        .block_on(auto::reconcile(&ctx, &lane, NOW_MS + 60_000))
        .unwrap();
    assert_eq!(lane.active().len(), 2);
    assert_eq!(lane.counters.created.load(Ordering::Relaxed), 2);
    assert_eq!(fake.used.load(Ordering::SeqCst), 204 + 2 + 10);
    // A restart with a wider budget sees the stored records: 29 days later (a new month for
    // the budget) A and B are within 48 h of expiry, so both are recreated and their old
    // queries cancelled, and C is finally created.
    let mut wide = lane.cfg.clone();
    wide.credit_budget_per_month = 60;
    let later = AutoLane::new(&server.base, "test-key", wide, Arc::clone(&lane.billing));
    let day29 = NOW_MS + 29 * 86_400_000;
    runtime
        .block_on(auto::reconcile(&ctx, &later, day29))
        .unwrap();
    let mut titles: Vec<String> = later.active().iter().map(|q| q.title.clone()).collect();
    titles.sort();
    assert_eq!(titles, ["A", "B", "C"]);
    assert_eq!(later.counters.created.load(Ordering::Relaxed), 3);
    assert_eq!(later.counters.renewed.load(Ordering::Relaxed), 2);
    assert_eq!(later.counters.cancelled.load(Ordering::Relaxed), 2);
    assert_eq!(server.hits_of("/v2/auto/queries/q-1/cancel"), 1);
    assert_eq!(server.hits_of("/v2/auto/queries/q-2/cancel"), 1);
    assert_eq!(
        fake.queries
            .lock()
            .unwrap()
            .iter()
            .filter(|q| q["status"] == "active")
            .count(),
        3
    );
    // The listing plus three creations, counted from zero in the new month.
    assert_eq!(later.counters.spent_month.load(Ordering::Relaxed), 16);
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_stream_records_notifications_with_receipt_and_handles_the_end() {
    let server = Server::start();
    let _fake = mount_auto(&server);
    let connections = Arc::new(AtomicI64::new(0));
    let seen = Arc::clone(&connections);
    server.route_raw("/v2/auto/queries/stream", move |request, stream| {
        assert_eq!(request.header("x-elfa-api-key"), Some("test-key"));
        let n = seen.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(b": keep-alive\n\n");
            let _ = stream.flush();
            std::thread::sleep(std::time::Duration::from_millis(200));
            let _ = stream.write_all(b"id: evt-1\nevent: notification\ndata: {\"status\":\"triggered\",\"title\":\"Plan Triggered\",\"body\":\"SOL funding\",\"queryId\":\"q-1\",\"executionId\":\"x1\",\"triggerTime\":\"2026-10-09T12:00:00.000Z\",\"conditionsMet\":1,\"timestamp\":1791547200000}\n\n");
            let _ = stream.write_all(b"id: evt-1\nevent: notification\ndata: {\"status\":\"triggered\",\"queryId\":\"q-1\"}\n\n");
            let _ = stream.write_all(b"id: evt-2\nevent: notification\ndata: {\"status\":\"update\",\"title\":\"Plan Updated\",\"queryId\":\"q-1\"}\n\n");
            let _ = stream.write_all(b"event: end\ndata: {\"code\":\"USER_STREAM_CLOSED\"}\n\n");
        } else {
            let _ = stream.write_all(b"HTTP/1.1 410 Gone\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
        let _ = stream.flush();
    });
    let root = tempdir("solos-alerts");
    let (ctx, thread) = ctx(&server, &root);
    let cfg = ElfaAuto {
        enabled: true,
        alerts: vec![alert("SOL funding")],
        ..ElfaAuto::default()
    };
    let lane = Arc::new(AutoLane::new(
        &server.base,
        "test-key",
        cfg,
        Arc::new(tokio::sync::Mutex::new(())),
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(auto::reconcile(&ctx, &lane, NOW_MS))
        .unwrap();
    assert_eq!(lane.active()[0].id, "q-1");
    let stop = Arc::clone(&ctx.stop);
    runtime.block_on(async {
        let streaming = auto::stream_loop(&ctx, &lane);
        let stopper = async {
            // The stream ends after its frames; the second connection answers 410; stop then.
            while connections.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            stop.store(true, Ordering::SeqCst);
        };
        tokio::join!(streaming, stopper);
    });
    assert_eq!(lane.counters.fired.load(Ordering::Relaxed), 3);
    assert_eq!(lane.counters.connections.load(Ordering::Relaxed), 1);
    assert!(!lane.counters.connected.load(Ordering::Relaxed));
    catalog(&ctx);
    let out = rows(
        &root,
        "SELECT event_id, query_id, query_title, status, title, execution_id, conditions_met, received_at_ms > 1700000000000 AS recent FROM elfa_auto_events ORDER BY event_id",
    );
    assert_eq!(out.len(), 2, "the duplicate frame merged on its id");
    assert_eq!(out[0]["event_id"], "evt-1");
    assert_eq!(out[0]["query_title"], "SOL funding");
    assert_eq!(
        out[0]["execution_id"],
        Value::Null,
        "the later frame for evt-1 won the merge"
    );
    assert_eq!(out[1]["status"], "update");
    assert_eq!(out[0]["recent"], true);
    let status = lane.status().to_value();
    assert_eq!(status["fired"], 3);
    assert_eq!(status["titles"], json!(["SOL funding"]));
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}
