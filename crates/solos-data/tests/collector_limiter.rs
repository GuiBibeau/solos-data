//! Ported from `tests/limiter.test.ts` and `tests/rpc.test.ts`.

mod collector_common;

use solos_data::collector::config::{Lane, Lanes, load_config, repository_config_path};
use solos_data::collector::limiter::Limiter;
use solos_data::collector::rpc::{HttpReply, Provider, Rpc, Transport};
use solos_data::jsonout::safe_error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn idle_backfill_borrowing_cannot_consume_reserved_tail_tokens_when_tail_is_waiting() {
    let limiter = Arc::new(Limiter::new(
        400.0,
        0.7,
        Lanes {
            tail: 2,
            backfill: 2,
        },
    ));
    {
        let mut s = limiter.state.lock().unwrap();
        s.tokens = 100.0;
        s.tail_tokens = 80.0;
        s.backfill_tokens = 0.0;
        s.waiting_tail = 1;
        s.last_tail = Some(Instant::now());
        s.last = Instant::now() + Duration::from_secs(3600);
    }
    let shutdown = Arc::new(AtomicBool::new(false));
    let granted = Arc::new(AtomicBool::new(false));
    let (limiter2, shutdown2, granted2) = (
        Arc::clone(&limiter),
        Arc::clone(&shutdown),
        Arc::clone(&granted),
    );
    let backfill = tokio::spawn(async move {
        limiter2
            .acquire(Lane::Backfill, 40, &shutdown2)
            .await
            .unwrap();
        granted2.store(true, Ordering::Relaxed);
    });
    limiter.acquire(Lane::Tail, 40, &shutdown).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!granted.load(Ordering::Relaxed));
    assert_eq!(limiter.state.lock().unwrap().active.tail, 1);
    limiter.release(Lane::Tail, false, 0);
    {
        let mut s = limiter.state.lock().unwrap();
        s.waiting_tail = 0;
        s.backfill_tokens = 40.0;
        s.tokens = 40.0;
    }
    backfill.await.unwrap();
    limiter.release(Lane::Backfill, false, 0);
}

#[test]
fn a_429_reduces_throughput_and_concurrency_and_honors_retry_pause() {
    let limiter = Limiter::new(
        400.0,
        0.3,
        Lanes {
            tail: 8,
            backfill: 4,
        },
    );
    limiter.state.lock().unwrap().active.tail = 1;
    limiter.release(Lane::Tail, true, 2500);
    let s = limiter.state.lock().unwrap();
    assert!((s.rate - 280.0).abs() < 1e-9);
    assert_eq!(s.windows.tail, 4);
    assert!(s.cooldown_until.unwrap().duration_since(Instant::now()) > Duration::from_millis(2400));
    assert!(!safe_error("provider https://example.com/v2/secret failed").contains("secret"));
}

#[test]
fn an_idle_paid_plan_bucket_cannot_release_more_than_a_100ms_burst() {
    let limiter = Limiter::new(
        8000.0,
        0.2,
        Lanes {
            tail: 24,
            backfill: 32,
        },
    );
    limiter.state.lock().unwrap().last = Instant::now() - Duration::from_secs(10);
    limiter.refill();
    let s = limiter.state.lock().unwrap();
    assert!((s.tokens - 800.0).abs() < 1e-9);
    assert!((s.tail_tokens - 800.0).abs() < 1e-9);
    assert!((s.backfill_tokens - 800.0).abs() < 1e-9);
}

struct FakeTransport {
    body: String,
    calls: AtomicUsize,
}

impl Transport for FakeTransport {
    fn post<'a>(
        &'a self,
        _url: &'a str,
        _body: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<HttpReply, String>> + Send + 'a>>
    {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(HttpReply {
                status: 200,
                retry_after: None,
                body: self.body.clone(),
            })
        })
    }
}

fn provider(body: &str, max_retries: u32) -> (Arc<Provider>, Arc<Limiter>) {
    let mut config = load_config(Some(repository_config_path().to_str().unwrap())).unwrap();
    config.max_retries = max_retries;
    let limiter = Arc::new(Limiter::new(700.0, 0.7, config.concurrency.clone()));
    {
        let mut s = limiter.state.lock().unwrap();
        s.tokens = 700.0;
        s.tail_tokens = 700.0;
    }
    let transport = Box::new(FakeTransport {
        body: body.into(),
        calls: AtomicUsize::new(0),
    });
    (
        Arc::new(Provider::with_transport(
            "https://rpc.invalid/v2/secret".into(),
            config,
            Arc::clone(&limiter),
            transport,
        )),
        limiter,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unhealthy_solana_node_is_retriable_but_is_not_a_throughput_throttle() {
    let (provider, limiter) = provider(
        r#"{"error":{"code":-32005,"message":"Node is behind by 100 slots"}}"#,
        1,
    );
    let error = provider
        .call(
            "getSlot",
            serde_json::json!([{ "commitment": "finalized" }]),
            Lane::Tail,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, -32005);
    assert!((limiter.rate() - 700.0).abs() < 1e-9);
    let counters = provider.counters();
    assert_eq!(counters.int("throttled").unwrap_or(0), 0);
    assert_eq!(counters.int("error_code_-32005"), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_transaction_versions_fail_without_useless_retries_or_provider_message_disclosure()
 {
    let (provider, _) = provider(
        r#"{"error":{"code":-32015,"message":"provider https://rpc.invalid/v2/secret says version unsupported"}}"#,
        6,
    );
    let error = provider
        .call(
            "getTransaction",
            serde_json::json!(["sig", { "maxSupportedTransactionVersion": 0 }]),
            Lane::Tail,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, -32015);
    assert!(!error.to_string().contains("secret"));
    assert_eq!(provider.counters().int("requests"), Some(1));
}
