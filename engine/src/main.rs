//! Whal Bot Trading Engine binary entry point (`whal-engine`).
//!
//! Wires together:
//! - Step 2: `whal-config` (typed config + hot reload)
//! - Step 3: `/internal/engine` WebSocket bridge (snapshot, live telemetry, control-plane commands)
//! - Step 7: `ingest` (ShredStream + Geyser + SignalDetector + OpportunityBus)
//! - Step 8: `executor` (TxBuilder + Ed25519 Wire Signer + NoncePool + Multi-Lander SWQoS Fan-Out)
//! - Step 9: `exits` (ExitEngine + Lighthouse) & `treasury` (Jupiter V6 / ZeroFi WSOL -> USDC Sweeper)
//! - Step 10: `safety` (Hard Circuit Breakers + Shadow Mode + NonceHealthMonitor) & `metrics` (Prometheus `/metrics` + `/health`)

use std::env;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, info, warn};

use whal_engine::executor::{
    builder::{build_venue_cpi_accounts, SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID},
    ExecutorRuntimeConfig, ExecutorService, ExecutorTelemetryEvent, HotWalletSigner, LanderConfig,
    LanderId, NonceAccount, NoncePool, ProgramIds, TxVersion, V1TransactionConfig,
};
use whal_engine::exits::{ExitEngine, ExitPolicy, LadderStep, LadderTrigger, SellTxParams};
use whal_engine::ingest::{
    types::{compute_opportunity_key, pubkey_from_bs58, StrategyId, TriggerEvent, TriggerKind, WSOL_MINT},
    GeyserConfig, IngestEngine, KolRule, LaunchFilterConfig, OpportunityBus, PubkeyBytes,
    ShredStreamConfig, SignalDetector, Venue, WatchlistCache,
};
use whal_engine::metrics::EngineMetrics;
use whal_engine::safety::{HardSafetyLimits, SafetyCircuitBreakers};
use whal_engine::treasury::{SweepRecord, SweeperConfig, TreasurySweeper};

#[derive(Debug, Clone)]
struct CliArgs {
    config_path: Option<String>,
    metrics_bind: String,
    bridge_url: String,
    bridge_token: String,
}

fn parse_args() -> CliArgs {
    let mut config_path = env::var("WHAL_CONFIG_PATH").ok();
    let default_port = env::var("PORT").unwrap_or_else(|_| "9100".into());
    let mut metrics_bind = env::var("WHAL_METRICS_BIND")
        .unwrap_or_else(|_| format!("0.0.0.0:{default_port}"));
    let mut bridge_url = env::var("WHAL_BRIDGE_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:8787/internal/engine".into());
    let bridge_token = env::var("WHAL_ENGINE_BRIDGE_TOKEN").unwrap_or_default();

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = it.next(),
            "--metrics-bind" => {
                if let Some(v) = it.next() {
                    metrics_bind = v;
                }
            }
            "--bridge-url" => {
                if let Some(v) = it.next() {
                    bridge_url = v;
                }
            }
            _ => {}
        }
    }

    CliArgs {
        config_path,
        metrics_bind,
        bridge_url,
        bridge_token,
    }
}

fn env_pubkey(name: &str) -> PubkeyBytes {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => match pubkey_from_bs58(raw.trim()) {
            Ok(pk) => pk,
            Err(e) => {
                warn!(var = name, error = %e, "Invalid base58 pubkey in env; defaulting to zero");
                [0u8; 32]
            }
        },
        _ => [0u8; 32],
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Minimal zero-dependency HTTP server exposing Prometheus `/metrics` and Railway `/health`.
async fn run_metrics_server(bind: String, metrics: Arc<EngineMetrics>) {
    let listener = match TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            error!(bind = %bind, error = %e, "Failed to bind Prometheus metrics server");
            return;
        }
    };
    info!(bind = %bind, "Prometheus metrics & health server listening");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let (status, content_type, body) = if req.starts_with("GET /health") {
                ("200 OK", "application/json", "{\"ok\":true}\n".to_string())
            } else if req.starts_with("GET /metrics") {
                (
                    "200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    metrics.render_prometheus(),
                )
            } else {
                ("404 Not Found", "text/plain", "not found\n".to_string())
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}

/// Parses a `config.apply` JSON payload from Step 3 and hot-reloads `ExecutorService` + `NoncePool`.
fn apply_bridge_config(
    cmd: &Value,
    executor: &ExecutorService,
    rpc_url: &str,
    default_hot_wallet: PubkeyBytes,
) {
    let cfg = cmd.get("config").unwrap_or(cmd);
    let hot_wallet = cfg
        .pointer("/wallets/hot_wallet_pubkey")
        .and_then(|v| v.as_str())
        .and_then(|s| pubkey_from_bs58(s).ok())
        .unwrap_or(default_hot_wallet);

    // Nonce pool — initialize `ready: false` until live RPC populates each 32-byte durable blockhash.
    if let Some(arr) = cfg.pointer("/wallets/nonce_accounts").and_then(|v| v.as_array()) {
        let nonces: Vec<NonceAccount> = arr
            .iter()
            .filter_map(|v| v.as_str())
            .filter_map(|s| pubkey_from_bs58(s).ok())
            .map(|pubkey| NonceAccount {
                pubkey,
                nonce_hash: [0u8; 32],
                ready: false,
            })
            .collect();
        if !nonces.is_empty() {
            let count = nonces.len();
            let pool = executor.nonce_pool();
            pool.replace(nonces);
            let rpc = rpc_url.to_string();
            tokio::spawn(async move {
                let http = reqwest::Client::new();
                let _ = pool.refresh_from_rpc(&http, &rpc).await;
            });
            info!(count, "Updated durable nonce pool from control-plane config (refreshing hashes)");
        }
    }

    // Parse all 9 SWQoS landers
    let lander_defs: [(&str, LanderId); 9] = [
        ("jito", LanderId::Jito),
        ("nozomi", LanderId::Nozomi),
        ("stellium", LanderId::Stellium),
        ("landx", LanderId::Landx),
        ("bloom", LanderId::Bloom),
        ("astralane", LanderId::Astralane),
        ("glaive", LanderId::Glaive),
        ("fastlanes", LanderId::Fastlanes),
        ("tpu", LanderId::Tpu),
    ];

    let mut landers = Vec::new();
    for (key, id) in lander_defs {
        if let Some(l) = cfg
            .pointer(&format!("/landers/providers/{key}"))
            .or_else(|| cfg.pointer(&format!("/landers/{key}")))
        {
            let enabled = l.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
            let endpoint_url = l
                .get("endpoint_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let auth_token = l
                .get("auth_token")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned);
            let tip_accounts: Vec<PubkeyBytes> = l
                .get("tip_accounts")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str())
                        .filter_map(|s| pubkey_from_bs58(s).ok())
                        .collect()
                })
                .unwrap_or_default();
            let min_tip_lamports = l
                .get("min_tip_sol")
                .and_then(|v| v.as_f64())
                .map(|s| (s * 1_000_000_000.0).max(0.0) as u64)
                .or_else(|| l.get("min_tip_lamports").and_then(|v| v.as_u64()))
                .unwrap_or(100_000);
            let max_tip_lamports = l
                .get("max_tip_sol")
                .and_then(|v| v.as_f64())
                .map(|s| (s * 1_000_000_000.0).max(0.0) as u64)
                .or_else(|| l.get("max_tip_lamports").and_then(|v| v.as_u64()))
                .unwrap_or(5_000_000);
            let fanout = l.get("fanout").and_then(|v| v.as_u64()).unwrap_or(1) as usize;

            landers.push(LanderConfig {
                id,
                enabled,
                endpoint_url,
                auth_token,
                tip_accounts,
                min_tip_lamports,
                max_tip_lamports,
                fanout,
            });
        }
    }

    let tx_version = match cfg
        .pointer("/landers/tx/version")
        .or_else(|| cfg.pointer("/execution/tx_version"))
        .and_then(|v| v.as_str())
    {
        Some("v0") => TxVersion::V0,
        _ => TxVersion::V1,
    };
    let buy_slot_ttl = cfg
        .pointer("/strategy/buy_slot_ttl/max")
        .or_else(|| cfg.pointer("/execution/buy_slot_ttl"))
        .and_then(|v| v.as_u64())
        .unwrap_or(4);
    let slippage_bps = cfg
        .pointer("/strategy/slippage_bps")
        .and_then(|v| v.as_u64())
        .unwrap_or(1800)
        .clamp(800, 3500);
    let cu_limit = cfg
        .pointer("/landers/tx/compute_unit_limit")
        .or_else(|| cfg.pointer("/execution/compute_unit_limit"))
        .and_then(|v| v.as_u64())
        .unwrap_or(200_000) as u32;
    let loaded_accounts_limit = cfg
        .pointer("/landers/tx/loaded_accounts_data_size_limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(256 * 1024) as u32;
    let priority_fee_lamports = cfg
        .pointer("/landers/tx/priority_fee_lamports")
        .or_else(|| cfg.pointer("/execution/priority_fee_lamports"))
        .and_then(|v| v.as_u64())
        .unwrap_or(100_000);

    let enabled_count = landers.iter().filter(|l| l.enabled).count();
    executor.hot_reload(ExecutorRuntimeConfig {
        hot_wallet,
        tx_version,
        v1_config: V1TransactionConfig {
            compute_unit_limit: cu_limit,
            loaded_accounts_data_size_limit: loaded_accounts_limit,
            priority_fee_lamports,
        },
        lookup_tables: vec![],
        buy_slot_ttl,
        slippage_bps,
        landers,
    });
    info!(enabled_landers = enabled_count, buy_slot_ttl, slippage_bps, "Hot-reloaded ExecutorRuntimeConfig from bridge");
}

fn apply_bridge_exits(cmd: &Value, exits: &ExitEngine) {
    let cfg = cmd.get("config").unwrap_or(cmd);
    let Some(ex) = cfg.get("exits") else { return };
    let mut policy = ExitPolicy::default();
    if let Some(v) = ex.get("instant_tp_pct").and_then(|v| v.as_u64()) {
        policy.instant_tp_pct = v.min(100);
        policy.same_slot_take_profit_pct = v.min(100);
        policy.same_slot_take_profit_enabled = v > 0;
    }
    if let Some(v) = ex.get("stop_loss_pct").and_then(|v| v.as_f64()) {
        policy.stop_loss_pct = v.clamp(0.0, 99.0);
    }
    if let Some(ts) = ex.get("trailing_stop_pct") {
        if ts.is_null() {
            policy.trailing_stop_pct = 0.0;
        } else if let Some(v) = ts.as_f64() {
            policy.trailing_stop_pct = v.clamp(0.0, 95.0);
        }
    }
    if let Some(cf) = ex.get("curve_floor") {
        if let Some(en) = cf.get("enabled").and_then(|v| v.as_bool()) {
            policy.curve_floor_enabled = en;
        }
        if let Some(sol) = cf.get("min_real_sol_reserves_sol").and_then(|v| v.as_f64()) {
            policy.min_liquidity_floor_lamports = (sol.max(0.0) * 1_000_000_000.0) as u64;
        }
    }
    if let Some(arr) = ex.get("ladder").and_then(|v| v.as_array()) {
        let mut steps = Vec::new();
        for item in arr {
            let sell_pct = item.get("sell_pct").and_then(|v| v.as_u64()).unwrap_or(0).min(100);
            let trig = item.get("trigger");
            let kind = trig.and_then(|t| t.get("kind")).and_then(|v| v.as_str()).unwrap_or("");
            let val = trig.and_then(|t| t.get("value")).and_then(|v| v.as_f64()).unwrap_or(0.0);
            if sell_pct > 0 && val > 0.0 {
                let trigger = match kind {
                    "time_secs" => LadderTrigger::TimeSecs(val as u64),
                    _ => LadderTrigger::PnlMultiple(val),
                };
                steps.push(LadderStep { sell_pct, trigger });
            }
        }
        if !steps.is_empty() {
            policy.ladder = steps;
        }
    }
    info!(
        instant_tp_pct = policy.instant_tp_pct,
        stop_loss_pct = policy.stop_loss_pct,
        trailing_stop_pct = policy.trailing_stop_pct,
        ladder_steps = policy.ladder.len(),
        "Hot-reloaded ExitPolicy from bridge (1:1 Paper Mode parity)"
    );
    exits.update_policy(policy);
}

fn apply_bridge_launch_filter(cmd: &Value, detector: &SignalDetector) {
    let cfg = cmd.get("config").unwrap_or(cmd);
    let mut filter = LaunchFilterConfig::default();
    if let Some(en) = cfg.pointer("/strategy/modes/pumpfun_create_snipe").and_then(|v| v.as_bool()) {
        filter.enabled = en;
    }
    if let Some(sol) = cfg.pointer("/strategy/default_buy_sol").and_then(|v| v.as_f64()) {
        filter.default_buy_lamports = (sol.clamp(0.01, 50.0) * 1_000_000_000.0) as u64;
    }
    info!(
        enabled = filter.enabled,
        default_buy_lamports = filter.default_buy_lamports,
        "Hot-reloaded LaunchFilterConfig for LocalShred from bridge"
    );
    detector.update_launch_filter(filter);
}

fn apply_kol_upsert(cmd: &Value, watchlist: &WatchlistCache) -> Result<(), String> {
    let k = cmd.get("kol").unwrap_or(cmd);
    let id = k.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let label = k.get("label").and_then(|v| v.as_str()).unwrap_or(&id).to_string();
    let pubkey_str = k.get("pubkey").and_then(|v| v.as_str()).ok_or("missing kol.pubkey")?;
    let pubkey = pubkey_from_bs58(pubkey_str).map_err(|e| e.to_string())?;
    let quote = k.get("quote").and_then(|v| v.as_str()).unwrap_or("SOL");
    let is_spl = quote != "SOL";
    let scale = match quote {
        "USDC" => 1_000_000.0,
        "BONK" => 100_000.0,
        _ => 1_000_000_000.0,
    };
    let trigger_margin = k.get("triggerMargin").and_then(|v| v.as_f64()).unwrap_or(0.02);
    let buy_size_sol = k.get("buySizeSol").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let max_curve = k.get("maxCurveReserveSol").and_then(|v| v.as_f64()).unwrap_or(85.0);
    let enabled = k.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);

    watchlist.upsert(KolRule {
        id: id.clone(),
        label: label.clone(),
        pubkey,
        quote_token_account: None,
        is_spl_quote: is_spl,
        margin_lamports: (trigger_margin * scale).max(1.0) as u64,
        buy_size_lamports: (buy_size_sol * 1_000_000_000.0) as u64,
        max_curve_reserve_lamports: (max_curve * 1_000_000_000.0) as u64,
        enabled,
    });
    if enabled {
        info!(
            kol = %label,
            pubkey = %pubkey_str,
            buy_size_sol,
            max_curve,
            watched_total = watchlist.watched_accounts().len(),
            "Synced enabled KOL rule into LocalShred WatchlistCache"
        );
    }
    Ok(())
}

struct BridgeContext {
    engine_status: Arc<RwLock<String>>,
    executor: Arc<ExecutorService>,
    exit_engine: Arc<ExitEngine>,
    breakers: Arc<SafetyCircuitBreakers>,
    watchlist: WatchlistCache,
    detector: Arc<SignalDetector>,
    shredstream: Arc<whal_engine::ingest::ShredStreamIngestor>,
    bus: OpportunityBus,
    rpc_url: String,
    default_hot_wallet: PubkeyBytes,
    outbound_tx: broadcast::Sender<Value>,
}

/// Connects to Step 3's `/internal/engine` WebSocket bridge, pushes an initial clean snapshot,
/// forwards live engine events, and handles all control-plane commands.
async fn run_bridge_loop(url: String, token: String, ctx: Arc<BridgeContext>) {
    if token.len() < 32 {
        warn!("WHAL_ENGINE_BRIDGE_TOKEN not set (<32 chars); bridge connection disabled");
        return;
    }

    let mut backoff_ms = 500u64;
    loop {
        match connect_bridge_once(&url, &token, &ctx).await {
            Ok(()) => backoff_ms = 500,
            Err(e) => {
                warn!(error = %e, backoff_ms, "Control-plane bridge disconnected; reconnecting");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
        }
    }
}

async fn connect_bridge_once(
    url: &str,
    token: &str,
    ctx: &Arc<BridgeContext>,
) -> Result<(), String> {
    let mut req = url.into_client_request().map_err(|e| e.to_string())?;
    let auth_val = format!("Bearer {token}")
        .parse()
        .map_err(|e: tokio_tungstenite::tungstenite::http::header::InvalidHeaderValue| e.to_string())?;
    req.headers_mut().insert("Authorization", auth_val);

    let (ws, _) = connect_async(req).await.map_err(|e| e.to_string())?;
    info!(url = %url, "Connected to Step 3 control-plane bridge (/internal/engine)");
    let (mut sink, mut stream) = ws.split();

    let status = ctx
        .engine_status
        .read()
        .map(|s| s.clone())
        .unwrap_or_else(|_| "PAUSED".into());
    let started = now_ms();
    let nonce_count = ctx.executor.nonce_pool().len();
    let snapshot = json!({
        "type": "snapshot",
        "runtime": {
            "engine": { "status": status, "changedAt": started },
            "chain": { "slot": 0, "shredLatencyMs": 0, "nonces": { "ready": nonce_count, "total": nonce_count }, "ring": { "used": 0, "size": 64 } },
            "balances": { "sol": 0.0, "wsol": 0.0, "usdc": 0.0, "solUsd": 0.0 },
            "feed": [],
            "positions": [],
            "closed": [],
            "kols": ctx.watchlist.list_ids().into_iter().map(|id| json!({ "id": id })).collect::<Vec<_>>(),
            "landers": [],
            "sweeps": [],
            "inventory": [],
            "rent": { "accountsClosed": 0, "reclaimedSol": 0.0, "openSeededAccounts": 0, "rentPerAccountSol": 0.00203928 },
            "session": { "startedAt": started, "bursts": 0, "fills": 0, "txSent": 0, "realizedSol": 0.0 },
            "nonces": []
        }
    });
    sink.send(Message::Text(snapshot.to_string().into()))
        .await
        .map_err(|e| e.to_string())?;

    let mut outbound_rx = ctx.outbound_tx.subscribe();

    loop {
        tokio::select! {
            ev = outbound_rx.recv() => {
                if let Ok(payload) = ev {
                    sink.send(Message::Text(payload.to_string().into()))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            msg = stream.next() => {
                let Some(msg) = msg else { break; };
                match msg {
                    Ok(Message::Text(txt)) => {
                        let parsed: Value = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if parsed.get("type").and_then(|v| v.as_str()) != Some("command") {
                            continue;
                        }
                        let id = parsed.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let cmd = parsed.get("command").cloned().unwrap_or(Value::Null);
                        let cmd_type = cmd.get("type").and_then(|v| v.as_str()).unwrap_or("");

                        let (ok, err_msg) = match cmd_type {
                            "control" => {
                                let next = cmd.get("status").and_then(|v| v.as_str()).unwrap_or("PAUSED");
                                if ctx.breakers.is_tripped() && next == "RUNNING" {
                                    ctx.breakers.reset();
                                }
                                ctx.executor.set_shadow_mode(next == "SHADOW");
                                ctx.executor.set_paused(next != "RUNNING" && next != "SHADOW");
                                if let Ok(mut st) = ctx.engine_status.write() {
                                    *st = next.to_string();
                                }
                                let ev = json!({
                                    "type": "event",
                                    "event": { "type": "engine_status", "ts": now_ms(), "status": next, "source": "bridge" }
                                });
                                let _ = sink.send(Message::Text(ev.to_string().into())).await;
                                (true, None)
                            }
                            "kill" => {
                                let reason = cmd.get("reason").and_then(|v| v.as_str()).unwrap_or("Emergency kill");
                                ctx.executor.set_paused(true);
                                if let Ok(mut st) = ctx.engine_status.write() {
                                    *st = "KILLED".to_string();
                                }
                                let ev = json!({
                                    "type": "event",
                                    "event": { "type": "engine_status", "ts": now_ms(), "status": "KILLED", "reason": reason, "source": "bridge" }
                                });
                                let _ = sink.send(Message::Text(ev.to_string().into())).await;
                                (true, None)
                            }
                            "config.apply" => {
                                apply_bridge_config(&cmd, &ctx.executor, &ctx.rpc_url, ctx.default_hot_wallet);
                                apply_bridge_exits(&cmd, &ctx.exit_engine);
                                apply_bridge_launch_filter(&cmd, &ctx.detector);
                                (true, None)
                            }
                            "kol.upsert" => match apply_kol_upsert(&cmd, &ctx.watchlist) {
                                Ok(()) => (true, None),
                                Err(e) => (false, Some(e)),
                            },
                            "kol.remove" => {
                                if let Some(id) = cmd.get("id").and_then(|v| v.as_str()) {
                                    ctx.watchlist.remove_by_id(id);
                                }
                                if let Some(pk) = cmd.get("pubkey").and_then(|v| v.as_str()).and_then(|s| pubkey_from_bs58(s).ok()) {
                                    ctx.watchlist.remove_by_wallet(&pk);
                                }
                                (true, None)
                            }
                            "nonce" => {
                                let pool = ctx.executor.nonce_pool();
                                let rpc = ctx.rpc_url.clone();
                                tokio::spawn(async move {
                                    let http = reqwest::Client::new();
                                    let _ = pool.refresh_from_rpc(&http, &rpc).await;
                                });
                                (true, None)
                            }
                            "trigger.dispatch" => {
                                let cur_status = ctx
                                    .engine_status
                                    .read()
                                    .map(|s| s.clone())
                                    .unwrap_or_default();
                                if cur_status != "RUNNING" {
                                    (false, Some("Engine is not RUNNING".into()))
                                } else if let Some(t) = cmd.get("trigger") {
                                    let mint_s = t.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                                    let pool_s = t.get("poolPda").and_then(|v| v.as_str()).unwrap_or(mint_s);
                                    match (pubkey_from_bs58(mint_s), pubkey_from_bs58(pool_s)) {
                                        (Ok(mint), Ok(pool_pda)) => {
                                            let venue = match t.get("venue").and_then(|v| v.as_str()).unwrap_or("Pump.fun") {
                                                "PumpSwap" => Venue::PumpSwap,
                                                "Raydium LaunchLab" => Venue::RaydiumLaunchLab,
                                                "Raydium CPMM" => Venue::RaydiumCpmm,
                                                _ => Venue::PumpFun,
                                            };
                                            let raw_slot = t.get("slot").and_then(|v| v.as_u64()).unwrap_or(0);
                                            let slot = if raw_slot > 0 { raw_slot } else { ctx.shredstream.latest_slot() };
                                            let buy_size_sol = t.get("buySizeSol").and_then(|v| v.as_f64()).unwrap_or(0.1).clamp(0.01, 50.0);
                                            let max_curve_sol = t.get("maxCurveReserveSol").and_then(|v| v.as_f64()).unwrap_or(55.0);
                                            let dex_accounts: Vec<PubkeyBytes> = t
                                                .get("dexAccounts")
                                                .and_then(|v| v.as_array())
                                                .map(|arr| {
                                                    arr.iter()
                                                        .filter_map(|x| x.as_str().and_then(|s| pubkey_from_bs58(s).ok()))
                                                        .collect()
                                                })
                                                .unwrap_or_default();
                                            let kind_str = t.get("kind").and_then(|v| v.as_str()).unwrap_or("kol_buy");
                                            let (strategy_id, kind) = if kind_str == "kol_buy" {
                                                let kol_id = t.get("kolId").and_then(|v| v.as_str()).unwrap_or("kol").to_string();
                                                let kol_wallet = t
                                                    .get("kolWallet")
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| pubkey_from_bs58(s).ok())
                                                    .unwrap_or([0u8; 32]);
                                                (
                                                    StrategyId::KolBackrun,
                                                    TriggerKind::KolBuy {
                                                        kol_id,
                                                        kol_wallet,
                                                        watched_balance_account: kol_wallet,
                                                        is_spl_quote: false,
                                                        pre_balance: 0,
                                                        balance_threshold: 0,
                                                        estimated_kol_spend: (buy_size_sol * 1_000_000_000.0) as u64,
                                                        buy_size_lamports: (buy_size_sol * 1_000_000_000.0) as u64,
                                                        max_curve_reserve_lamports: (max_curve_sol * 1_000_000_000.0) as u64,
                                                    },
                                                )
                                            } else {
                                                let creator = t
                                                    .get("creator")
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| pubkey_from_bs58(s).ok())
                                                    .unwrap_or([0u8; 32]);
                                                let dev_buy_sol = t.get("devBuySol").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                (
                                                    StrategyId::PumpFunCreateV2Snipe,
                                                    TriggerKind::ZeroBlockLaunch {
                                                        creator,
                                                        dev_buy_lamports: (buy_size_sol * 1_000_000_000.0) as u64,
                                                        initial_dev_buy_lamports: (dev_buy_sol.max(0.0) * 1_000_000_000.0) as u64,
                                                        name: t.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                        symbol: t.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                        uri: t.get("uri").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                    },
                                                )
                                            };
                                            let opportunity_key = compute_opportunity_key(&mint, slot, strategy_id);
                                            let trigger = TriggerEvent {
                                                opportunity_key,
                                                strategy_id,
                                                trigger_slot: slot,
                                                detected_at_us: now_ms() * 1_000,
                                                venue,
                                                mint,
                                                quote_mint: WSOL_MINT,
                                                pool_pda,
                                                dex_accounts,
                                                kind,
                                            };
                                            info!(mint = %mint_s, kind = %kind_str, slot, "Received bridge trigger.dispatch -> publishing to OpportunityBus");
                                            ctx.bus.publish(trigger);
                                            (true, None)
                                        }
                                        _ => (false, Some("Invalid mint or poolPda".into())),
                                    }
                                } else {
                                    (false, Some("Missing trigger payload".into()))
                                }
                            }
                            "sell" => {
                                let mint_str = cmd.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                                let pct = cmd.get("pct").and_then(|v| v.as_u64()).unwrap_or(100).clamp(1, 100);
                                let close_on_full = cmd.get("closeAccount").and_then(|v| v.as_bool()).unwrap_or(true);
                                match pubkey_from_bs58(mint_str) {
                                    Ok(mint_pk) => match ctx.exit_engine.get_position(&mint_pk) {
                                        Some(pos) => {
                                            let progs = ctx.executor.programs();
                                            let sell_router_program = progs.sell_router_program;
                                            let target_dex = match pos.venue {
                                                Venue::PumpFun => progs.pumpfun_program,
                                                Venue::PumpSwap => progs.pumpswap_program,
                                                Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => progs.launchlab_program,
                                            };
                                            let token_prog = if pos.is_token_2022 {
                                                TOKEN_2022_PROGRAM_ID
                                            } else {
                                                SPL_TOKEN_PROGRAM_ID
                                            };
                                            let hot_wallet = ctx.executor.hot_wallet();
                                            let cpi_accounts = build_venue_cpi_accounts(
                                                pos.venue,
                                                &pos.dex_accounts,
                                                hot_wallet,
                                                pos.bot_token_account,
                                                None,
                                                token_prog,
                                            );
                                            let exec = ctx.executor.clone();
                                            let exits = ctx.exit_engine.clone();
                                            let shred = ctx.shredstream.clone();
                                            let rpc_url = ctx.rpc_url.clone();
                                            let ata_b58 = bs58::encode(&pos.bot_token_account).into_string();
                                            let mint_log = mint_str.to_string();
                                            tokio::spawn(async move {
                                                let http = reqwest::Client::builder()
                                                    .timeout(Duration::from_secs(3))
                                                    .build()
                                                    .unwrap_or_default();
                                                let fetch_bal = || {
                                                    let http = http.clone();
                                                    let rpc_url = rpc_url.clone();
                                                    let ata_b58 = ata_b58.clone();
                                                    async move {
                                                        let body = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": 1,
                                                            "method": "getAccountInfo",
                                                            "params": [ata_b58, { "encoding": "base64", "commitment": "processed" }]
                                                        });
                                                        let resp = http.post(&rpc_url).json(&body).send().await.ok()?;
                                                        let val = resp.json::<Value>().await.ok()?;
                                                        let acc = val.pointer("/result/value")?;
                                                        if acc.is_null() {
                                                            return Some(0);
                                                        }
                                                        let s = acc.pointer("/data/0")?.as_str()?;
                                                        let buf = base64::engine::general_purpose::STANDARD.decode(s).ok()?;
                                                        if buf.len() >= 72 {
                                                            Some(u64::from_le_bytes(buf[64..72].try_into().unwrap_or([0; 8])))
                                                        } else {
                                                            Some(0)
                                                        }
                                                    }
                                                };

                                                // Force-sell loop: for 100% panic sell, re-check live on-chain balance and retry
                                                // up to 4 times with 0 slippage floor until raw_tok_bal == 0 is confirmed on-chain!
                                                let max_attempts: usize = if pct >= 100 { 4 } else { 1 };
                                                for attempt in 0..max_attempts {
                                                    let raw_tok_bal = match fetch_bal().await {
                                                        Some(b) => b,
                                                        None => {
                                                            if pos.token_balance > 0 {
                                                                pos.token_balance
                                                            } else {
                                                                warn!(mint = %mint_log, attempt, "Force sell RPC balance check failed; retrying instead of deleting position");
                                                                tokio::time::sleep(Duration::from_millis(600)).await;
                                                                continue;
                                                            }
                                                        }
                                                    };
                                                    if raw_tok_bal == 0 {
                                                        info!(mint = %mint_log, attempt, "Force sell confirmed 0 tokens remaining on-chain");
                                                        if pct >= 100 {
                                                            exits.remove_position(&mint_pk);
                                                        }
                                                        return;
                                                    }
                                                    info!(
                                                        mint = %mint_log,
                                                        pct,
                                                        raw_tok_bal,
                                                        attempt = attempt + 1,
                                                        max_attempts,
                                                        "Executing forced manual sell (min_quote_out=0)"
                                                    );
                                                    let live_dex_accs = exits
                                                        .get_position(&mint_pk)
                                                        .map(|p| p.dex_accounts)
                                                        .unwrap_or_else(|| pos.dex_accounts.clone());
                                                    let live_cpi = build_venue_cpi_accounts(
                                                        pos.venue,
                                                        &live_dex_accs,
                                                        hot_wallet,
                                                        pos.bot_token_account,
                                                        None,
                                                        token_prog,
                                                    );
                                                    let live_slot = shred.latest_slot();
                                                    let params = SellTxParams {
                                                        sell_router_program,
                                                        target_dex_program: target_dex,
                                                        venue: pos.venue,
                                                        hot_wallet,
                                                        bot_token_account: pos.bot_token_account,
                                                        pool_or_curve: pos.pool_or_curve,
                                                        is_token_2022: pos.is_token_2022,
                                                        pre_balance: raw_tok_bal,
                                                        sell_pct: pct,
                                                        min_quote_out: 0,
                                                        min_liquidity_floor_lamports: 0,
                                                        current_slot: if live_slot > 0 { live_slot } else { pos.opened_slot },
                                                        close_on_full_exit: close_on_full,
                                                        cpi_accounts: if live_cpi.is_empty() { cpi_accounts.clone() } else { live_cpi },
                                                    };
                                                    let _ = exec.execute_sell(params).await;
                                                    if pct >= 100 {
                                                        tokio::time::sleep(Duration::from_millis(1_500)).await;
                                                    }
                                                }
                                                if pct >= 100 && fetch_bal().await == Some(0) {
                                                    exits.remove_position(&mint_pk);
                                                }
                                            });
                                            (true, None)
                                        }
                                        None => (false, Some(format!("No open position for mint {mint_str}"))),
                                    },
                                    Err(e) => (false, Some(format!("Invalid mint pubkey: {e}"))),
                                }
                            }
                            _ => (false, Some(format!("Unknown command: {cmd_type}"))),
                        };

                        let ack = json!({ "type": "ack", "id": id, "ok": ok, "error": err_msg });
                        sink.send(Message::Text(ack.to_string().into()))
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    Ok(Message::Ping(p)) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) => return Err("bridge closed by control plane".into()),
                    Err(e) => return Err(e.to_string()),
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let signer = match HotWalletSigner::from_env() {
        Ok(Some(s)) => {
            info!(hot_wallet = %s.pubkey_bs58(), "Loaded HotWalletSigner from environment");
            Some(Arc::new(s))
        }
        Ok(None) => {
            warn!("No WHAL_HOT_WALLET_KEYPAIR set; running in unsigned/shadow-safe mode");
            None
        }
        Err(e) => {
            error!(error = %e, "Failed to parse WHAL_HOT_WALLET_KEYPAIR");
            None
        }
    };
    let hot_wallet_pubkey = signer.as_ref().map(|s| s.pubkey()).unwrap_or([0u8; 32]);

    info!(
        config = ?args.config_path,
        metrics = %args.metrics_bind,
        bridge = %args.bridge_url,
        "Starting whal-engine"
    );

    let metrics = Arc::new(EngineMetrics::default());
    let breakers = Arc::new(SafetyCircuitBreakers::new(HardSafetyLimits::default()));
    let engine_status = Arc::new(RwLock::new("PAUSED".to_string()));
    let (outbound_tx, _) = broadcast::channel::<Value>(512);

    // Step 7: Live Ingest (Jito ShredStream + Yellowstone Geyser) & Opportunity Bus
    let ingest = IngestEngine::new(
        ShredStreamConfig {
            endpoint_url: env::var("WHAL_SHREDSTREAM_URL").unwrap_or_default(),
            auth_token: env::var("WHAL_SHREDSTREAM_TOKEN").ok(),
        },
        GeyserConfig {
            grpc_url: env::var("WHAL_GEYSER_URL").unwrap_or_default(),
            x_token: env::var("WHAL_GEYSER_TOKEN").ok(),
        },
        LaunchFilterConfig::default(),
        None,
    );
    let _shred_handle = ingest.shredstream.clone().spawn_live();
    let _geyser_handle = ingest.geyser.clone().spawn_live();

    let rpc_url = env::var("WHAL_RPC_HTTP_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());

    // Step 8 & 9: Exit Engine + Executor Service with Ed25519 HotWalletSigner & Env Program IDs
    let exit_engine = Arc::new(ExitEngine::new(ExitPolicy::default()));
    let (exec_tel_tx, mut exec_tel_rx) = mpsc::channel::<ExecutorTelemetryEvent>(256);
    let executor = Arc::new(ExecutorService::with_signer(
        ProgramIds {
            dedupe_program: env_pubkey("WHAL_DEDUPE_PROGRAM_ID"),
            dedupe_state_account: env_pubkey("WHAL_DEDUPE_STATE_ACCOUNT"),
            router_program: env_pubkey("WHAL_ROUTER_PROGRAM_ID"),
            generic_quote_program: env_pubkey("WHAL_GENERIC_QUOTE_PROGRAM_ID"),
            sell_router_program: env_pubkey("WHAL_SELL_ROUTER_PROGRAM_ID"),
            pumpfun_program: whal_engine::ingest::detector::PUMPFUN_PROGRAM_ID,
            pumpswap_program: whal_engine::ingest::detector::PUMPSWAP_PROGRAM_ID,
            launchlab_program: whal_engine::ingest::detector::LAUNCHLAB_PROGRAM_ID,
        },
        NoncePool::new(vec![]),
        ExecutorRuntimeConfig {
            hot_wallet: hot_wallet_pubkey,
            tx_version: TxVersion::V0,
            v1_config: V1TransactionConfig {
                compute_unit_limit: 200_000,
                loaded_accounts_data_size_limit: 256 * 1024,
                priority_fee_lamports: 100_000,
            },
            lookup_tables: vec![],
            buy_slot_ttl: 4,
            slippage_bps: 1800,
            landers: vec![],
        },
        exec_tel_tx,
        signer.clone(),
        Some(exit_engine.clone()),
    ));
    let _nonce_sync_handle = executor.nonce_pool().spawn_rpc_sync(rpc_url.clone(), 4);
    let _keepalive_handle = executor.clone().spawn_keepalive();
    let _exec_handle = executor.clone().spawn(ingest.bus.subscribe());

    // 500ms KOL Pre-Slot Balance RPC Refresher + background v0 ALT cache loader for LocalShred
    whal_engine::ingest::shredstream::spawn_alt_cache_loader(rpc_url.clone());
    {
        let kol_rpc = rpc_url.clone();
        let kol_watchlist = ingest.watchlist.clone();
        let kol_balances = ingest.balances.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap_or_default();
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                ticker.tick().await;
                let accounts = kol_watchlist.watched_accounts();
                if accounts.is_empty() {
                    continue;
                }
                let keys: Vec<String> = accounts.iter().map(|a| bs58::encode(a).into_string()).collect();
                let req_body = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getMultipleAccounts",
                    "params": [keys, { "encoding": "base64", "commitment": "processed" }]
                });
                if let Ok(resp) = http.post(&kol_rpc).json(&req_body).send().await {
                    if let Ok(body) = resp.json::<Value>().await {
                        if let Some(vals) = body.pointer("/result/value").and_then(|v| v.as_array()) {
                            for (i, acc) in vals.iter().enumerate() {
                                if let Some(pk) = accounts.get(i) {
                                    let owner = acc.get("owner").and_then(|v| v.as_str()).unwrap_or("");
                                    let is_token_acc = owner == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                                        || owner == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
                                    if is_token_acc {
                                        if let Some(b64) = acc.pointer("/data/0").and_then(|v| v.as_str()) {
                                            if let Ok(buf) = base64::engine::general_purpose::STANDARD.decode(b64) {
                                                kol_balances.update_from_spl_data(*pk, &buf);
                                            }
                                        }
                                    } else if let Some(lamports) = acc.get("lamports").and_then(|v| v.as_u64()) {
                                        kol_balances.set(*pk, lamports);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // 0-Block Dev-Dump Instant Exit consumer (reacts in <1ms to creator Pump.fun sells seen in LocalShred!)
    {
        let (dev_dump_tx, mut dev_dump_rx) = tokio::sync::mpsc::channel::<PubkeyBytes>(32);
        ingest.shredstream.set_dev_dump_watcher(exit_engine.clone(), dev_dump_tx);
        let dump_exits = exit_engine.clone();
        let dump_exec = executor.clone();
        let dump_shred = ingest.shredstream.clone();
        let dump_detector = ingest.detector.clone();
        let dump_watchlist = ingest.watchlist.clone();
        let dump_hot = hot_wallet_pubkey;
        let dump_sell_router = env_pubkey("WHAL_SELL_ROUTER_PROGRAM_ID");
        tokio::spawn(async move {
            let mut recent: std::collections::HashMap<PubkeyBytes, u64> = std::collections::HashMap::new();
            while let Some(mint) = dev_dump_rx.recv().await {
                let now_ms = now_ms();
                if let Some(&prev) = recent.get(&mint) {
                    if now_ms.saturating_sub(prev) < 2_500 {
                        continue;
                    }
                }
                recent.insert(mint, now_ms);
                let Some(pos) = dump_exits.get_position(&mint) else { continue; };
                if pos.token_balance == 0 {
                    continue;
                }
                let token_prog = if pos.is_token_2022 {
                    whal_engine::executor::TOKEN_2022_PROGRAM_ID
                } else {
                    whal_engine::executor::SPL_TOKEN_PROGRAM_ID
                };
                let cpi_accounts = whal_engine::executor::builder::build_venue_cpi_accounts(
                    pos.venue,
                    &pos.dex_accounts,
                    dump_hot,
                    pos.bot_token_account,
                    None,
                    token_prog,
                );
                let target_dex_program = match pos.venue {
                    Venue::PumpFun => dump_exec.programs().pumpfun_program,
                    Venue::PumpSwap => dump_exec.programs().pumpswap_program,
                    Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => dump_exec.programs().launchlab_program,
                };
                if let Some(cr) = pos.creator {
                    if dump_watchlist.match_signer(&cr).is_none() {
                        dump_detector.block_rugged_creator(cr);
                    }
                }
                info!(
                    mint = %bs58::encode(&mint).into_string(),
                    raw_tok_bal = pos.token_balance,
                    "0-BLOCK DEV-DUMP / KOL EXIT DETECTED in LocalShred -> firing instant 100% 5-lander exit!"
                );
                let exec_clone = dump_exec.clone();
                let params = whal_engine::exits::SellTxParams {
                    sell_router_program: dump_sell_router,
                    target_dex_program,
                    venue: pos.venue,
                    hot_wallet: dump_hot,
                    bot_token_account: pos.bot_token_account,
                    pool_or_curve: pos.pool_or_curve,
                    is_token_2022: pos.is_token_2022,
                    pre_balance: pos.token_balance,
                    sell_pct: 100,
                    min_quote_out: 0,
                    min_liquidity_floor_lamports: 0,
                    current_slot: dump_shred.latest_slot(),
                    close_on_full_exit: true,
                    cpi_accounts,
                };
                tokio::spawn(async move {
                    let _ = exec_clone.execute_sell(params).await;
                });
            }
        });
    }

    // 1s Live Exit Engine Monitor Loop (drives Fail-Fast SL, TWAP drip, Exit Ladder & 45s Time-Stop on live positions)
    {
        let exit_rpc = rpc_url.clone();
        let exit_mgr = exit_engine.clone();
        let exit_exec = executor.clone();
        let exit_shred = ingest.shredstream.clone();
        let exit_hot = hot_wallet_pubkey;
        let sell_router_prog = env_pubkey("WHAL_SELL_ROUTER_PROGRAM_ID");
        let exit_out = outbound_tx.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap_or_default();
            let mut ticker = tokio::time::interval(Duration::from_millis(400));
            let mut zero_ticks: std::collections::HashMap<PubkeyBytes, u8> = std::collections::HashMap::new();
            let mut last_ui: std::collections::HashMap<PubkeyBytes, Value> = std::collections::HashMap::new();
            let mut last_sell_at: std::collections::HashMap<PubkeyBytes, u64> = std::collections::HashMap::new();
            let mut had_open_positions = false;
            loop {
                ticker.tick().await;
                let open = exit_mgr.list_positions();
                if open.is_empty() {
                    zero_ticks.clear();
                    last_ui.clear();
                    last_sell_at.clear();
                    if had_open_positions {
                        had_open_positions = false;
                        let _ = exit_out.send(json!({
                            "type": "event",
                            "event": { "type": "position_updated", "ts": now_ms(), "positions": [] }
                        }));
                    }
                    continue;
                }
                had_open_positions = true;
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut keys: Vec<String> = Vec::with_capacity(open.len() * 3);
                for p in &open {
                    if p.venue == Venue::PumpSwap && p.dex_accounts.len() > 8 {
                        // PumpSwap live price = pool_quote_vault[64..72] / pool_base_vault[64..72]
                        keys.push(bs58::encode(&p.dex_accounts[7]).into_string());
                        keys.push(bs58::encode(&p.bot_token_account).into_string());
                        keys.push(bs58::encode(&p.dex_accounts[8]).into_string());
                    } else {
                        keys.push(bs58::encode(&p.pool_or_curve).into_string());
                        keys.push(bs58::encode(&p.bot_token_account).into_string());
                        keys.push(bs58::encode(&p.pool_or_curve).into_string());
                    }
                }
                let req_body = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getMultipleAccounts",
                    "params": [keys, { "encoding": "base64", "commitment": "processed" }]
                });
                let vals = match http.post(&exit_rpc).json(&req_body).send().await {
                    Ok(resp) => resp
                        .json::<Value>()
                        .await
                        .ok()
                        .and_then(|b| b.pointer("/result/value").and_then(|v| v.as_array()).cloned())
                        .unwrap_or_default(),
                    Err(_) => continue,
                };
                if vals.len() != keys.len() {
                    continue;
                }
                let mut live_ui_positions: Vec<Value> = Vec::with_capacity(open.len());
                for (i, pos) in open.iter().enumerate() {
                    let b64 = vals
                        .get(i * 3)
                        .and_then(|v| v.pointer("/data/0"))
                        .and_then(|v| v.as_str());
                    let (price_sol, v_sol_lamports) = if pos.venue == Venue::PumpSwap && pos.dex_accounts.len() > 8 {
                        let q_b64 = vals
                            .get(i * 3 + 2)
                            .and_then(|v| v.pointer("/data/0"))
                            .and_then(|v| v.as_str());
                        match (
                            b64.and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()),
                            q_b64.and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()),
                        ) {
                            (Some(base_buf), Some(quote_buf)) if base_buf.len() >= 72 && quote_buf.len() >= 72 => {
                                let base_tok = u64::from_le_bytes(base_buf[64..72].try_into().unwrap_or([0; 8])) as f64 / 1e6;
                                let quote_sol = u64::from_le_bytes(quote_buf[64..72].try_into().unwrap_or([0; 8]));
                                let px = if base_tok > 0.0 { (quote_sol as f64 / 1e9) / base_tok } else { 0.0 };
                                (px, quote_sol)
                            }
                            _ => (0.0, 30_000_000_000),
                        }
                    } else {
                        match b64.and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()) {
                            Some(buf) if buf.len() >= 24 => {
                                if pos.venue == Venue::PumpFun && buf.len() >= 81 {
                                    if let Ok(live_creator) = <[u8; 32]>::try_from(&buf[49..81]) {
                                        if live_creator != [0u8; 32] {
                                            let live_vault = whal_engine::ingest::detector::derive_pumpfun_creator_vault(&live_creator);
                                            exit_mgr.sync_creator_vault(&pos.mint, live_vault);
                                        }
                                    }
                                }
                                let v_tokens = u64::from_le_bytes(buf[8..16].try_into().unwrap_or([0; 8])) as f64 / 1e6;
                                let v_sol = u64::from_le_bytes(buf[16..24].try_into().unwrap_or([0; 8]));
                                let px = if v_tokens > 0.0 { (v_sol as f64 / 1e9) / v_tokens } else { 0.0 };
                                (px, v_sol)
                            }
                            _ => (0.0, 30_000_000_000),
                        }
                    };
                    let tok_b64 = vals
                        .get(i * 3 + 1)
                        .and_then(|v| v.pointer("/data/0"))
                        .and_then(|v| v.as_str());
                    let raw_tok_bal: u64 = match tok_b64.and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()) {
                        Some(buf) if buf.len() >= 72 => u64::from_le_bytes(buf[64..72].try_into().unwrap_or([0; 8])),
                        _ => 0,
                    };
                    // Auto-prune unconfirmed/reverted buys after 6s; for confirmed live positions require
                    // 3 consecutive zero-balance ticks (1.2s) before closing so a 1-tick RPC blip never triggers a duplicate close.
                    if raw_tok_bal == 0 {
                        if pos.initial_tokens > 0.0 {
                            let cnt = zero_ticks.entry(pos.mint).or_insert(0);
                            *cnt = cnt.saturating_add(1);
                            if *cnt < 3 {
                                if let Some(prev_val) = last_ui.get(&pos.mint) {
                                    live_ui_positions.push(prev_val.clone());
                                }
                                continue;
                            }
                            zero_ticks.remove(&pos.mint);
                            last_ui.remove(&pos.mint);
                            last_sell_at.remove(&pos.mint);
                            exit_mgr.remove_position(&pos.mint);
                        } else if now_secs.saturating_sub(pos.opened_at_secs) >= 6 {
                            exit_mgr.remove_position(&pos.mint);
                        }
                        continue;
                    }
                    zero_ticks.remove(&pos.mint);
                    exit_mgr.set_token_balance(&pos.mint, raw_tok_bal);
                    let ui_tokens = raw_tok_bal as f64 / 1e6;
                    let init_toks = if pos.initial_tokens > 0.0 { pos.initial_tokens } else { ui_tokens };
                    let ui_price = if price_sol > 0.0 { price_sol } else if init_toks > 0.0 { pos.entry_price_sol / init_toks } else { 0.0 };
                    let sell_cooling_down = last_sell_at
                        .get(&pos.mint)
                        .map(|&t| now_ms().saturating_sub(t) < 1_000)
                        .unwrap_or(false);
                    if raw_tok_bal > 0 && price_sol > 0.0 && !sell_cooling_down {
                        if let Some(order) = exit_mgr.on_price_update(&pos.mint, price_sol, ui_tokens, v_sol_lamports, now_secs) {
                            last_sell_at.insert(pos.mint, now_ms());
                            info!(
                                mint = %bs58::encode(&pos.mint).into_string(),
                                sell_pct = order.sell_pct,
                                reason = order.reason,
                                "ExitEngine triggered automated live sell"
                            );
                            let target_dex = match pos.venue {
                                Venue::PumpFun => whal_engine::ingest::detector::PUMPFUN_PROGRAM_ID,
                                Venue::PumpSwap => whal_engine::ingest::detector::PUMPSWAP_PROGRAM_ID,
                                Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => whal_engine::ingest::detector::LAUNCHLAB_PROGRAM_ID,
                            };
                            let tok_prog = if pos.is_token_2022 { TOKEN_2022_PROGRAM_ID } else { SPL_TOKEN_PROGRAM_ID };
                            let live_dex_accs = exit_mgr
                                .get_position(&pos.mint)
                                .map(|p| p.dex_accounts)
                                .unwrap_or_else(|| pos.dex_accounts.clone());
                            let cpi_accounts = build_venue_cpi_accounts(
                                pos.venue,
                                &live_dex_accs,
                                exit_hot,
                                pos.bot_token_account,
                                None,
                                tok_prog,
                            );
                            let params = SellTxParams {
                                sell_router_program: sell_router_prog,
                                target_dex_program: target_dex,
                                venue: pos.venue,
                                hot_wallet: exit_hot,
                                bot_token_account: pos.bot_token_account,
                                pool_or_curve: pos.pool_or_curve,
                                is_token_2022: pos.is_token_2022,
                                pre_balance: raw_tok_bal,
                                sell_pct: order.sell_pct,
                                min_quote_out: 0,
                                min_liquidity_floor_lamports: 0,
                                current_slot: exit_shred.latest_slot(),
                                close_on_full_exit: order.close_account,
                                cpi_accounts,
                            };
                            let sell_exec = Arc::clone(&exit_exec);
                            tokio::spawn(async move {
                                let _ = sell_exec.execute_sell(params).await;
                            });
                        }
                    }
                    // Keep position visible in Active Positions until raw_tok_bal == 0 is confirmed on-chain!
                    let m = bs58::encode(&pos.mint).into_string();
                    let ui_val = json!({
                        "id": format!("live_{}", &m[..m.len().min(12)]),
                        "mint": m,
                        "symbol": &m[..m.len().min(5)],
                        "venue": pos.venue,
                        "kolId": pos.kol_id,
                        "openedAt": pos.opened_at_secs * 1_000,
                        "openedSlot": pos.opened_slot,
                        "entrySol": pos.entry_price_sol,
                        "initialTokens": init_toks,
                        "tokens": ui_tokens,
                        "entryPrice": if init_toks > 0.0 { pos.entry_price_sol / init_toks } else { pos.entry_price_sol },
                        "price": ui_price,
                        "realizedSol": 0.0,
                        "pendingSell": Value::Null
                    });
                    last_ui.insert(pos.mint, ui_val.clone());
                    live_ui_positions.push(ui_val);
                }
                let _ = exit_out.send(json!({
                    "type": "event",
                    "event": { "type": "position_updated", "ts": now_ms(), "positions": live_ui_positions }
                }));
            }
        });
    }

    // Forward Executor telemetry -> Step 3 Control Plane bridge
    let exec_out = outbound_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = exec_tel_rx.recv().await {
            if let Ok(val) = serde_json::to_value(&ev) {
                let _ = exec_out.send(json!({ "type": "event", "event": val }));
            }
        }
    });

    // 30s Signed Jupiter WSOL -> USDC Treasury Sweeper
    let (sweep_tx, mut sweep_rx) = mpsc::channel::<SweepRecord>(64);
    let mut sweeper_cfg = SweeperConfig::default();
    sweeper_cfg.wsol_vault_ata = env_pubkey("WHAL_WSOL_VAULT_ATA");
    sweeper_cfg.hot_wallet_bs58 = signer.as_ref().map(|s| s.pubkey_bs58()).unwrap_or_default();
    sweeper_cfg.rpc_url = rpc_url.clone();
    sweeper_cfg.enabled = signer.is_some() && sweeper_cfg.wsol_vault_ata != [0u8; 32];
    let sweeper = Arc::new(TreasurySweeper::with_signer(
        sweeper_cfg,
        ingest.balances.clone(),
        sweep_tx,
        signer,
    ));
    let _sweep_handle = sweeper.spawn();

    // Forward Treasury sweeps -> Step 3 Control Plane bridge
    let sweep_out = outbound_tx.clone();
    tokio::spawn(async move {
        while let Some(rec) = sweep_rx.recv().await {
            let _ = sweep_out.send(json!({
                "type": "event",
                "event": { "type": "sweep_executed", "ts": rec.ts, "sweep": rec }
            }));
        }
    });

    // Periodic live slot_tick forwarder (from ShredStream / Geyser)
    let slot_out = outbound_tx.clone();
    let shred_ref = ingest.shredstream.clone();
    let geyser_ref = ingest.geyser.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(800));
        let mut last_sent = 0u64;
        loop {
            interval.tick().await;
            let slot = shred_ref.latest_slot().max(geyser_ref.latest_slot());
            if slot > last_sent {
                last_sent = slot;
                let _ = slot_out.send(json!({
                    "type": "event",
                    "event": {
                        "type": "slot_tick",
                        "ts": now_ms(),
                        "chain": {
                            "slot": slot,
                            "shredLatencyMs": 0,
                            "nonces": { "ready": 0, "total": 0 },
                            "ring": { "used": 0, "size": 64 }
                        }
                    }
                }));
            }
        }
    });

    // 4s Recent Blockhash RPC refresher (enables live execution even when durable nonce pool is 0/0)
    if !rpc_url.is_empty() {
        let bh_exec = executor.clone();
        let bh_rpc = rpc_url.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::new();
            let mut interval = tokio::time::interval(Duration::from_secs(4));
            loop {
                interval.tick().await;
                let body = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getLatestBlockhash",
                    "params": [{ "commitment": "processed" }]
                });
                if let Ok(resp) = http.post(&bh_rpc).json(&body).send().await {
                    if let Ok(v) = resp.json::<Value>().await {
                        if let Some(bh_str) = v.pointer("/result/value/blockhash").and_then(|x| x.as_str()) {
                            if let Ok(bh) = pubkey_from_bs58(bh_str) {
                                bh_exec.set_recent_blockhash(bh);
                            }
                        }
                    }
                }
            }
        });
    }

    // Step 10: Prometheus /metrics + /health server & Step 3 /internal/engine bridge
    let bridge_ctx = Arc::new(BridgeContext {
        engine_status,
        executor,
        exit_engine,
        breakers,
        watchlist: ingest.watchlist.clone(),
        detector: ingest.detector.clone(),
        shredstream: ingest.shredstream.clone(),
        bus: ingest.bus.clone(),
        rpc_url,
        default_hot_wallet: hot_wallet_pubkey,
        outbound_tx,
    });
    let metrics_task = tokio::spawn(run_metrics_server(args.metrics_bind, metrics));
    let bridge_task = tokio::spawn(run_bridge_loop(
        args.bridge_url,
        args.bridge_token,
        bridge_ctx,
    ));

    let _ = tokio::signal::ctrl_c().await;
    info!("Received Ctrl+C, shutting down whal-engine");
    metrics_task.abort();
    bridge_task.abort();
}
