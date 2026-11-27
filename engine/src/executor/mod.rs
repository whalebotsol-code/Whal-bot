//! Transaction Builder, Dedupe Mutex & Multi-Lander SWQoS Fan-Out (`/engine/src/executor`).

pub mod builder;
pub mod landers;
pub mod nonce;
pub mod telemetry;
pub mod wire;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::ingest::{pubkey_to_bs58, PubkeyBytes, TriggerEvent, TriggerKind, Venue};
pub use builder::{
    derive_associated_token_account, derive_ephemeral_seed, derive_seeded_pubkey, AccountMetaSpec,
    BuildVariantParams, BuiltTransaction, InstructionSpec, ProgramIds, TxBuilder, TxVersion,
    V1TransactionConfig, ASSOCIATED_TOKEN_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID, SYSTEM_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
};
pub use landers::{BurstDispatchSummary, LanderConfig, LanderFanOut, LanderId, VariantDispatch};
pub use nonce::{NonceAccount, NoncePool};
pub use telemetry::{BurstFeedPayload, ExecutorTelemetryEvent, RevertBreakdown};
pub use wire::{serialize_and_sign_tx, sign_serialized_tx_base64, HotWalletSigner};

#[derive(Debug, Clone)]
pub struct ExecutorRuntimeConfig {
    pub hot_wallet: PubkeyBytes,
    pub tx_version: TxVersion,
    pub v1_config: V1TransactionConfig,
    pub lookup_tables: Vec<PubkeyBytes>,
    pub buy_slot_ttl: u64,
    pub slippage_bps: u64,
    pub landers: Vec<LanderConfig>,
}

/// Consumes `TriggerEvent`s from Step 7's `OpportunityBus`, builds distinct-signature
/// v1/v0 transactions, fans out concurrently across all enabled SWQoS landers, and
/// emits `burst_dispatched` and `tx_landed` telemetry to Step 3.
pub struct ExecutorService {
    fanout: Arc<LanderFanOut>,
    cfg: Arc<RwLock<ExecutorRuntimeConfig>>,
    shadow_mode: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    telemetry_tx: mpsc::Sender<ExecutorTelemetryEvent>,
    exit_engine: Option<Arc<crate::exits::ExitEngine>>,
    recent_buy_mints: dashmap::DashMap<PubkeyBytes, u64>,
}

impl ExecutorService {
    pub fn new(
        programs: ProgramIds,
        nonces: NoncePool,
        cfg: ExecutorRuntimeConfig,
        telemetry_tx: mpsc::Sender<ExecutorTelemetryEvent>,
    ) -> Self {
        Self::with_signer(programs, nonces, cfg, telemetry_tx, None, None)
    }

    pub fn with_signer(
        programs: ProgramIds,
        nonces: NoncePool,
        cfg: ExecutorRuntimeConfig,
        telemetry_tx: mpsc::Sender<ExecutorTelemetryEvent>,
        signer: Option<Arc<HotWalletSigner>>,
        exit_engine: Option<Arc<crate::exits::ExitEngine>>,
    ) -> Self {
        let builder = Arc::new(TxBuilder::new(programs));
        let fanout = Arc::new(LanderFanOut::with_signer(builder, nonces, signer));
        Self {
            fanout,
            cfg: Arc::new(RwLock::new(cfg)),
            shadow_mode: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(true)),
            telemetry_tx,
            exit_engine,
            recent_buy_mints: dashmap::DashMap::new(),
        }
    }

    pub fn nonce_pool(&self) -> NoncePool {
        self.fanout.nonce_pool()
    }

    pub fn set_recent_blockhash(&self, hash: [u8; 32]) {
        self.fanout.set_recent_blockhash(hash);
    }

    pub fn programs(&self) -> &ProgramIds {
        self.fanout.programs()
    }

    pub fn hot_wallet(&self) -> PubkeyBytes {
        self.cfg.read().map(|c| c.hot_wallet).unwrap_or([0u8; 32])
    }

    /// Pre-warms HTTP/TCP/TLS connections to all enabled SWQoS landers every 5s
    /// so Slot-0 bursts incur 0ms handshake latency.
    pub fn spawn_keepalive(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = self.fanout.http_client();
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let urls: Vec<String> = self
                    .cfg
                    .read()
                    .map(|c| {
                        c.landers
                            .iter()
                            .filter(|l| l.enabled && !l.endpoint_url.trim().is_empty())
                            .map(|l| l.endpoint_url.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                for url in urls {
                    let c = client.clone();
                    let ping_url = if url.contains("blockrazor.xyz") {
                        url.replace("/sendTransaction", "/health")
                    } else {
                        url
                    };
                    tokio::spawn(async move {
                        let _ = c.get(&ping_url).send().await;
                    });
                }
            }
        })
    }

    /// Executes a Lighthouse-protected percentage sell (`whal-sell-router` + optional `CloseAccount`)
    /// across all enabled SWQoS landers.
    pub async fn execute_sell(&self, params: crate::exits::SellTxParams) -> BurstDispatchSummary {
        let cfg = self.cfg.read().expect("executor config lock").clone();
        let shadow = self.shadow_mode.load(Ordering::Relaxed);
        let sell_ixs = crate::exits::build_protected_sell_instructions(&params);
        let variants = self.fanout.build_sell_variants(
            &sell_ixs,
            &cfg.landers,
            cfg.hot_wallet,
            params.bot_token_account,
            cfg.tx_version,
            cfg.v1_config,
            &cfg.lookup_tables,
        );
        let summary = self.fanout.fan_out(params.current_slot, variants, shadow).await;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let outcome = if shadow {
            "shadow"
        } else if summary.winner.is_some() {
            "filled"
        } else {
            "missed"
        };
        let _ = self.telemetry_tx.try_send(ExecutorTelemetryEvent::TxLanded {
            ts,
            burst_id: format!("sell_{:x}_{}", params.current_slot, ts & 0xFFFF),
            slot: params.current_slot,
            outcome,
            lander: summary.winner,
            signature: summary.winning_signature.clone(),
        });
        summary
    }

    pub fn set_shadow_mode(&self, enabled: bool) {
        self.shadow_mode.store(enabled, Ordering::Relaxed);
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
    }

    pub fn hot_reload(&self, next: ExecutorRuntimeConfig) {
        if let Ok(mut g) = self.cfg.write() {
            *g = next;
        }
    }

    /// Runs the consumer loop reading `TriggerEvent`s from the Step 7 `OpportunityBus`.
    pub fn spawn(
        self: Arc<Self>,
        mut rx: broadcast::Receiver<TriggerEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("ExecutorService subscribed to OpportunityBus");
            loop {
                match rx.recv().await {
                    Ok(trigger) => {
                        let svc = self.clone();
                        tokio::spawn(async move { svc.execute_trigger(trigger).await });
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "ExecutorService lagged behind OpportunityBus");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    pub async fn execute_trigger(&self, trigger: TriggerEvent) -> BurstDispatchSummary {
        if self.paused.load(Ordering::Relaxed) {
            return BurstDispatchSummary {
                slot: trigger.trigger_slot,
                total_sent: 0,
                accepted: 0,
                winner: None,
                winning_signature: None,
                results: Vec::new(),
            };
        }
        // Atomic in-flight + 60s mint dedupe so LocalShred (slot 0) and whal-api bridge (slot +1) never double-buy.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if let Some(prev) = self.recent_buy_mints.get(&trigger.mint) {
            if now_ms.saturating_sub(*prev) < 60_000 {
                return BurstDispatchSummary {
                    slot: trigger.trigger_slot,
                    total_sent: 0,
                    accepted: 0,
                    winner: None,
                    winning_signature: None,
                    results: Vec::new(),
                };
            }
        }
        self.recent_buy_mints.insert(trigger.mint, now_ms);
        if self.recent_buy_mints.len() > 500 {
            self.recent_buy_mints.retain(|_, &mut t| now_ms.saturating_sub(t) < 60_000);
        }
        if let Some(exits) = &self.exit_engine {
            // Only count on-chain confirmed positions (`token_balance > 0`) toward the max-5 cap
            // so strict Slot-0 Custom(6001) reverts never lock out subsequent launches.
            let confirmed_open = exits
                .list_positions()
                .iter()
                .filter(|p| p.token_balance > 0)
                .count();
            if confirmed_open >= 5 || exits.get_position(&trigger.mint).is_some() {
                return BurstDispatchSummary {
                    slot: trigger.trigger_slot,
                    total_sent: 0,
                    accepted: 0,
                    winner: None,
                    winning_signature: None,
                    results: Vec::new(),
                };
            }
        }
        let cfg = self.cfg.read().expect("executor config lock").clone();
        let shadow = self.shadow_mode.load(Ordering::Relaxed);
        let variants = self.fanout.build_variants(
            &trigger,
            &cfg.landers,
            cfg.hot_wallet,
            cfg.tx_version,
            cfg.v1_config,
            &cfg.lookup_tables,
            cfg.buy_slot_ttl,
            cfg.slippage_bps,
        );

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let burst_id = format!("ev_{:x}_{}", trigger.trigger_slot, ts & 0xFFFF);
        let (kind_str, kol_id, size_sol, trigger_symbol) = match &trigger.kind {
            TriggerKind::KolBuy { kol_id, buy_size_lamports, .. } => (
                "kol_buy",
                Some(kol_id.clone()),
                *buy_size_lamports as f64 / 1_000_000_000.0,
                String::new(),
            ),
            TriggerKind::ZeroBlockLaunch { dev_buy_lamports, symbol, .. } => (
                "launch",
                None,
                *dev_buy_lamports as f64 / 1_000_000_000.0,
                symbol.clone(),
            ),
        };

        let ephemeral_ata = variants
            .first()
            .map(|v| v.tx.ephemeral_token_account)
            .unwrap_or([0u8; 32]);
        let summary = self.fanout.fan_out(trigger.trigger_slot, variants, shadow).await;
        let outcome = if shadow {
            "shadow"
        } else if summary.winner.is_some() {
            "filled"
        } else {
            "missed"
        };

        // Register filled position in ExitEngine so automated TP/SL & manual sells have full PDA accounts.
        if summary.winner.is_some() && !shadow {
            if let Some(exits) = &self.exit_engine {
                let high_conviction = matches!(trigger.kind, TriggerKind::KolBuy { .. });
                let is_token_2022 = trigger.dex_accounts.contains(&TOKEN_2022_PROGRAM_ID);
                let token_program = if is_token_2022 {
                    TOKEN_2022_PROGRAM_ID
                } else {
                    SPL_TOKEN_PROGRAM_ID
                };
                let tp_order = exits.register_position(
                    crate::exits::ManagedPosition {
                        mint: trigger.mint,
                        pool_or_curve: trigger.pool_pda,
                        bot_token_account: ephemeral_ata,
                        venue: trigger.venue,
                        is_token_2022,
                        opened_at_secs: ts / 1_000,
                        opened_slot: trigger.trigger_slot,
                        entry_price_sol: size_sol,
                        token_balance: 0,
                        initial_tokens: 0.0,
                        peak_price_sol: 0.0,
                        breakeven_armed: false,
                        instant_tp_fired: false,
                        dex_accounts: trigger.dex_accounts.clone(),
                        same_slot_tp_fired: false,
                        fired_steps: vec![],
                        creator: match &trigger.kind {
                            TriggerKind::ZeroBlockLaunch { creator, .. } => Some(*creator),
                            TriggerKind::KolBuy { kol_wallet, .. } => Some(*kol_wallet),
                        },
                        kol_id: match &trigger.kind {
                            TriggerKind::KolBuy { kol_id, .. } => Some(kol_id.clone()),
                            TriggerKind::ZeroBlockLaunch { .. } => None,
                        },
                    },
                    high_conviction,
                );
                // Note: live UI `position_updated` is emitted by the 1s on-chain ExitEngine monitor
                // ONLY once `raw_tok_bal > 0` is confirmed on-chain (prevents ghost positions when a Jito TX reverts).

                // Fhbh1D same-slot 25% opening TP1 via Program #4 percentage sell router (skipped in direct native mode until token balance is read)
                let direct_native = std::env::var("WHAL_DIRECT_NATIVE_DEX")
                    .ok()
                    .map(|v| matches!(v.trim(), "1" | "true" | "TRUE"))
                    .unwrap_or(false);
                if !direct_native {
                    if let Some(order) = tp_order {
                        let progs = self.fanout.programs();
                        let target_dex = match trigger.venue {
                            Venue::PumpFun => progs.pumpfun_program,
                            Venue::PumpSwap => progs.pumpswap_program,
                            Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => progs.launchlab_program,
                        };
                        let cpi_accounts = builder::build_venue_cpi_accounts(
                            trigger.venue,
                            &trigger.dex_accounts,
                            cfg.hot_wallet,
                            ephemeral_ata,
                            None,
                            token_program,
                        );
                        let sell_params = crate::exits::SellTxParams {
                            sell_router_program: progs.sell_router_program,
                            target_dex_program: target_dex,
                            venue: trigger.venue,
                            hot_wallet: cfg.hot_wallet,
                            bot_token_account: ephemeral_ata,
                            pool_or_curve: trigger.pool_pda,
                            is_token_2022,
                            pre_balance: 0,
                            sell_pct: order.sell_pct,
                            min_quote_out: 1,
                            min_liquidity_floor_lamports: 5_000_000_000,
                            current_slot: trigger.trigger_slot,
                            close_on_full_exit: order.close_account,
                            cpi_accounts,
                        };
                        let _ = self.execute_sell(sell_params).await;
                    }
                }
            }
        }

        let _ = self.telemetry_tx.try_send(ExecutorTelemetryEvent::BurstDispatched {
            ts,
            burst: BurstFeedPayload {
                id: burst_id.clone(),
                ts,
                slot: trigger.trigger_slot,
                kind: kind_str,
                kol_id,
                mint: pubkey_to_bs58(&trigger.mint),
                symbol: if trigger_symbol.is_empty() {
                    let m = pubkey_to_bs58(&trigger.mint);
                    m[..m.len().min(5)].to_string()
                } else {
                    trigger_symbol.clone()
                },
                venue: trigger.venue,
                size_sol,
                fanout: summary.total_sent,
                winner: summary.winner,
                signature: summary.winning_signature.clone(),
                cu_used: if summary.winner.is_some() { 48_000 } else { 2_500 },
                reverts: RevertBreakdown::default(),
                outcome,
                note: if shadow { Some("Shadow mode: simulated, nothing sent".into()) } else { None },
            },
        });

        let _ = self.telemetry_tx.try_send(ExecutorTelemetryEvent::TxLanded {
            ts,
            burst_id,
            slot: trigger.trigger_slot,
            outcome,
            lander: summary.winner,
            signature: summary.winning_signature.clone(),
        });

        summary
    }
}
