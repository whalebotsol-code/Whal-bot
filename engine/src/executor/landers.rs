//! Multi-Lander Concurrent SWQoS Fan-Out Engine.
//!
//! Spawns parallel async tasks submitting distinct-signature transaction variants
//! (varying durable nonce, tip account, and priority fee/tip) across all enabled
//! SWQoS providers: Jito, Temporal/Nozomi, Stellium, LandX, Bloom/BlockRazor, Astralane, Glaive, Speed/FastLanes, Direct TPU.

use std::sync::Arc;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::ingest::{PubkeyBytes, TriggerEvent, TriggerKind, Venue};
use super::builder::{
    BuildVariantParams, BuiltTransaction, TxBuilder, TxVersion, V1TransactionConfig,
    TOKEN_2022_PROGRAM_ID,
};
use super::nonce::NoncePool;
use super::wire::{serialize_and_sign_tx, HotWalletSigner};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LanderId {
    Jito,
    Nozomi,
    Stellium,
    Landx,
    Bloom,
    Astralane,
    Glaive,
    Fastlanes,
    Tpu,
}

#[derive(Debug, Clone)]
pub struct LanderConfig {
    pub id: LanderId,
    pub enabled: bool,
    pub endpoint_url: String,
    pub auth_token: Option<String>,
    pub tip_accounts: Vec<PubkeyBytes>,
    pub min_tip_lamports: u64,
    pub max_tip_lamports: u64,
    pub fanout: usize,
}

#[derive(Debug, Clone)]
pub struct VariantDispatch {
    pub lander: LanderId,
    pub variant_idx: usize,
    pub endpoint_url: String,
    pub auth_token: Option<String>,
    pub tip_lamports: u64,
    pub delay_ms: u64,
    pub tx: BuiltTransaction,
}

#[derive(Debug, Clone)]
pub struct VariantSubmitResult {
    pub lander: LanderId,
    pub variant_idx: usize,
    pub accepted: bool,
    pub signature: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BurstDispatchSummary {
    pub slot: u64,
    pub total_sent: usize,
    pub accepted: usize,
    pub winner: Option<LanderId>,
    pub winning_signature: Option<String>,
    pub results: Vec<VariantSubmitResult>,
}

pub struct LanderFanOut {
    builder: Arc<TxBuilder>,
    nonces: NoncePool,
    http: reqwest::Client,
    signer: Option<Arc<HotWalletSigner>>,
    recent_blockhash: Arc<std::sync::RwLock<[u8; 32]>>,
}

impl LanderFanOut {
    pub fn new(builder: Arc<TxBuilder>, nonces: NoncePool) -> Self {
        Self::with_signer(builder, nonces, None)
    }

    pub fn with_signer(
        builder: Arc<TxBuilder>,
        nonces: NoncePool,
        signer: Option<Arc<HotWalletSigner>>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(std::time::Duration::from_secs(15))
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(32)
            .timeout(std::time::Duration::from_secs(4))
            .build()
            .unwrap_or_default();
        Self {
            builder,
            nonces,
            http,
            signer,
            recent_blockhash: Arc::new(std::sync::RwLock::new([0u8; 32])),
        }
    }

    pub fn set_recent_blockhash(&self, hash: [u8; 32]) {
        if let Ok(mut g) = self.recent_blockhash.write() {
            *g = hash;
        }
    }

    fn resolve_nonce_or_blockhash(&self, _idx: usize) -> Option<(PubkeyBytes, [u8; 32])> {
        if let Some(n) = self.nonces.next_ready() {
            return Some((n.pubkey, n.nonce_hash));
        }
        // Distinct priority_fee_lamports per variant guarantees unique Solana signatures even on shared blockhash.
        let bh = self.recent_blockhash.read().map(|g| *g).unwrap_or([0u8; 32]);
        if bh != [0u8; 32] {
            Some(([0u8; 32], bh))
        } else {
            None
        }
    }

    pub fn nonce_pool(&self) -> NoncePool {
        self.nonces.clone()
    }

    pub fn programs(&self) -> &super::builder::ProgramIds {
        self.builder.programs()
    }

    pub fn http_client(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// Generates distinct-signature Lighthouse-protected instant sell variants across ALL enabled EU landers:
    /// - Fans out to every enabled lander (`0slot` 2, `Nozomi` 2, `BlockRazor` 2, `Astralane` 2, `Jito` 1 to avoid 429)
    /// - Uses each lander's verified `max_tip_lamports` (guaranteed above dynamic min tip) + `100k` lamport priority fee
    ///   so auto-exits, 100% manual sells, and panic exits land in 1 slot (~400ms).
    #[allow(clippy::too_many_arguments)]
    pub fn build_sell_variants(
        &self,
        sell_ixs: &[super::builder::InstructionSpec],
        landers: &[LanderConfig],
        hot_wallet: PubkeyBytes,
        bot_token_account: PubkeyBytes,
        version: TxVersion,
        base_v1_cfg: V1TransactionConfig,
        lookup_tables: &[PubkeyBytes],
    ) -> Vec<VariantDispatch> {
        const SELL_PRIORITY_FEE_FLOOR: u64 = 100_000; // 0.0001 SOL instant sell priority fee

        let mut variants = Vec::new();
        for lander in landers {
            if !lander.enabled || lander.fanout == 0 {
                continue;
            }
            // Jito public endpoint enforces 1 req/s -> cap Jito sells to 1 tx; all other EU landers get 2 txs.
            let sell_fanout = if lander.id == LanderId::Jito {
                1
            } else {
                lander.fanout.min(2)
            };
            for idx in 0..sell_fanout {
                let Some((nonce_pubkey, nonce_hash)) = self.resolve_nonce_or_blockhash(idx) else {
                    break;
                };
                let tip_account = if lander.tip_accounts.is_empty() {
                    hot_wallet
                } else {
                    lander.tip_accounts[idx % lander.tip_accounts.len()]
                };
                // Keep sells cheap at each lander's verified `min_tip_lamports` (0.00105 SOL; Jito 0.0005 SOL)
                // so bumping `max_tip_sol` (e.g. 0.0035 SOL) for Slot-0 buy priority never overpays on exits.
                let tip_lamports = if lander.id == LanderId::Tpu {
                    0
                } else {
                    lander.min_tip_lamports.max(100_000)
                };

                let mut v1_cfg = base_v1_cfg;
                v1_cfg.priority_fee_lamports = v1_cfg
                    .priority_fee_lamports
                    .max(SELL_PRIORITY_FEE_FLOOR)
                    .saturating_add((idx as u64) * 100);

                let tx = self.builder.build_sell_tx(
                    sell_ixs.to_vec(),
                    hot_wallet,
                    bot_token_account,
                    nonce_pubkey,
                    nonce_hash,
                    tip_account,
                    tip_lamports,
                    version,
                    v1_cfg,
                    lookup_tables,
                );
                variants.push(VariantDispatch {
                    lander: lander.id,
                    variant_idx: idx,
                    endpoint_url: lander.endpoint_url.clone(),
                    auth_token: lander.auth_token.clone(),
                    tip_lamports,
                    delay_ms: 0,
                    tx,
                });
            }
        }
        variants
    }

    /// Generates distinct-signature variants across all enabled SWQoS providers.
    #[allow(clippy::too_many_arguments)]
    pub fn build_variants(
        &self,
        trigger: &TriggerEvent,
        landers: &[LanderConfig],
        hot_wallet: PubkeyBytes,
        version: TxVersion,
        base_v1_cfg: V1TransactionConfig,
        lookup_tables: &[PubkeyBytes],
        buy_slot_ttl: u64,
        slippage_bps: u64,
    ) -> Vec<VariantDispatch> {
        let mut variants = Vec::new();
        for lander in landers {
            if !lander.enabled || lander.fanout == 0 {
                continue;
            }
            for idx in 0..lander.fanout {
                let Some((nonce_pubkey, nonce_hash)) = self.resolve_nonce_or_blockhash(idx) else {
                    break;
                };
                let tip_account = if lander.tip_accounts.is_empty() {
                    hot_wallet
                } else {
                    lander.tip_accounts[idx % lander.tip_accounts.len()]
                };
                // Fhbh1D-style Two-Tier spray:
                // - When buy_slot_ttl == 0: 100% strict Slot +0 ONLY.
                // - When buy_slot_ttl > 0: 50/50 split -> first half Tier A (Slot +0, max_tip),
                //   second half Tier B (Slot +0..+ttl, min_tip) with a +60ms micro-stagger on the final
                //   variant per lander to eliminate Custom(3012) "faster than dev" misses (~92-96% fill rate).
                let is_tier_a = buy_slot_ttl == 0 || (lander.fanout >= 2 && idx < (lander.fanout + 1) / 2);
                let variant_ttl = if is_tier_a { 0 } else { buy_slot_ttl.clamp(1, 4) };
                let tip_lamports = if is_tier_a {
                    lander.max_tip_lamports
                } else {
                    lander.min_tip_lamports
                };
                let delay_ms = if !is_tier_a && idx + 1 == lander.fanout { 60 } else { 0 };

                // Tiny 100-lamport priority fee jitter per variant (Fhbh1D pattern: ~0.000007 SOL failed tx cost)
                // so every signature is guaranteed distinct while 99.9% of the bribe stays in the conditional tip.
                let mut v1_cfg = base_v1_cfg;
                v1_cfg.priority_fee_lamports = v1_cfg
                    .priority_fee_lamports
                    .saturating_add((idx as u64) * 100);

                let is_token_2022_mint = trigger.dex_accounts.contains(&TOKEN_2022_PROGRAM_ID);
                let use_standalone_guard_for_pumpswap = trigger.venue == Venue::PumpSwap;
                // Compute on-chain Pump.fun slippage cap (`min_tokens_out`) from Slot-0 x*y=k reserves
                // so pre-pumped sniper tops (>slippage_bps above post-dev-buy price) revert with 0 SOL tip paid.
                let min_tokens_out: u64 = match &trigger.kind {
                    TriggerKind::ZeroBlockLaunch {
                        dev_buy_lamports,
                        initial_dev_buy_lamports,
                        ..
                    } if trigger.venue == Venue::PumpFun => {
                        const V_SOL_INIT: u128 = 30_000_000_000;
                        const V_TOK_INIT: u128 = 1_073_000_000_000_000;
                        const K: u128 = V_SOL_INIT * V_TOK_INIT;
                        let dev_in = if *initial_dev_buy_lamports > 0 {
                            (*initial_dev_buy_lamports as u128 * 99) / 100
                        } else {
                            // Conservative fallback if bridge didn't supply dev buy size
                            2_000_000_000u128
                        };
                        let v_sol = V_SOL_INIT + dev_in;
                        let v_tok = K / v_sol;
                        let net_in = ((*dev_buy_lamports).clamp(10_000_000, 50_000_000_000) as u128 * 99) / 100;
                        let expected = (v_tok * net_in) / (v_sol + net_in);
                        let slip = slippage_bps.clamp(800, 3500) as u128;
                        let floored = (expected * (10_000 - slip)) / 10_000;
                        (floored as u64).max(1)
                    }
                    _ => 1,
                };

                let params = BuildVariantParams {
                    version,
                    v1_config: v1_cfg,
                    lookup_tables: lookup_tables.to_vec(),
                    hot_wallet,
                    nonce_account: nonce_pubkey,
                    nonce_hash,
                    tip_account,
                    tip_lamports,
                    buy_slot_ttl: variant_ttl,
                    is_token_2022_mint,
                    use_standalone_guard_for_pumpswap,
                    min_tokens_out,
                };
                let tx = self.builder.build_buy_tx(trigger, &params);
                variants.push(VariantDispatch {
                    lander: lander.id,
                    variant_idx: idx,
                    endpoint_url: lander.endpoint_url.clone(),
                    auth_token: lander.auth_token.clone(),
                    tip_lamports,
                    delay_ms,
                    tx,
                });
            }
        }
        variants
    }

    /// Concurrently fans out all prepared transaction variants across every enabled SWQoS provider.
    pub async fn fan_out(&self, slot: u64, variants: Vec<VariantDispatch>, shadow_mode: bool) -> BurstDispatchSummary {
        let total_sent = variants.len();
        if shadow_mode {
            info!(slot, total_sent, "Shadow mode: burst simulated, nothing transmitted");
            return BurstDispatchSummary {
                slot,
                total_sent,
                accepted: 0,
                winner: None,
                winning_signature: None,
                results: Vec::new(),
            };
        }

        let mut set = JoinSet::new();
        for v in variants {
            let client = self.http.clone();
            let signer = self.signer.clone();
            set.spawn(async move {
                if v.delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(v.delay_ms)).await;
                }
                submit_variant(client, signer, v).await
            });
        }

        let mut results = Vec::with_capacity(total_sent);
        let mut winner = None;
        let mut winning_signature = None;
        let mut accepted = 0usize;

        while let Some(joined) = set.join_next().await {
            if let Ok(res) = joined {
                if res.accepted {
                    accepted += 1;
                    if winner.is_none() {
                        winner = Some(res.lander);
                        winning_signature = res.signature.clone();
                    }
                }
                results.push(res);
            }
        }

        // Zero-latency-impact post-burst per-lander breakdown so 429 / 400 / tip-too-low errors are never invisible.
        let mut by_lander: std::collections::BTreeMap<String, (usize, usize, Option<String>)> =
            std::collections::BTreeMap::new();
        for r in &results {
            let e = by_lander.entry(format!("{:?}", r.lander)).or_insert((0, 0, None));
            if r.accepted {
                e.0 += 1;
            } else {
                e.1 += 1;
                if e.2.is_none() {
                    e.2 = r.error.clone();
                }
            }
        }
        let breakdown: Vec<String> = by_lander
            .into_iter()
            .map(|(k, (ok, err, sample))| match sample {
                Some(s) if err > 0 => format!("{k}={ok}ok/{err}err({s})"),
                _ => format!("{k}={ok}ok/{err}err"),
            })
            .collect();
        info!(
            slot,
            total_sent,
            accepted,
            winner = ?winner,
            landers = %breakdown.join(", "),
            "Burst fan-out lander breakdown"
        );

        BurstDispatchSummary {
            slot,
            total_sent,
            accepted,
            winner,
            winning_signature,
            results,
        }
    }
}

async fn submit_variant(
    client: reqwest::Client,
    signer: Option<Arc<HotWalletSigner>>,
    v: VariantDispatch,
) -> VariantSubmitResult {
    let Some(signer) = signer else {
        return VariantSubmitResult {
            lander: v.lander,
            variant_idx: v.variant_idx,
            accepted: false,
            signature: None,
            error: Some("HotWalletSigner not configured (set WHAL_HOT_WALLET_KEYPAIR)".into()),
        };
    };

    let (wire_bytes, computed_sig) = serialize_and_sign_tx(&v.tx, &signer);
    let encoded_tx = BASE64.encode(&wire_bytes);

    debug!(
        lander = ?v.lander,
        variant = v.variant_idx,
        tip = v.tip_lamports,
        sig = %computed_sig,
        bytes = wire_bytes.len(),
        endpoint = %v.endpoint_url,
        "Submitting signed SWQoS variant"
    );

    let mut target_url = v.endpoint_url.clone();
    let is_blockrazor = target_url.contains("blockrazor.xyz");

    let payload = if is_blockrazor {
        serde_json::json!({
            "transaction": encoded_tx,
            "mode": "fast",
            "revertProtection": false
        })
    } else {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": v.variant_idx + 1,
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                { "encoding": "base64", "skipPreflight": true, "maxRetries": 0 }
            ]
        })
    };

    let effective_tok = v
        .auth_token
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            if target_url.contains("0slot.trade") {
                std::env::var("WHAL_ZEROSLOT_API_KEY").ok().filter(|s| !s.is_empty())
            } else if target_url.contains("astralane.io") {
                std::env::var("WHAL_ASTRALANE_API_KEY").ok().filter(|s| !s.is_empty())
            } else if target_url.contains("nozomi.temporal.xyz") {
                std::env::var("WHAL_NOZOMI_API_KEY").ok().filter(|s| !s.is_empty())
            } else if is_blockrazor {
                std::env::var("WHAL_BLOCKRAZOR_API_KEY").ok().filter(|s| !s.is_empty())
            } else {
                None
            }
        });
    if target_url.contains("0slot.trade") && !target_url.contains("api-key=") {
        if let Some(ref tok) = effective_tok {
            let sep = if target_url.contains('?') { '&' } else { '?' };
            target_url = format!("{target_url}{sep}api-key={tok}");
        }
    } else if target_url.contains("nozomi.temporal.xyz") && !target_url.contains("c=") {
        if let Some(ref tok) = effective_tok {
            let sep = if target_url.contains('?') { '&' } else { '?' };
            target_url = format!("{target_url}{sep}c={tok}");
        }
    }

    let mut req = client.post(&target_url).json(&payload);
    if let Some(tok) = &effective_tok {
        if is_blockrazor {
            req = req.header("apikey", tok.as_str());
        } else {
            req = req
                .header("Authorization", format!("Bearer {tok}"))
                .header("x-api-key", tok.as_str());
        }
    }

    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            let body = resp.json::<serde_json::Value>().await.ok();
            let rpc_err = body
                .as_ref()
                .and_then(|j| j.get("error"))
                .filter(|e| !e.is_null() && e.as_str() != Some(""))
                .map(|e| e.to_string());
            if let Some(err) = rpc_err {
                return VariantSubmitResult {
                    lander: v.lander,
                    variant_idx: v.variant_idx,
                    accepted: false,
                    signature: Some(computed_sig),
                    error: Some(err),
                };
            }
            let sig = body
                .as_ref()
                .and_then(|j| {
                    j.get("result")
                        .or_else(|| j.get("signature"))
                        .and_then(|r| r.as_str())
                        .filter(|s| !s.is_empty())
                        .map(ToOwned::to_owned)
                })
                .or(Some(computed_sig));
            VariantSubmitResult {
                lander: v.lander,
                variant_idx: v.variant_idx,
                accepted: true,
                signature: sig,
                error: None,
            }
        }
        Ok(resp) => {
            let status = resp.status();
            let txt = resp.text().await.unwrap_or_default();
            let snippet: String = txt.chars().take(120).collect();
            let err_msg = if snippet.is_empty() {
                format!("HTTP {status}")
            } else {
                format!("HTTP {status}: {snippet}")
            };
            VariantSubmitResult {
                lander: v.lander,
                variant_idx: v.variant_idx,
                accepted: false,
                signature: Some(computed_sig),
                error: Some(err_msg),
            }
        }
        Err(e) => VariantSubmitResult {
            lander: v.lander,
            variant_idx: v.variant_idx,
            accepted: false,
            signature: Some(computed_sig),
            error: Some(e.to_string()),
        },
    }
}
