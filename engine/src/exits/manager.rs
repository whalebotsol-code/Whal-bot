//! Position Manager & Automated Staged Exit Ladder Engine.

use std::sync::{Arc, RwLock};
use dashmap::DashMap;

use crate::ingest::{PubkeyBytes, Venue};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LadderTrigger {
    PnlMultiple(f64),
    TimeSecs(u64),
}

#[derive(Debug, Clone, Copy)]
pub struct LadderStep {
    pub sell_pct: u64,
    pub trigger: LadderTrigger,
}

#[derive(Debug, Clone)]
pub struct ExitPolicy {
    pub same_slot_take_profit_enabled: bool,
    pub same_slot_take_profit_pct: u64, // default 25%
    pub instant_tp_pct: u64,            // default 25% on +18% pop (1:1 with Paper Mode)
    pub stop_loss_pct: f64,             // default 30% hard SL
    pub trailing_stop_pct: f64,         // default 18% trailing drop from peak (arms at +30%)
    pub curve_floor_enabled: bool,
    pub min_liquidity_floor_lamports: u64,
    pub max_hold_secs: u64,             // default 1560s (26m moonbag hard cap)
    pub ladder: Vec<LadderStep>,
}

impl Default for ExitPolicy {
    fn default() -> Self {
        Self {
            same_slot_take_profit_enabled: true,
            same_slot_take_profit_pct: 25,
            instant_tp_pct: 25,
            stop_loss_pct: 16.0,
            trailing_stop_pct: 0.0,
            curve_floor_enabled: true,
            min_liquidity_floor_lamports: 10_000_000_000, // 10 SOL
            max_hold_secs: 1560,
            ladder: vec![
                LadderStep { sell_pct: 15, trigger: LadderTrigger::PnlMultiple(1.5) },
                LadderStep { sell_pct: 33, trigger: LadderTrigger::PnlMultiple(2.5) },
                LadderStep { sell_pct: 100, trigger: LadderTrigger::PnlMultiple(5.0) },
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagedPosition {
    pub mint: PubkeyBytes,
    pub pool_or_curve: PubkeyBytes,
    pub bot_token_account: PubkeyBytes,
    pub venue: Venue,
    pub is_token_2022: bool,
    pub opened_at_secs: u64,
    pub opened_slot: u64,
    pub entry_price_sol: f64,
    pub token_balance: u64,
    pub initial_tokens: f64,
    pub peak_price_sol: f64,
    pub breakeven_armed: bool,
    pub instant_tp_fired: bool,
    pub dex_accounts: Vec<PubkeyBytes>,
    pub same_slot_tp_fired: bool,
    pub fired_steps: Vec<bool>,
    pub creator: Option<PubkeyBytes>,
    pub kol_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitOrder {
    pub mint: PubkeyBytes,
    pub sell_pct: u64,
    pub close_account: bool,
    pub reason: &'static str,
}

pub struct ExitEngine {
    positions: Arc<DashMap<PubkeyBytes, ManagedPosition>>,
    twap_state: Arc<DashMap<PubkeyBytes, (u64, u8)>>, // (last_drip_secs, drips_fired)
    policy: RwLock<ExitPolicy>,
}

impl ExitEngine {
    pub fn new(policy: ExitPolicy) -> Self {
        Self {
            positions: Arc::new(DashMap::new()),
            twap_state: Arc::new(DashMap::new()),
            policy: RwLock::new(policy),
        }
    }

    /// Hot-reloads the live exit policy from Control Plane `config.apply` (1:1 parity with Paper Mode).
    pub fn update_policy(&self, policy: ExitPolicy) {
        if let Ok(mut guard) = self.policy.write() {
            *guard = policy;
        }
    }

    /// Registers a newly opened position and optionally returns an immediate same-slot 25%
    /// take-profit `ExitOrder` for high-conviction entries.
    pub fn register_position(
        &self,
        mut pos: ManagedPosition,
        high_conviction: bool,
    ) -> Option<ExitOrder> {
        let policy = self.policy.read().map(|g| g.clone()).unwrap_or_default();
        pos.fired_steps = vec![false; policy.ladder.len()];
        let mint = pos.mint;
        let same_slot_order = if high_conviction && policy.same_slot_take_profit_enabled {
            pos.same_slot_tp_fired = true;
            Some(ExitOrder {
                mint,
                sell_pct: policy.same_slot_take_profit_pct,
                close_account: policy.same_slot_take_profit_pct >= 100,
                reason: "Same-slot take-profit",
            })
        } else {
            None
        };
        self.positions.insert(mint, pos);
        same_slot_order
    }

    /// Evaluates a live on-chain per-token price + remaining UI tokens + curve liquidity against
    /// the exact 1:1 Paper Mode exit rules (curve floor, 2-3s fail-fast SL, hard SL, breakeven shield,
    /// trailing stop, 45s dead-pos time-stop, 5m/26m caps, instant +18% TP, TWAP drip, exit ladder).
    pub fn on_price_update(
        &self,
        mint: &PubkeyBytes,
        current_unit_price_sol: f64,
        current_ui_tokens: f64,
        curve_reserve_lamports: u64,
        now_secs: u64,
    ) -> Option<ExitOrder> {
        let policy = self.policy.read().map(|g| g.clone()).unwrap_or_default();
        let mut entry = self.positions.get_mut(mint)?;
        let pos = entry.value_mut();

        // Capture initial confirmed on-chain token balance once so partial TP sells never distort `mult`.
        if pos.initial_tokens <= 0.0 && current_ui_tokens > 0.0 {
            pos.initial_tokens = current_ui_tokens;
        }
        let entry_unit_price = if pos.initial_tokens > 0.0 {
            pos.entry_price_sol / pos.initial_tokens
        } else {
            pos.entry_price_sol
        };
        if current_unit_price_sol > pos.peak_price_sol {
            pos.peak_price_sol = current_unit_price_sol;
        }

        // 1. Emergency liquidity floor protection -> 100% exit + rent reclaim
        if policy.curve_floor_enabled && curve_reserve_lamports < policy.min_liquidity_floor_lamports {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Liquidity floor",
            });
        }

        // 1a. Pre-graduation 100% take-profit (~94% Pump.fun curve = 108 vSOL) so winning runners
        // lock in full profit cleanly before migration locks the bonding curve (Custom: 6005).
        if pos.venue == Venue::PumpFun && curve_reserve_lamports >= 108_000_000_000 {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Pre-graduation TP (94% curve)",
            });
        }

        let held_secs = now_secs.saturating_sub(pos.opened_at_secs);
        let mult = if entry_unit_price > 0.0 {
            current_unit_price_sol / entry_unit_price
        } else {
            1.0
        };
        // Arm Breakeven shield at +18% (same tick as Instant TP1) so CreateV2 pops never turn into red -16% SLs.
        if mult >= 1.18 {
            pos.breakeven_armed = true;
        }

        // 1b. Fail-Fast Stop-Loss: dump 100% at -8% between 3–12s if momentum fails on Slot +0 entries
        if held_secs >= 3
            && held_secs <= 12
            && mult > 0.0
            && mult <= 0.92
            && pos.peak_price_sol < entry_unit_price * 1.10
        {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Fail-fast SL (-8%)",
            });
        }

        // 2. Hard Stop-Loss (e.g. -30%)
        if policy.stop_loss_pct > 0.0 && mult > 0.0 && mult <= 1.0 - policy.stop_loss_pct / 100.0 {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Hard stop-loss",
            });
        }

        // 2b. Breakeven Shield: once up >= +25%, protect entry + 3% fee buffer so winners never turn red
        if pos.breakeven_armed && mult > 0.0 && mult <= 1.03 {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Breakeven shield",
            });
        }

        // 3. Trailing Stop-Loss (arms once peak >= +30%)
        if policy.trailing_stop_pct > 0.0
            && pos.peak_price_sol >= entry_unit_price * 1.30
            && pos.peak_price_sol > 0.0
        {
            let drop_from_peak = (pos.peak_price_sol - current_unit_price_sol) / pos.peak_price_sol;
            if drop_from_peak >= policy.trailing_stop_pct / 100.0 {
                return Some(ExitOrder {
                    mint: *mint,
                    sell_pct: 100,
                    close_account: true,
                    reason: "Trailing stop",
                });
            }
        }

        // 3a. 45s dead-position time-stop (1:1 parity with 11-24 Paper Mode winner) for flat/dead positions (< +5% and never popped >= +18%)
        if held_secs >= 45 && mult > 0.0 && mult < 1.05 && pos.peak_price_sol < entry_unit_price * 1.18 {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Time-stop (45s)",
            });
        }

        // 3b. 5m time cap for non-runners (< +18% and never armed breakeven); let moonbags ride to max_hold_secs (26m)
        if held_secs >= 300 && mult < 1.18 && !pos.breakeven_armed {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Time cap (5m)",
            });
        }
        if policy.max_hold_secs > 0 && held_secs >= policy.max_hold_secs {
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: 100,
                close_account: true,
                reason: "Moonbag max hold",
            });
        }

        // 4. Instant Opening Take-Profit (+18% pop -> sell instant_tp_pct, e.g. 25%)
        if !pos.instant_tp_fired && policy.instant_tp_pct > 0 && mult >= 1.18 {
            pos.instant_tp_fired = true;
            self.twap_state.insert(*mint, (now_secs, 0));
            return Some(ExitOrder {
                mint: *mint,
                sell_pct: policy.instant_tp_pct,
                close_account: policy.instant_tp_pct >= 100,
                reason: "Instant TP1",
            });
        }

        // 4b. Fhbh1D 15%-every-2s Exponential TWAP Drip while >= +18%, leaving a ~30% moonbag to trail
        let rem_ratio = if pos.initial_tokens > 0.0 {
            current_ui_tokens / pos.initial_tokens
        } else {
            1.0
        };
        if pos.entry_price_sol >= 0.50 && pos.instant_tp_fired && mult >= 1.18 && rem_ratio > 0.32 {
            let mut twap = self.twap_state.entry(*mint).or_insert((pos.opened_at_secs, 0));
            let (last_drip, drips) = *twap;
            if drips < 6 && now_secs.saturating_sub(last_drip) >= 2 {
                *twap = (now_secs, drips + 1);
                return Some(ExitOrder {
                    mint: *mint,
                    sell_pct: 15,
                    close_account: false,
                    reason: "TWAP drip (15%/2s)",
                });
            }
        }

        // 5. Staged exit ladder steps (synced from Settings UI)
        if pos.fired_steps.len() < policy.ladder.len() {
            pos.fired_steps.resize(policy.ladder.len(), false);
        }
        for (idx, step) in policy.ladder.iter().enumerate() {
            if pos.fired_steps.get(idx).copied().unwrap_or(true) {
                continue;
            }
            let hit = match step.trigger {
                LadderTrigger::PnlMultiple(target) => mult >= target,
                LadderTrigger::TimeSecs(secs) => held_secs >= secs,
            };
            if hit {
                pos.fired_steps[idx] = true;
                return Some(ExitOrder {
                    mint: *mint,
                    sell_pct: step.sell_pct,
                    close_account: step.sell_pct >= 100,
                    reason: "Auto exit ladder",
                });
            }
        }

        None
    }

    pub fn get_position(&self, mint: &PubkeyBytes) -> Option<ManagedPosition> {
        self.positions.get(mint).map(|p| p.clone())
    }

    /// Refreshes a Pump.fun position's `creator_vault` (`dex_accounts[9]`) from the live on-chain
    /// bonding curve (`buf[49..81]`) so mid-trade Fee-Sharing / CTO creator changes never cause Custom(2006) on sells.
    pub fn sync_creator_vault(&self, mint: &PubkeyBytes, live_vault: PubkeyBytes) {
        if live_vault == [0u8; 32] {
            return;
        }
        if let Some(mut entry) = self.positions.get_mut(mint) {
            if entry.dex_accounts.len() > 9 && entry.dex_accounts[9] != live_vault {
                entry.dex_accounts[9] = live_vault;
            }
        }
    }

    pub fn list_positions(&self) -> Vec<ManagedPosition> {
        self.positions.iter().map(|e| e.value().clone()).collect()
    }

    #[inline]
    pub fn has_open_positions(&self) -> bool {
        !self.positions.is_empty()
    }

    /// Updates the live on-chain raw token balance for a tracked position (used by Dev-Dump, Max-5 cap, and manual sells).
    #[inline]
    pub fn set_token_balance(&self, mint: &PubkeyBytes, raw_balance: u64) {
        if let Some(mut pos) = self.positions.get_mut(mint) {
            pos.token_balance = raw_balance;
        }
    }

    /// O(N<=5) lookup of all open confirmed positions whose creator/KOL matches `signer` (0-block Dev-Dump / Mirrored KOL Exit).
    #[inline]
    pub fn find_by_creator(&self, signer: &PubkeyBytes) -> Vec<ManagedPosition> {
        if self.positions.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for entry in self.positions.iter() {
            let p = entry.value();
            if p.token_balance > 0 && p.creator.as_ref() == Some(signer) {
                out.push(p.clone());
            }
        }
        out
    }

    pub fn remove_position(&self, mint: &PubkeyBytes) {
        self.positions.remove(mint);
        self.twap_state.remove(mint);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exits::builder::{build_protected_sell_instructions, SellTxParams};
    use crate::exits::lighthouse::sell_dedupe_min_balance;

    #[test]
    fn lighthouse_sell_dedupe_and_close_account_on_100_pct() {
        // 1,000,000 tokens, 25% sell -> post-sell balance = 750,000 -> min_balance = 750,001
        assert_eq!(sell_dedupe_min_balance(1_000_000, 25), 750_001);
        assert_eq!(sell_dedupe_min_balance(1_000_000, 100), 1);

        let ixs = build_protected_sell_instructions(&SellTxParams {
            sell_router_program: [0xCC; 32],
            target_dex_program: [0x40; 32],
            venue: Venue::PumpFun,
            hot_wallet: [0x01; 32],
            bot_token_account: [0x02; 32],
            pool_or_curve: [0x03; 32],
            is_token_2022: false,
            pre_balance: 1_000_000,
            sell_pct: 100,
            min_quote_out: 100,
            min_liquidity_floor_lamports: 10_000_000_000,
            current_slot: 500_000,
            close_on_full_exit: true,
            cpi_accounts: vec![],
        });
        let labels: Vec<&str> = ixs.iter().map(|i| i.label).collect();
        assert_eq!(
            labels,
            vec![
                "lighthouse_assert_token_balance",
                "lighthouse_assert_pumpfun_liquidity",
                "program4_percentage_sell",
                "token_close_account_rent_reclaim",
                "lighthouse_assert_clock_slot",
            ]
        );
    }

    #[test]
    fn staged_exit_ladder_and_same_slot_tp() {
        let engine = ExitEngine::new(ExitPolicy::default());
        let mint = [0xAA; 32];
        let tp = engine.register_position(
            ManagedPosition {
                mint,
                pool_or_curve: [0xBB; 32],
                bot_token_account: [0xCC; 32],
                venue: Venue::PumpFun,
                is_token_2022: false,
                opened_at_secs: 1_000,
                opened_slot: 500,
                entry_price_sol: 1.0,
                token_balance: 1_000_000,
                initial_tokens: 1_000_000.0,
                peak_price_sol: 1e-6,
                breakeven_armed: false,
                instant_tp_fired: true,
                dex_accounts: vec![],
                same_slot_tp_fired: false,
                fired_steps: vec![],
                creator: None,
                kol_id: None,
            },
            true,
        );
        assert_eq!(tp.unwrap().sell_pct, 25);

        // 1.6x hits rung 0 (15%)
        let r0 = engine.on_price_update(&mint, 1.6e-6, 1_000_000.0, 30_000_000_000, 1_010).unwrap();
        assert_eq!(r0.sell_pct, 15);

        // 2.6x hits rung 1 (33%)
        let r1 = engine.on_price_update(&mint, 2.6e-6, 850_000.0, 30_000_000_000, 1_020).unwrap();
        assert_eq!(r1.sell_pct, 33);

        // Curve liquidity drop below 10 SOL triggers immediate 100% exit + close
        let floor = engine.on_price_update(&mint, 2.0e-6, 569_500.0, 5_000_000_000, 1_025).unwrap();
        assert_eq!(floor.sell_pct, 100);
        assert!(floor.close_account);
    }
}
