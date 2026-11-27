//! Lock-free balance cache (`DashMap<PubkeyBytes, u64>`) and KOL watchlist registry.

use std::sync::Arc;
use dashmap::DashMap;
use serde::Deserialize;
use tracing::{info, warn};

use super::types::{pubkey_from_bs58, KolRule, PubkeyBytes};

/// Lock-free account balance cache updated continuously by Yellowstone Geyser gRPC.
/// Stores native lamports for system wallets and SPL/Token-2022 `amount` (offset 64) for token accounts.
#[derive(Clone, Default)]
pub struct BalanceCache {
    balances: Arc<DashMap<PubkeyBytes, u64>>,
}

impl BalanceCache {
    pub fn new() -> Self {
        Self {
            balances: Arc::new(DashMap::new()),
        }
    }

    #[inline]
    pub fn set(&self, account: PubkeyBytes, balance: u64) {
        self.balances.insert(account, balance);
    }

    #[inline]
    pub fn get(&self, account: &PubkeyBytes) -> Option<u64> {
        self.balances.get(account).map(|v| *v)
    }

    /// Updates a token account balance directly from raw SPL / Token-2022 account bytes (`data[64..72]`).
    #[inline]
    pub fn update_from_spl_data(&self, account: PubkeyBytes, data: &[u8]) -> Option<u64> {
        if data.len() < 72 {
            return None;
        }
        let amount = u64::from_le_bytes(data[64..72].try_into().ok()?);
        self.set(account, amount);
        Some(amount)
    }

    pub fn len(&self) -> usize {
        self.balances.len()
    }

    pub fn is_empty(&self) -> bool {
        self.balances.is_empty()
    }
}

/// Lock-free active KOL watchlist keyed by KOL signer wallet pubkey for O(1) shred-time lookup.
#[derive(Clone, Default)]
pub struct WatchlistCache {
    by_wallet: Arc<DashMap<PubkeyBytes, KolRule>>,
}

impl WatchlistCache {
    pub fn new() -> Self {
        Self {
            by_wallet: Arc::new(DashMap::new()),
        }
    }

    pub fn upsert(&self, rule: KolRule) {
        self.by_wallet.insert(rule.pubkey, rule);
    }

    pub fn remove_by_wallet(&self, wallet: &PubkeyBytes) {
        self.by_wallet.remove(wallet);
    }

    pub fn remove_by_id(&self, id: &str) {
        self.by_wallet.retain(|_, rule| rule.id != id);
    }

    pub fn list_ids(&self) -> Vec<String> {
        self.by_wallet.iter().map(|e| e.value().id.clone()).collect()
    }

    pub fn replace_all(&self, rules: Vec<KolRule>) {
        self.by_wallet.clear();
        for r in rules {
            self.by_wallet.insert(r.pubkey, r);
        }
    }

    #[inline]
    pub fn match_signer(&self, signer: &PubkeyBytes) -> Option<KolRule> {
        self.by_wallet
            .get(signer)
            .filter(|r| r.enabled)
            .map(|r| r.clone())
    }

    /// All accounts (KOL wallets + optional SPL quote accounts) that Geyser should subscribe to.
    pub fn watched_accounts(&self) -> Vec<PubkeyBytes> {
        let mut out = Vec::with_capacity(self.by_wallet.len() * 2);
        for entry in self.by_wallet.iter() {
            let r = entry.value();
            if !r.enabled {
                continue;
            }
            out.push(r.pubkey);
            if let Some(ata) = r.quote_token_account {
                out.push(ata);
            }
        }
        out
    }

    /// Loads the active KOL watchlist from Step 3's `GET /api/kols` REST endpoint.
    pub async fn sync_from_control_plane(
        &self,
        api_base_url: &str,
        bearer_token: &str,
    ) -> Result<usize, reqwest::Error> {
        #[derive(Deserialize)]
        struct ApiKol {
            id: String,
            label: String,
            pubkey: String,
            quote: String,
            #[serde(rename = "triggerMargin")]
            trigger_margin: f64,
            #[serde(rename = "buySizeSol")]
            buy_size_sol: f64,
            #[serde(rename = "maxCurveReserveSol")]
            max_curve_reserve_sol: f64,
            enabled: bool,
        }
        #[derive(Deserialize)]
        struct KolsResponse {
            items: Vec<ApiKol>,
        }

        let url = format!("{}/api/kols", api_base_url.trim_end_matches('/'));
        let client = reqwest::Client::new();
        let resp: KolsResponse = client
            .get(&url)
            .bearer_auth(bearer_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut rules = Vec::with_capacity(resp.items.len());
        for k in resp.items {
            match pubkey_from_bs58(&k.pubkey) {
                Ok(pubkey) => {
                    let is_spl = k.quote != "SOL";
                    let scale = match k.quote.as_str() {
                        "USDC" => 1_000_000.0,
                        "BONK" => 100_000.0,
                        _ => 1_000_000_000.0,
                    };
                    rules.push(KolRule {
                        id: k.id,
                        label: k.label,
                        pubkey,
                        quote_token_account: None,
                        is_spl_quote: is_spl,
                        margin_lamports: (k.trigger_margin * scale).max(1.0) as u64,
                        buy_size_lamports: (k.buy_size_sol * 1_000_000_000.0) as u64,
                        max_curve_reserve_lamports: (k.max_curve_reserve_sol * 1_000_000_000.0) as u64,
                        enabled: k.enabled,
                    });
                }
                Err(e) => warn!(kol = %k.id, error = %e, "Skipping KOL with invalid base58 pubkey"),
            }
        }
        let count = rules.len();
        self.replace_all(rules);
        info!(count, "Synced KOL watchlist from control plane");
        Ok(count)
    }
}
