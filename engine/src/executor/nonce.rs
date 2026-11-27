//! Lock-free round-robin Durable Nonce Pool (8+ accounts) for distinct-signature fan-out,
//! with live Solana RPC `getMultipleAccounts` durable-nonce hash synchronization.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use tracing::{debug, info, warn};

use crate::ingest::{pubkey_to_bs58, PubkeyBytes};

/// Extracts the 32-byte durable nonce blockhash from raw 80-byte Solana `NonceState::Initialized` data.
/// Layout: `version: u32 (0..4) | state: u32 (4..8, 1=Initialized) | authority: [u8;32] (8..40) | blockhash: [u8;32] (40..72) | fee_calc: u64 (72..80)`
pub fn parse_nonce_hash_from_account_data(data: &[u8]) -> Option<[u8; 32]> {
    if data.len() < 72 {
        return None;
    }
    let state = u32::from_le_bytes(data[4..8].try_into().ok()?);
    if state != 1 {
        return None;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&data[40..72]);
    Some(hash)
}

#[derive(Debug, Clone)]
pub struct NonceAccount {
    pub pubkey: PubkeyBytes,
    pub nonce_hash: [u8; 32],
    pub ready: bool,
}

#[derive(Clone)]
pub struct NoncePool {
    accounts: Arc<RwLock<Vec<NonceAccount>>>,
    cursor: Arc<AtomicUsize>,
}

impl NoncePool {
    pub fn new(accounts: Vec<NonceAccount>) -> Self {
        Self {
            accounts: Arc::new(RwLock::new(accounts)),
            cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Replaces the pool contents on hot-reload or nonce creation.
    pub fn replace(&self, accounts: Vec<NonceAccount>) {
        if let Ok(mut guard) = self.accounts.write() {
            *guard = accounts;
        }
    }

    /// Updates a single nonce account's 32-byte durable blockhash and marks it ready.
    pub fn update_hash(&self, pubkey: &PubkeyBytes, hash: [u8; 32]) {
        if let Ok(mut guard) = self.accounts.write() {
            if let Some(acc) = guard.iter_mut().find(|a| &a.pubkey == pubkey) {
                acc.nonce_hash = hash;
                acc.ready = hash != [0u8; 32];
            }
        }
    }

    pub fn pubkeys(&self) -> Vec<PubkeyBytes> {
        self.accounts
            .read()
            .map(|g| g.iter().map(|a| a.pubkey).collect())
            .unwrap_or_default()
    }

    /// Claims the next ready durable nonce account in round-robin order and marks it in-flight
    /// (`ready = false`) until the RPC sync task observes its advanced hash.
    pub fn next_ready(&self) -> Option<NonceAccount> {
        let mut guard = self.accounts.write().ok()?;
        let len = guard.len();
        if len == 0 {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
        for offset in 0..len {
            let idx = (start + offset) % len;
            if guard[idx].ready {
                guard[idx].ready = false;
                return Some(guard[idx].clone());
            }
        }
        None
    }

    /// Fetches fresh durable nonce hashes for all configured accounts via Solana RPC `getMultipleAccounts`.
    pub async fn refresh_from_rpc(
        &self,
        http: &reqwest::Client,
        rpc_url: &str,
    ) -> Result<usize, String> {
        let pks = self.pubkeys();
        if pks.is_empty() || rpc_url.trim().is_empty() {
            return Ok(0);
        }
        let pk_strings: Vec<String> = pks.iter().map(pubkey_to_bs58).collect();
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [pk_strings, { "encoding": "base64", "commitment": "processed" }]
        });
        let resp = http
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("nonce RPC request failed: {e}"))?;
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("nonce RPC parse failed: {e}"))?;
        let values = json
            .pointer("/result/value")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "nonce RPC missing result.value".to_string())?;

        let mut updated = 0usize;
        for (pk, entry) in pks.iter().zip(values.iter()) {
            let Some(b64) = entry
                .get("data")
                .and_then(|d| d.as_array())
                .and_then(|a| a.first())
                .and_then(|s| s.as_str())
            else {
                continue;
            };
            if let Ok(raw) = BASE64.decode(b64) {
                if let Some(hash) = parse_nonce_hash_from_account_data(&raw) {
                    self.update_hash(pk, hash);
                    updated += 1;
                }
            }
        }
        debug!(updated, total = pks.len(), "Refreshed durable nonce hashes from RPC");
        Ok(updated)
    }

    /// Spawns a background task that continuously refreshes durable nonce hashes from RPC.
    pub fn spawn_rpc_sync(self, rpc_url: String, interval_secs: u64) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let http = reqwest::Client::new();
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(2)));
            info!(interval_secs, "Durable NoncePool RPC sync task started");
            loop {
                ticker.tick().await;
                if self.is_empty() {
                    continue;
                }
                if let Err(e) = self.refresh_from_rpc(&http, &rpc_url).await {
                    warn!(error = %e, "Durable nonce RPC refresh failed");
                }
            }
        })
    }

    pub fn len(&self) -> usize {
        self.accounts.read().map(|g| g.len()).unwrap_or(0)
    }

    pub fn ready_count(&self) -> usize {
        self.accounts
            .read()
            .map(|g| g.iter().filter(|a| a.ready).count())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_initialized_solana_nonce_hash() {
        let mut raw = vec![0u8; 80];
        raw[0..4].copy_from_slice(&1u32.to_le_bytes()); // version
        raw[4..8].copy_from_slice(&1u32.to_le_bytes()); // Initialized
        let expected = [0xAB; 32];
        raw[40..72].copy_from_slice(&expected);
        assert_eq!(parse_nonce_hash_from_account_data(&raw), Some(expected));

        // Uninitialized state (0) returns None
        raw[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(parse_nonce_hash_from_account_data(&raw), None);
    }
}
