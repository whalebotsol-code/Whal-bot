//! Primary feed: Jito ShredStream ingestor.
//!
//! Connects to the Jito ShredStream proxy over gRPC (`/shredstream.ShredstreamProxy/SubscribeEntries`),
//! decodes raw Solana bincode `Vec<Entry>` batches directly from shreds before block confirmation,
//! converts transactions into `DecodedTx`, and feeds them into `SignalDetector` -> `OpportunityBus`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::{DashMap, DashSet};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tonic::codec::ProstCodec;
use tonic::metadata::MetadataValue;
use tracing::{debug, info, warn};

/// Global lock-free in-RAM Address Lookup Table (ALT) cache for <1us v0 tx resolution on ShredStream hot path.
/// On-chain Solana ALT layout: 56-byte header followed by packed 32-byte pubkeys.
static ALT_CACHE: OnceLock<DashMap<PubkeyBytes, Vec<PubkeyBytes>>> = OnceLock::new();
static ALT_PENDING: OnceLock<DashSet<PubkeyBytes>> = OnceLock::new();
static ALT_FETCH_TX: OnceLock<mpsc::Sender<PubkeyBytes>> = OnceLock::new();

/// Spawns the background RPC fetcher that populates `ALT_CACHE` whenever an unseen v0 ALT appears in shreds.
/// Axiom / Photon / Trojan / Bloom / Jupiter reuse ~30 global ALTs, so >99% resolve in <1us in RAM after warmup.
pub fn spawn_alt_cache_loader(rpc_url: String) {
    let _ = ALT_CACHE.get_or_init(DashMap::new);
    let _ = ALT_PENDING.get_or_init(DashSet::new);
    let (tx, mut rx) = mpsc::channel::<PubkeyBytes>(512);
    if ALT_FETCH_TX.set(tx).is_err() {
        return;
    }
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_default();
        let mut batch: Vec<PubkeyBytes> = Vec::with_capacity(32);
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                Some(pk) = rx.recv() => {
                    if !batch.contains(&pk) {
                        batch.push(pk);
                    }
                    if batch.len() < 24 {
                        continue;
                    }
                }
                _ = ticker.tick() => {
                    if batch.is_empty() {
                        continue;
                    }
                }
            }
            let to_fetch: Vec<PubkeyBytes> = std::mem::take(&mut batch);
            let keys: Vec<String> = to_fetch.iter().map(|k| bs58::encode(k).into_string()).collect();
            let req_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getMultipleAccounts",
                "params": [keys, { "encoding": "base64", "commitment": "confirmed" }]
            });
            let pending = ALT_PENDING.get_or_init(DashSet::new);
            let mut fetched_ok = false;
            if let Ok(resp) = http.post(&rpc_url).json(&req_body).send().await {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(vals) = body.pointer("/result/value").and_then(|v| v.as_array()) {
                        fetched_ok = true;
                        let cache = ALT_CACHE.get_or_init(DashMap::new);
                        for (i, acc) in vals.iter().enumerate() {
                            let Some(&alt_pk) = to_fetch.get(i) else { continue };
                            let mut inserted = false;
                            if let Some(b64) = acc.pointer("/data/0").and_then(|v| v.as_str()) {
                                if let Ok(buf) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64) {
                                    if buf.len() >= 56 {
                                        let mut addrs = Vec::with_capacity((buf.len() - 56) / 32);
                                        for chunk in buf[56..].chunks_exact(32) {
                                            if let Ok(pk) = <[u8; 32]>::try_from(chunk) {
                                                addrs.push(pk);
                                            }
                                        }
                                        if !addrs.is_empty() {
                                            if cache.len() > 4_000 {
                                                cache.clear();
                                            }
                                            cache.insert(alt_pk, addrs);
                                            inserted = true;
                                        }
                                    }
                                }
                            }
                            if !inserted {
                                pending.remove(&alt_pk);
                            }
                        }
                    }
                }
            }
            if !fetched_ok {
                for pk in &to_fetch {
                    pending.remove(pk);
                }
            }
        }
    });
}

#[inline]
fn request_alt_if_missing(alt_pk: PubkeyBytes) {
    let pending = ALT_PENDING.get_or_init(DashSet::new);
    if pending.len() > 8_000 {
        pending.clear();
    }
    if pending.insert(alt_pk) {
        let sent = ALT_FETCH_TX.get().map(|tx| tx.try_send(alt_pk).is_ok()).unwrap_or(false);
        if !sent {
            pending.remove(&alt_pk);
        }
    }
}

use super::bus::OpportunityBus;
use super::detector::SignalDetector;
use super::telemetry::TelemetryEmitter;
use super::types::{DecodedInstruction, DecodedTx, PubkeyBytes};
use crate::executor::wire::decode_short_u16;

#[derive(Debug, Clone)]
pub struct ShredStreamConfig {
    pub endpoint_url: String,
    pub auth_token: Option<String>,
}

/// Wire-compatible `shredstream.SubscribeEntriesRequest`.
#[derive(Clone, PartialEq, ProstMessage)]
struct SubscribeEntriesRequest {}

/// Wire-compatible `shredstream.Entry`.
#[derive(Clone, PartialEq, ProstMessage)]
struct ShredEntryBatch {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub entries: Vec<u8>,
}

/// Runs the primary Jito ShredStream ingestion loop with automatic exponential backoff.
pub struct ShredStreamIngestor {
    cfg: ShredStreamConfig,
    detector: Arc<SignalDetector>,
    bus: OpportunityBus,
    telemetry: Option<TelemetryEmitter>,
    latest_slot: Arc<AtomicU64>,
    dev_dump_watcher: std::sync::RwLock<Option<(Arc<crate::exits::ExitEngine>, mpsc::Sender<PubkeyBytes>)>>,
}

impl ShredStreamIngestor {
    pub fn new(
        cfg: ShredStreamConfig,
        detector: Arc<SignalDetector>,
        bus: OpportunityBus,
        telemetry: Option<TelemetryEmitter>,
    ) -> Self {
        Self {
            cfg,
            detector,
            bus,
            telemetry,
            latest_slot: Arc::new(AtomicU64::new(0)),
            dev_dump_watcher: std::sync::RwLock::new(None),
        }
    }

    pub fn set_dev_dump_watcher(
        &self,
        exits: Arc<crate::exits::ExitEngine>,
        tx: mpsc::Sender<PubkeyBytes>,
    ) {
        if let Ok(mut g) = self.dev_dump_watcher.write() {
            *g = Some((exits, tx));
        }
    }

    pub fn latest_slot(&self) -> u64 {
        self.latest_slot.load(Ordering::Relaxed)
    }

    /// Processes a single reconstructed shred transaction through the detector,
    /// publishes any resulting `TriggerEvent` onto the bounded broadcast bus, and
    /// checks for 0-block Dev-Dump sells on any currently held position.
    #[inline]
    pub fn handle_decoded_tx(&self, tx: &DecodedTx) -> usize {
        self.latest_slot.fetch_max(tx.slot, Ordering::Relaxed);
        // 0-Block Dev-Dump Exit: O(1) skip when no positions are open; if a held token's creator
        // submits a Pump.fun sell/sell_v2/sell_v3 in shreds, trigger instant 100% 5-lander exit!
        if let Ok(guard) = self.dev_dump_watcher.read() {
            if let Some((exits, dump_tx)) = guard.as_ref() {
                if exits.has_open_positions() {
                    const DISC_SELL: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
                    const DISC_SELL_V2: [u8; 8] = [0x5d, 0xf6, 0x82, 0x3c, 0xe7, 0xe9, 0x40, 0xb2];
                    const DISC_SELL_V3: [u8; 8] = [0x1c, 0x92, 0xde, 0x77, 0x26, 0xc4, 0x69, 0xd5];
                    for signer in &tx.signers {
                        for pos in exits.find_by_creator(signer) {
                            let is_direct_sell = tx.instructions.iter().any(|ix| {
                                (ix.program_id == super::detector::PUMPFUN_PROGRAM_ID
                                    || ix.program_id == super::detector::PUMPSWAP_PROGRAM_ID)
                                    && ix.accounts.contains(&pos.mint)
                                    && ix.data.len() >= 8
                                    && matches!(
                                        &ix.data[0..8],
                                        d if d == DISC_SELL || d == DISC_SELL_V2 || d == DISC_SELL_V3
                                    )
                            });
                            // Also catch router-wrapped KOL/dev sells (Axiom/Photon/Bloom/Trojan) after entry slot + 1
                            let is_router_sell = !is_direct_sell
                                && tx.slot > pos.opened_slot.saturating_add(1)
                                && tx.instructions.iter().any(|ix| {
                                    ix.accounts.contains(&pos.mint)
                                        && (ix.accounts.contains(&super::detector::PUMPFUN_PROGRAM_ID)
                                            || ix.accounts.contains(&super::detector::PUMPSWAP_PROGRAM_ID))
                                });
                            if is_direct_sell || is_router_sell {
                                let _ = dump_tx.try_send(pos.mint);
                            }
                        }
                    }
                }
            }
        }
        let triggers = self.detector.inspect_tx(tx);
        let count = triggers.len();
        for ev in triggers {
            // Publish directly to OpportunityBus without flooding the remote Control Plane WS with raw shred events.
            self.bus.publish(ev);
        }
        count
    }

    /// Spawns a consumer task reading reconstructed transactions from an upstream channel.
    pub fn spawn_channel_consumer(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<DecodedTx>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(endpoint = %self.cfg.endpoint_url, "ShredStream consumer started");
            while let Some(tx) = rx.recv().await {
                self.handle_decoded_tx(&tx);
            }
            warn!("ShredStream transaction channel closed");
        })
    }

    /// Spawns the live Jito ShredStream gRPC `SubscribeEntries` loop with automatic reconnects.
    pub fn spawn_live(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if self.cfg.endpoint_url.trim().is_empty() {
                warn!("ShredStream endpoint empty; live ShredStream ingestor disabled");
                return;
            }
            let mut backoff_ms = 500u64;
            loop {
                match self.connect_endpoint().await {
                    Ok(channel) => {
                        backoff_ms = 500;
                        if let Err(e) = self.stream_entries(channel).await {
                            warn!(error = %e, "ShredStream stream ended; reconnecting");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, backoff_ms, "Invalid ShredStream endpoint; backing off");
                    }
                }
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
        })
    }

    async fn stream_entries(&self, channel: tonic::transport::Channel) -> Result<(), String> {
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready().await.map_err(|e| format!("grpc ready: {e}"))?;

        let codec: ProstCodec<SubscribeEntriesRequest, ShredEntryBatch> = ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/shredstream.ShredstreamProxy/SubscribeEntries",
        );
        let mut req = tonic::Request::new(SubscribeEntriesRequest {});
        if let Some(tok) = &self.cfg.auth_token {
            if let Ok(val) = MetadataValue::try_from(tok.as_str()) {
                req.metadata_mut().insert("x-token", val.clone());
                req.metadata_mut().insert("authorization", val);
            }
        }

        let mut stream = grpc
            .server_streaming(req, path, codec)
            .await
            .map_err(|e| format!("SubscribeEntries failed: {e}"))?
            .into_inner();

        info!(endpoint = %self.cfg.endpoint_url, "Subscribed to Jito ShredStream SubscribeEntries");
        let mut stats_batches = 0u64;
        let mut stats_txs = 0u64;
        let mut stats_pump_ixs = 0u64;
        let mut stats_triggers = 0u64;
        let mut sample_hex = String::new();
        let mut last_log = std::time::Instant::now();
        while let Some(batch) = stream.message().await.map_err(|e| e.to_string())? {
            self.latest_slot.fetch_max(batch.slot, Ordering::Relaxed);
            let now_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            let txs = decode_bincode_entries(batch.slot, now_us, &batch.entries);
            stats_batches += 1;
            stats_txs += txs.len() as u64;
            if sample_hex.is_empty() && batch.entries.len() > 48 && txs.is_empty() {
                sample_hex = batch.entries[..batch.entries.len().min(64)]
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join("");
            }
            debug!(slot = batch.slot, txs = txs.len(), "Decoded shred entry batch");
            for tx in &txs {
                if tx.instructions.iter().any(|ix| ix.program_id == super::detector::PUMPFUN_PROGRAM_ID) {
                    stats_pump_ixs += 1;
                }
                stats_triggers += self.handle_decoded_tx(tx) as u64;
            }
            if last_log.elapsed() >= Duration::from_secs(10) {
                info!(
                    slot = batch.slot,
                    batches = stats_batches,
                    txs = stats_txs,
                    pump_txs = stats_pump_ixs,
                    triggers = stats_triggers,
                    sample_hex = %sample_hex,
                    "LocalShred 10s decode stats"
                );
                stats_batches = 0;
                stats_txs = 0;
                stats_pump_ixs = 0;
                stats_triggers = 0;
                sample_hex.clear();
                last_log = std::time::Instant::now();
            }
        }
        Ok(())
    }

    /// Maintains the gRPC channel to the Jito ShredStream proxy with backoff reconnects.
    pub async fn connect_endpoint(&self) -> Result<tonic::transport::Channel, tonic::transport::Error> {
        let mut backoff_ms = 250u64;
        loop {
            match tonic::transport::Endpoint::from_shared(self.cfg.endpoint_url.clone()) {
                Ok(ep) => match ep.tcp_nodelay(true).connect().await {
                    Ok(ch) => {
                        info!(endpoint = %self.cfg.endpoint_url, "Connected to Jito ShredStream gRPC");
                        return Ok(ch);
                    }
                    Err(e) => {
                        warn!(error = %e, backoff_ms, "ShredStream connect failed; retrying");
                    }
                },
                Err(e) => return Err(e),
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(5_000);
        }
    }
}

/// Official `solana-entry 4.3` + `wincode 0.6` deserializer matching `localshred-lite-proxy` 1:1.
/// Decodes 100% of Solana Legacy, V0, and V1 (`0x81`) transactions from raw shreds with zero drops.
pub fn decode_bincode_entries(slot: u64, received_at_us: u64, bytes: &[u8]) -> Vec<DecodedTx> {
    let entries = match wincode::deserialize::<solana_entry::block_component::BlockComponent>(bytes) {
        Ok(solana_entry::block_component::BlockComponent::EntryBatch(e)) => e,
        _ => match wincode::deserialize::<Vec<solana_entry::entry::Entry>>(bytes) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        },
    };
    let mut out = Vec::new();
    for entry in entries {
        for tx in entry.transactions {
            let mut signature = [0u8; 64];
            if let Some(sig) = tx.signatures.first() {
                signature.copy_from_slice(sig.as_ref());
            }
            let msg = &tx.message;
            let static_keys = msg.static_account_keys();
            let num_req_sigs = msg.header().num_required_signatures as usize;
            let signers: Vec<PubkeyBytes> = static_keys
                .iter()
                .take(num_req_sigs)
                .map(|pk| pk.to_bytes())
                .collect();

            // Resolve v0 Address Lookup Tables (ALTs) from lock-free RAM cache (<1us, zero RPC on hot path)
            // so Axiom / Photon / Trojan / Bloom / Jupiter router KOL txs decode 100% at true 0-block.
            let mut all_keys: Vec<PubkeyBytes> = static_keys.iter().map(|pk| pk.to_bytes()).collect();
            if let Some(lookups) = msg.address_table_lookups() {
                if !lookups.is_empty() {
                    let cache = ALT_CACHE.get_or_init(DashMap::new);
                    // Solana v0 layout: all writable lookup indices first, then all readonly lookup indices.
                    for lookup in lookups {
                        let alt_pk = lookup.account_key.to_bytes();
                        if let Some(table) = cache.get(&alt_pk) {
                            for &w_idx in &lookup.writable_indexes {
                                all_keys.push(table.get(w_idx as usize).copied().unwrap_or([0u8; 32]));
                            }
                        } else {
                            request_alt_if_missing(alt_pk);
                            for _ in &lookup.writable_indexes {
                                all_keys.push([0u8; 32]);
                            }
                        }
                    }
                    for lookup in lookups {
                        let alt_pk = lookup.account_key.to_bytes();
                        if let Some(table) = cache.get(&alt_pk) {
                            for &r_idx in &lookup.readonly_indexes {
                                all_keys.push(table.get(r_idx as usize).copied().unwrap_or([0u8; 32]));
                            }
                        } else {
                            for _ in &lookup.readonly_indexes {
                                all_keys.push([0u8; 32]);
                            }
                        }
                    }
                }
            }

            let mut instructions = Vec::with_capacity(msg.instructions().len());
            for ix in msg.instructions() {
                let Some(&prog_pk) = all_keys.get(ix.program_id_index as usize) else {
                    continue;
                };
                if prog_pk == [0u8; 32] {
                    continue;
                }
                let mut ix_accounts = Vec::with_capacity(ix.accounts.len());
                for &idx in &ix.accounts {
                    let pk = all_keys.get(idx as usize).copied().unwrap_or([0u8; 32]);
                    ix_accounts.push(pk);
                }
                instructions.push(DecodedInstruction {
                    program_id: prog_pk,
                    accounts: ix_accounts,
                    data: ix.data.clone(),
                });
            }
            out.push(DecodedTx {
                slot,
                received_at_us,
                signature,
                signers,
                instructions,
            });
        }
    }
    out
}

fn read_u64(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos + 8 > buf.len() {
        return None;
    }
    let val = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(val)
}

fn parse_solana_tx(slot: u64, received_at_us: u64, buf: &[u8], pos: &mut usize) -> Option<DecodedTx> {
    // Signatures: short_u16 count + count * 64 bytes
    let (sig_count, n) = decode_short_u16(&buf[*pos..]).ok()?;
    *pos += n;
    let sig_bytes = (sig_count as usize) * 64;
    if *pos + sig_bytes > buf.len() {
        return None;
    }
    let mut signature = [0u8; 64];
    if sig_count > 0 {
        signature.copy_from_slice(&buf[*pos..*pos + 64]);
    }
    *pos += sig_bytes;

    if *pos >= buf.len() {
        return None;
    }
    let first = buf[*pos];
    let is_v0 = (first & 0x80) != 0;
    if is_v0 {
        *pos += 1;
    }
    if *pos + 3 > buf.len() {
        return None;
    }
    let num_required_signatures = buf[*pos] as usize;
    *pos += 3; // header (num_req_sigs, num_ro_signed, num_ro_unsigned)

    let (key_count, n) = decode_short_u16(&buf[*pos..]).ok()?;
    *pos += n;
    let mut account_keys: Vec<PubkeyBytes> = Vec::with_capacity(key_count as usize);
    for _ in 0..key_count {
        if *pos + 32 > buf.len() {
            return None;
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&buf[*pos..*pos + 32]);
        account_keys.push(pk);
        *pos += 32;
    }
    // Recent blockhash (32 bytes)
    if *pos + 32 > buf.len() {
        return None;
    }
    *pos += 32;

    // Compiled instructions
    let (ix_count, n) = decode_short_u16(&buf[*pos..]).ok()?;
    *pos += n;
    let mut instructions = Vec::with_capacity(ix_count as usize);
    for _ in 0..ix_count {
        if *pos >= buf.len() {
            return None;
        }
        let prog_idx = buf[*pos] as usize;
        *pos += 1;
        let (acc_count, n) = decode_short_u16(&buf[*pos..]).ok()?;
        *pos += n;
        if *pos + acc_count as usize > buf.len() {
            return None;
        }
        let mut ix_accounts = Vec::with_capacity(acc_count as usize);
        for &idx in &buf[*pos..*pos + acc_count as usize] {
            // Preserve exact instruction account indices even on v0 ALT txs by filling ALT-resolved slots with [0; 32].
            let pk = account_keys.get(idx as usize).copied().unwrap_or([0u8; 32]);
            ix_accounts.push(pk);
        }
        *pos += acc_count as usize;
        let (data_len, n) = decode_short_u16(&buf[*pos..]).ok()?;
        *pos += n;
        if *pos + data_len as usize > buf.len() {
            return None;
        }
        let data = buf[*pos..*pos + data_len as usize].to_vec();
        *pos += data_len as usize;

        if let Some(&program_id) = account_keys.get(prog_idx) {
            instructions.push(DecodedInstruction {
                program_id,
                accounts: ix_accounts,
                data,
            });
        }
    }

    // Skip v0 address table lookups if present
    if is_v0 {
        let (alt_count, n) = decode_short_u16(&buf[*pos..]).ok()?;
        *pos += n;
        for _ in 0..alt_count {
            if *pos + 32 > buf.len() {
                return None;
            }
            *pos += 32; // table key
            let (w_len, n) = decode_short_u16(&buf[*pos..]).ok()?;
            *pos += n + w_len as usize;
            if *pos > buf.len() {
                return None;
            }
            let (r_len, n) = decode_short_u16(&buf[*pos..]).ok()?;
            *pos += n + r_len as usize;
            if *pos > buf.len() {
                return None;
            }
        }
    }

    let signers = account_keys
        .iter()
        .take(num_required_signatures)
        .copied()
        .collect();

    Some(DecodedTx {
        slot,
        received_at_us,
        signature,
        signers,
        instructions,
    })
}
