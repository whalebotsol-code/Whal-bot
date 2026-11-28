//! Sub-millisecond Trigger Detectors:
//! 1. KOL Buy Detector (Pump.fun, PumpSwap, Raydium LaunchLab)
//! 2. 0-Block Launch Detector (Pump.fun CreateV2 & Raydium LaunchLab Initialize)

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tracing::info;

use super::state::{BalanceCache, WatchlistCache};
use super::types::{
    compute_opportunity_key, DecodedInstruction, DecodedTx, LaunchFilterConfig, PubkeyBytes,
    StrategyId, TriggerEvent, TriggerKind, Venue, WSOL_MINT,
};

/// Program IDs (32-byte raw keys).
/// Pump.fun (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`)
pub const PUMPFUN_PROGRAM_ID: PubkeyBytes = [
    0x01, 0x56, 0xe0, 0xf6, 0x93, 0x66, 0x5a, 0xcf,
    0x44, 0xdb, 0x15, 0x68, 0xbf, 0x17, 0x5b, 0xaa,
    0x51, 0x89, 0xcb, 0x97, 0xf5, 0xd2, 0xff, 0x3b,
    0x65, 0x5d, 0x2b, 0xb6, 0xfd, 0x6d, 0x18, 0xb0,
];

/// PumpSwap AMM (`pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`)
pub const PUMPSWAP_PROGRAM_ID: PubkeyBytes = [
    0x0c, 0x14, 0xde, 0xfc, 0x82, 0x5e, 0xc6, 0x76,
    0x94, 0x25, 0x08, 0x18, 0xbb, 0x65, 0x40, 0x65,
    0xf4, 0x29, 0x8d, 0x31, 0x56, 0xd5, 0x71, 0xb4,
    0xd4, 0xf8, 0x09, 0x0c, 0x18, 0xe9, 0xa8, 0x63,
];

/// Raydium LaunchLab (`LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj`)
pub const LAUNCHLAB_PROGRAM_ID: PubkeyBytes = [
    0x04, 0x67, 0x9c, 0x35, 0x2f, 0x14, 0x59, 0x72,
    0xa1, 0x92, 0x83, 0x74, 0x65, 0x56, 0x47, 0x38,
    0x29, 0x1a, 0x0b, 0xfc, 0xed, 0xde, 0xcf, 0xb0,
    0xa1, 0x92, 0x83, 0x74, 0x65, 0x56, 0x47, 0x01,
];

/// 8-byte Anchor discriminators for buy and launch instructions.
pub const DISC_PUMPFUN_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
pub const DISC_PUMPFUN_BUY_V2: [u8; 8] = [0xb8, 0x17, 0xee, 0x61, 0x67, 0xc5, 0xd3, 0x3d];
pub const DISC_PUMPFUN_BUY_V3: [u8; 8] = [0x07, 0x05, 0x1d, 0xc4, 0xf5, 0x17, 0x65, 0x50];
pub const DISC_PUMPFUN_BUY_EXACT_QUOTE_V3: [u8; 8] = [0xc6, 0x2e, 0x15, 0x52, 0xb4, 0xd9, 0xe8, 0x70];
pub const DISC_PUMPFUN_BUY_EXACT_QUOTE_IN_V2: [u8; 8] = [0xc2, 0xab, 0x1c, 0x46, 0x68, 0x4d, 0x5b, 0x2f];
pub const DISC_PUMPFUN_BUY_EXACT_QUOTE_IN_V3: [u8; 8] = [0xe1, 0xf7, 0x50, 0x1e, 0xd5, 0xb3, 0x84, 0x88];
pub const DISC_PUMPFUN_BUY_EXACT_SOL_IN: [u8; 8] = [0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f];
pub const DISC_PUMPFUN_BUY_EXACT_SOL_IN_V2: [u8; 8] = [0x5e, 0x8f, 0xa6, 0x98, 0x98, 0x76, 0x18, 0x2a];
pub const DISC_PUMPFUN_BUY_EXACT_SOL_IN_V3: [u8; 8] = [0x2a, 0x39, 0xbe, 0xb0, 0x4b, 0x3d, 0x9e, 0x7f];
pub const DISC_PUMPFUN_CREATE_V2: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
pub const DISC_PUMPFUN_CREATE_V2_TOKEN2022: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];
pub const DISC_PUMPSWAP_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
pub const DISC_PUMPSWAP_BUY_EXACT_QUOTE_IN: [u8; 8] = [0xc6, 0x2e, 0x15, 0x52, 0xb4, 0xd9, 0xe8, 0x70];
pub const DISC_LAUNCHLAB_BUY_EXACT_IN: [u8; 8] = [0xfa, 0xea, 0x0d, 0x7b, 0xd5, 0x9c, 0x13, 0xec];
pub const DISC_LAUNCHLAB_INITIALIZE: [u8; 8] = [0xaf, 0xaf, 0x6d, 0x1f, 0x0d, 0x98, 0x9b, 0xed];

// Verified 2026 mainnet Pump.fun static accounts for 0.1ms zero-RPC 18-account LocalShred Buy builder.
const PUMPSWAP_GLOBAL_CONFIG: PubkeyBytes = [0x89, 0x0b, 0xa6, 0x44, 0xfe, 0x1f, 0x55, 0xaa, 0x19, 0xf1, 0x1c, 0xd2, 0xd2, 0xec, 0x14, 0xd3, 0x23, 0x3b, 0x6e, 0x0a, 0x4b, 0xea, 0xee, 0xf7, 0x2b, 0x69, 0x85, 0x8e, 0x21, 0xe1, 0x70, 0xd6];
const PUMP_GLOBAL: PubkeyBytes = [0x3a, 0x86, 0x5e, 0x69, 0xee, 0x0f, 0x54, 0x80, 0xca, 0xbc, 0xf6, 0x63, 0x57, 0xe4, 0xdc, 0x2f, 0x18, 0xd5, 0x8d, 0x45, 0xc1, 0xea, 0x74, 0x89, 0xfb, 0x37, 0x23, 0xd9, 0x79, 0x3c, 0x72, 0xa6];
const PUMP_FEE_STD: PubkeyBytes = [0x17, 0x6c, 0xb2, 0x84, 0xb5, 0xbc, 0x50, 0x95, 0xcc, 0x34, 0x30, 0x16, 0x93, 0xe2, 0xcf, 0x68, 0xe2, 0x22, 0x4b, 0x41, 0x8c, 0x63, 0x9c, 0x45, 0xc8, 0xa6, 0x85, 0x58, 0x68, 0x5a, 0x08, 0x6d];
const PUMP_FEE_MAYHEM: PubkeyBytes = [0xe8, 0x93, 0x14, 0x1f, 0xb1, 0x8e, 0x9f, 0x15, 0x74, 0xd8, 0x10, 0xe1, 0x78, 0xe1, 0x9e, 0x30, 0x60, 0x4e, 0x31, 0x75, 0xaa, 0x2e, 0x4a, 0x32, 0xdf, 0xc8, 0x60, 0x07, 0x27, 0xd1, 0x07, 0x09];
const PUMP_EVENT_AUTH: PubkeyBytes = [0xac, 0xf1, 0x36, 0xeb, 0x01, 0xfc, 0x1c, 0x4e, 0x88, 0x3d, 0x23, 0xc8, 0xb5, 0x84, 0x4a, 0xb5, 0x9a, 0x37, 0xf6, 0x6a, 0xdd, 0x57, 0xc5, 0xe9, 0xac, 0x3b, 0x53, 0xe0, 0x59, 0xd3, 0x5c, 0x64];
const PUMP_GLOBAL_VOL: PubkeyBytes = [0xfa, 0x09, 0x11, 0xa5, 0x48, 0x63, 0x41, 0x2d, 0x63, 0x1f, 0x4e, 0x07, 0x87, 0x03, 0x29, 0x6c, 0x03, 0x5f, 0x0d, 0x13, 0x33, 0xa0, 0xd9, 0xc8, 0x83, 0x8d, 0x73, 0xb7, 0x10, 0xfe, 0x6e, 0x2d];
const PUMP_FEE_CFG: PubkeyBytes = [0x6f, 0x9a, 0xb4, 0xa4, 0xf1, 0x95, 0x8d, 0xc0, 0xa9, 0xc9, 0x4c, 0x3f, 0xb7, 0x2c, 0x07, 0x99, 0x58, 0x43, 0xed, 0xa4, 0x85, 0xe3, 0xa2, 0x4f, 0x10, 0xc6, 0x93, 0x99, 0xf8, 0x19, 0x94, 0x0f];
const PUMP_FEE_PROG: PubkeyBytes = [0x0c, 0x35, 0xff, 0xa9, 0x05, 0x5a, 0x8e, 0x56, 0x8d, 0xa8, 0xf7, 0xbc, 0x07, 0x56, 0x15, 0x27, 0x4c, 0xf1, 0xc9, 0x2c, 0xa4, 0x1f, 0x40, 0x00, 0x9c, 0x51, 0x6a, 0xa4, 0x14, 0xc2, 0x7c, 0x70];
const PUMP_BUYBACK_0: PubkeyBytes = [0x43, 0x9e, 0x65, 0x10, 0xc0, 0x3d, 0x65, 0xfa, 0xd9, 0x31, 0xe8, 0x9d, 0x04, 0xbe, 0x0b, 0xb7, 0x0d, 0x51, 0x97, 0x1f, 0x51, 0xc4, 0x15, 0xfb, 0x34, 0x4c, 0x07, 0xdb, 0x41, 0x9f, 0x21, 0x22];
const ATA_PROGRAM_ID: PubkeyBytes = [0x8c, 0x97, 0x25, 0x8f, 0x4e, 0x24, 0x89, 0xf1, 0xbb, 0x3d, 0x10, 0x29, 0x14, 0x8e, 0x0d, 0x83, 0x0b, 0x5a, 0x13, 0x99, 0xda, 0xff, 0x10, 0x84, 0x04, 0x8e, 0x7b, 0xd8, 0xdb, 0xe9, 0xf8, 0x59];
const SYSTEM_PROGRAM_ID: PubkeyBytes = [0u8; 32];

fn find_pda(seeds: &[&[u8]], program_id: &PubkeyBytes) -> PubkeyBytes {
    const PDA_MARKER: &[u8] = b"ProgramDerivedAddress";
    for bump in (0u8..=255).rev() {
        let mut h = Sha256::new();
        for s in seeds {
            h.update(s);
        }
        h.update([bump]);
        h.update(program_id);
        h.update(PDA_MARKER);
        let digest = h.finalize();
        let mut cand = [0u8; 32];
        cand.copy_from_slice(&digest);
        if ed25519_dalek::VerifyingKey::from_bytes(&cand).is_err() {
            return cand;
        }
    }
    [0u8; 32]
}

/// Derives Pump.fun's `creator_vault` PDA (`["creator-vault", creator]`) for live sell refreshes.
pub fn derive_pumpfun_creator_vault(creator: &PubkeyBytes) -> PubkeyBytes {
    find_pda(&[b"creator-vault", creator], &PUMPFUN_PROGRAM_ID)
}

/// 0.1ms zero-RPC, zero-ALT 18-account Pump.fun Buy builder for Slot +0 LocalShred execution.
fn build_local_pumpfun_buy_accounts(
    mint: PubkeyBytes,
    bonding_curve: PubkeyBytes,
    creator: PubkeyBytes,
    is_token_2022: bool,
    is_mayhem: bool,
    bundled_buy_accounts: &[PubkeyBytes],
) -> Vec<PubkeyBytes> {
    // Only borrow accounts[1, 4, 8, 9, 16] when the dev buy uses the classic layout
    // ([2] == mint && [3] == bonding_curve). New 2026 BuyV2/BuyExactQuoteInV2/V3 shift
    // those indices ([1] == mint, [2] == WSOL), so derive exact PDAs locally instead.
    let is_classic_layout = bundled_buy_accounts.get(2) == Some(&mint)
        && bundled_buy_accounts.get(3) == Some(&bonding_curve);
    let pick = |idx: usize, fallback: PubkeyBytes| -> PubkeyBytes {
        if !is_classic_layout {
            return fallback;
        }
        bundled_buy_accounts
            .get(idx)
            .copied()
            .filter(|k| *k != [0u8; 32])
            .unwrap_or(fallback)
    };
    let token_prog = pick(
        8,
        if is_token_2022 {
            crate::executor::TOKEN_2022_PROGRAM_ID
        } else {
            crate::executor::SPL_TOKEN_PROGRAM_ID
        },
    );
    let fee_recipient = pick(1, if is_mayhem { PUMP_FEE_MAYHEM } else { PUMP_FEE_STD });
    let assoc_curve = pick(4, find_pda(&[&bonding_curve, &token_prog, &mint], &ATA_PROGRAM_ID));
    // On 27-account 2026 BuyV2/BuyExactQuoteInV2 layouts ([1]==mint, [2]==WSOL), Pump.fun puts
    // the exact on-chain creator_vault (including fee-sharing/cashback PDAs!) at index [16].
    let is_v2_27_layout = bundled_buy_accounts.len() >= 26
        && bundled_buy_accounts.get(1) == Some(&mint)
        && bundled_buy_accounts.get(2) == Some(&WSOL_MINT);
    let v2_creator_vault = if is_v2_27_layout {
        bundled_buy_accounts.get(16).copied().filter(|k| *k != [0u8; 32])
    } else {
        None
    };
    let creator_vault = v2_creator_vault
        .unwrap_or_else(|| pick(9, find_pda(&[b"creator-vault", &creator], &PUMPFUN_PROGRAM_ID)));
    let bc_v2 = pick(16, find_pda(&[b"bonding-curve-v2", &mint], &PUMPFUN_PROGRAM_ID));
    vec![
        PUMP_GLOBAL,        // [0] global
        fee_recipient,      // [1] fee_recipient (standard vs mayhem)
        mint,               // [2] mint
        bonding_curve,      // [3] bonding_curve
        assoc_curve,        // [4] associated_bonding_curve
        [0u8; 32],          // [5] assoc_user (substituted with ephemeral_ata in builder)
        [0u8; 32],          // [6] user (substituted with hot_wallet in builder)
        SYSTEM_PROGRAM_ID,  // [7] system_program
        token_prog,         // [8] token_program (SPL vs Token-2022)
        creator_vault,      // [9] creator_vault
        PUMP_EVENT_AUTH,    // [10] event_authority
        PUMPFUN_PROGRAM_ID, // [11] program
        PUMP_GLOBAL_VOL,    // [12] global_volume_accumulator
        [0u8; 32],          // [13] user_volume_accumulator (substituted in builder)
        PUMP_FEE_CFG,       // [14] fee_config
        PUMP_FEE_PROG,      // [15] fee_program
        bc_v2,              // [16] bonding_curve_v2
        PUMP_BUYBACK_0,     // [17] buyback_fee_recipient
    ]
}

#[inline]
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[inline]
fn read_u64(data: &[u8], off: usize) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(off..off + 8)?.try_into().ok()?))
}

fn read_borsh_string(data: &[u8], off: &mut usize) -> Option<String> {
    let len = u32::from_le_bytes(data.get(*off..*off + 4)?.try_into().ok()?) as usize;
    let start = *off + 4;
    let end = start.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    *off = end;
    Some(String::from_utf8_lossy(&data[start..end]).into_owned())
}

struct ParsedBuy {
    venue: Venue,
    mint: PubkeyBytes,
    quote_mint: PubkeyBytes,
    pool_pda: PubkeyBytes,
    estimated_spend: u64,
    ix_accounts: Vec<PubkeyBytes>,
}

struct ParsedLaunch {
    venue: Venue,
    strategy_id: StrategyId,
    mint: PubkeyBytes,
    quote_mint: PubkeyBytes,
    pool_pda: PubkeyBytes,
    creator: PubkeyBytes,
    is_token_2022: bool,
    is_mayhem: bool,
    name: String,
    symbol: String,
    uri: String,
    ix_accounts: Vec<PubkeyBytes>,
}

fn parse_buy_ix(ix: &DecodedInstruction) -> Option<ParsedBuy> {
    let disc: [u8; 8] = ix.data.get(0..8)?.try_into().ok()?;
    if ix.program_id == PUMPFUN_PROGRAM_ID {
        let is_exact_in = disc == DISC_PUMPFUN_BUY_EXACT_QUOTE_V3
            || disc == DISC_PUMPFUN_BUY_EXACT_QUOTE_IN_V2
            || disc == DISC_PUMPFUN_BUY_EXACT_QUOTE_IN_V3
            || disc == DISC_PUMPFUN_BUY_EXACT_SOL_IN
            || disc == DISC_PUMPFUN_BUY_EXACT_SOL_IN_V2
            || disc == DISC_PUMPFUN_BUY_EXACT_SOL_IN_V3;
        let is_amount_max_sol = disc == DISC_PUMPFUN_BUY
            || disc == DISC_PUMPFUN_BUY_V2
            || disc == DISC_PUMPFUN_BUY_V3;
        if is_exact_in || is_amount_max_sol {
            // Classic Pump.fun buy: [global(0), fee_recipient(1), mint(2), bonding_curve(3), ...]
            // New 2026 BuyV2 / BuyExactQuoteInV2/V3: [global(0), mint(1), WSOL(2), token_prog(3), ...]
            let is_v2_layout = ix.accounts.get(2) == Some(&WSOL_MINT);
            let (mint, pool_pda) = if is_v2_layout {
                let m = *ix.accounts.get(1)?;
                let curve = find_pda(&[b"bonding-curve", &m], &PUMPFUN_PROGRAM_ID);
                (m, curve)
            } else {
                (*ix.accounts.get(2)?, *ix.accounts.get(3)?)
            };
            let estimated_spend = if is_exact_in {
                read_u64(&ix.data, 8)?
            } else {
                // buy / buy_v2 / buy_v3(amount, max_sol_cost): compute exact x*y=k Pump.fun launch SOL cost
                // from `amount` [8..16] instead of inflated slippage ceiling `max_sol_cost` [16..24].
                let tok_amt = read_u64(&ix.data, 8)? as u128;
                let max_sol = read_u64(&ix.data, 16).unwrap_or(u64::MAX);
                const V_SOL_INIT: u128 = 30_000_000_000;
                const V_TOK_INIT: u128 = 1_073_000_000_000_000;
                if tok_amt > 0 && tok_amt < V_TOK_INIT {
                    let exact_sol = (V_SOL_INIT * tok_amt) / (V_TOK_INIT - tok_amt);
                    (exact_sol as u64).min(max_sol)
                } else {
                    max_sol
                }
            };
            return Some(ParsedBuy {
                venue: Venue::PumpFun,
                mint,
                quote_mint: WSOL_MINT,
                pool_pda,
                estimated_spend,
                ix_accounts: ix.accounts.clone(),
            });
        }
    }

    if ix.program_id == PUMPSWAP_PROGRAM_ID
        && (disc == DISC_PUMPSWAP_BUY || disc == DISC_PUMPSWAP_BUY_EXACT_QUOTE_IN)
    {
        // PumpSwap buy / buy_exact_quote_in layout: [pool(0), user(1), global_config(2), base_mint(3), quote_mint(4), ...]
        let pool_pda = *ix.accounts.get(0)?;
        let mint = *ix.accounts.get(3)?;
        let quote_mint = *ix.accounts.get(4)?;
        let estimated_spend = if disc == DISC_PUMPSWAP_BUY_EXACT_QUOTE_IN {
            read_u64(&ix.data, 8)?
        } else {
            read_u64(&ix.data, 16)?
        };
        return Some(ParsedBuy {
            venue: Venue::PumpSwap,
            mint,
            quote_mint,
            pool_pda,
            estimated_spend,
            ix_accounts: ix.accounts.clone(),
        });
    }

    if ix.program_id == LAUNCHLAB_PROGRAM_ID && disc == DISC_LAUNCHLAB_BUY_EXACT_IN {
        // Raydium LaunchLab BuyExactIn: [payer(0), auth(1), cfg(2), platform(3), pool_state(4), ..., base_mint(9), quote_mint(10)]
        let pool_pda = *ix.accounts.get(4)?;
        let mint = *ix.accounts.get(9)?;
        let quote_mint = *ix.accounts.get(10)?;
        let estimated_spend = read_u64(&ix.data, 8)?;
        return Some(ParsedBuy {
            venue: Venue::RaydiumLaunchLab,
            mint,
            quote_mint,
            pool_pda,
            estimated_spend,
            ix_accounts: ix.accounts.clone(),
        });
    }

    None
}

fn parse_launch_ix(ix: &DecodedInstruction) -> Option<ParsedLaunch> {
    let disc: [u8; 8] = ix.data.get(0..8)?.try_into().ok()?;
    if ix.program_id == PUMPFUN_PROGRAM_ID
        && (disc == DISC_PUMPFUN_CREATE_V2 || disc == DISC_PUMPFUN_CREATE_V2_TOKEN2022)
    {
        // Legacy create: [mint(0), mint_auth(1), bonding_curve(2), ..., user(7)].
        // Token-2022 create_v2: mpl_token_metadata & metadata removed -> user/creator is at index 5.
        let mint = *ix.accounts.get(0)?;
        if mint == [0u8; 32] {
            return None;
        }
        let raw_curve = ix.accounts.get(2).copied().unwrap_or([0u8; 32]);
        let pool_pda = if raw_curve != [0u8; 32] {
            raw_curve
        } else {
            find_pda(&[b"bonding-curve", &mint], &PUMPFUN_PROGRAM_ID)
        };
        let is_token_2022 = disc == DISC_PUMPFUN_CREATE_V2_TOKEN2022;
        let mut off = 8;
        let name = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        let symbol = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        let uri = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        // Borsh creator pubkey (32B) immediately follows uri in Pump.fun create/create_v2 -> exact on-chain bonding_curve.creator!
        let data_creator: Option<PubkeyBytes> = ix.data.get(off..off + 32).and_then(|s| s.try_into().ok());
        if data_creator.is_some() {
            off += 32;
        }
        let is_mayhem = is_token_2022 && ix.data.get(off).copied().unwrap_or(0) != 0;
        let creator_idx = if is_token_2022 { 5 } else { 7 };
        let creator = data_creator
            .filter(|c| *c != [0u8; 32])
            .or_else(|| ix.accounts.get(creator_idx).copied().filter(|c| *c != [0u8; 32]))
            .unwrap_or([0u8; 32]);
        if creator == [0u8; 32] {
            return None;
        }
        return Some(ParsedLaunch {
            venue: Venue::PumpFun,
            strategy_id: StrategyId::PumpFunCreateV2Snipe,
            mint,
            quote_mint: WSOL_MINT,
            pool_pda,
            creator,
            is_token_2022,
            is_mayhem,
            name,
            symbol,
            uri,
            ix_accounts: ix.accounts.clone(),
        });
    }

    if ix.program_id == LAUNCHLAB_PROGRAM_ID && disc == DISC_LAUNCHLAB_INITIALIZE {
        // Raydium LaunchLab Initialize: [payer/creator(0), ..., pool_state(4), base_mint(6), quote_mint(7)]
        let creator = *ix.accounts.get(0)?;
        let pool_pda = *ix.accounts.get(4)?;
        let mint = *ix.accounts.get(6)?;
        let quote_mint = *ix.accounts.get(7)?;
        let mut off = 8;
        let name = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        let symbol = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        let uri = read_borsh_string(&ix.data, &mut off).unwrap_or_default();
        return Some(ParsedLaunch {
            venue: Venue::RaydiumLaunchLab,
            strategy_id: StrategyId::LaunchLabInitSnipe,
            mint,
            quote_mint,
            pool_pda,
            creator,
            is_token_2022: false,
            is_mayhem: false,
            name,
            symbol,
            uri,
            ix_accounts: ix.accounts.clone(),
        });
    }

    None
}

/// High-speed signal detector operating directly on decoded ShredStream transactions.
pub struct SignalDetector {
    watchlist: WatchlistCache,
    balances: BalanceCache,
    launch_filter: RwLock<LaunchFilterConfig>,
    recent_symbols: Mutex<HashMap<String, u64>>,
    creator_launches: Mutex<HashMap<PubkeyBytes, Vec<u64>>>,
    rugged_creators: dashmap::DashSet<PubkeyBytes>,
}

impl SignalDetector {
    pub fn new(
        watchlist: WatchlistCache,
        balances: BalanceCache,
        launch_filter: LaunchFilterConfig,
    ) -> Self {
        Self {
            watchlist,
            balances,
            launch_filter: RwLock::new(launch_filter),
            recent_symbols: Mutex::new(HashMap::new()),
            creator_launches: Mutex::new(HashMap::new()),
            rugged_creators: dashmap::DashSet::new(),
        }
    }

    fn is_serial_deployer(&self, creator: PubkeyBytes, now_us: u64) -> bool {
        const ONE_HOUR_US: u64 = 3_600_000_000;
        let Ok(mut map) = self.creator_launches.lock() else {
            return false;
        };
        let entry = map.entry(creator).or_default();
        entry.retain(|&t| now_us.saturating_sub(t) < ONE_HOUR_US);
        entry.push(now_us);
        let is_repeat = entry.len() >= 2;
        if map.len() > 1_000 {
            map.retain(|_, v| {
                v.retain(|&t| now_us.saturating_sub(t) < ONE_HOUR_US);
                !v.is_empty()
            });
        }
        is_repeat
    }

    fn is_duplicate_symbol_clone(&self, symbol: &str, now_us: u64) -> bool {
        const COOLDOWN_US: u64 = 900_000_000; // 15 minutes (1:1 with Paper Mode)
        let norm: String = symbol
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();
        if norm.is_empty() {
            return false;
        }
        let Ok(mut map) = self.recent_symbols.lock() else {
            return false;
        };
        if let Some(&prev) = map.get(&norm) {
            if now_us.saturating_sub(prev) < COOLDOWN_US {
                return true;
            }
        }
        map.insert(norm, now_us);
        if map.len() > 500 {
            map.retain(|_, &mut t| now_us.saturating_sub(t) < COOLDOWN_US);
        }
        false
    }

    /// Permanently blocks a creator pubkey in RAM after a 0-block dev-dump or fail-fast rug.
    pub fn block_rugged_creator(&self, creator: PubkeyBytes) {
        if self.rugged_creators.len() > 10_000 {
            self.rugged_creators.clear();
        }
        self.rugged_creators.insert(creator);
    }

    pub fn update_launch_filter(&self, next: LaunchFilterConfig) {
        if let Ok(mut guard) = self.launch_filter.write() {
            *guard = next;
        }
    }

    /// Inspects a decoded ShredStream transaction and returns all matching `TriggerEvent`s.
    pub fn inspect_tx(&self, tx: &DecodedTx) -> Vec<TriggerEvent> {
        let mut out = Vec::new();
        let detected_at_us = now_us();

        // 1. KOL Buy Detector: check if any tx signer is on the active KOL watchlist.
        let matched_kol = tx.signers.iter().find_map(|s| self.watchlist.match_signer(s));
        if let Some(kol) = matched_kol {
            let mut found_direct = false;
            for ix in &tx.instructions {
                if let Some(buy) = parse_buy_ix(ix) {
                    found_direct = true;
                    let watched_account = kol.quote_token_account.unwrap_or(kol.pubkey);
                    let pre_balance = self.balances.get(&watched_account).unwrap_or(0);
                    let balance_threshold = pre_balance.saturating_sub(kol.margin_lamports);
                    let strategy_id = if buy.quote_mint != WSOL_MINT {
                        StrategyId::GenericQuoteSnipe
                    } else {
                        StrategyId::KolBackrun
                    };
                    let opportunity_key = compute_opportunity_key(&buy.mint, tx.slot, strategy_id);
                    info!(
                        slot = tx.slot,
                        kol = %kol.label,
                        venue = ?buy.venue,
                        buy_size_lamports = kol.buy_size_lamports,
                        "LocalShred 0-block KOL buy detected (direct)"
                    );
                    // Normalize direct Pump.fun V2/V3 KOL buys ([2] == WSOL) into the exact 18-account classic layout
                    let dex_accounts = if buy.venue == Venue::PumpFun
                        && buy.ix_accounts.get(2) == Some(&WSOL_MINT)
                    {
                        let is_t22 = buy.ix_accounts.get(3) == Some(&crate::executor::TOKEN_2022_PROGRAM_ID)
                            || buy.ix_accounts.contains(&crate::executor::TOKEN_2022_PROGRAM_ID);
                        let is_mayhem = buy.ix_accounts.contains(&PUMP_FEE_MAYHEM);
                        let mut norm = build_local_pumpfun_buy_accounts(
                            buy.mint,
                            buy.pool_pda,
                            kol.pubkey,
                            is_t22,
                            is_mayhem,
                            &[],
                        );
                        if let Some(&abc) = buy.ix_accounts.get(6) {
                            if abc != [0u8; 32] {
                                norm[4] = abc;
                            }
                        }
                        if let Some(&cv) = buy.ix_accounts.get(10) {
                            if cv != [0u8; 32] {
                                norm[9] = cv;
                            }
                        }
                        norm
                    } else {
                        buy.ix_accounts
                    };
                    out.push(TriggerEvent {
                        opportunity_key,
                        strategy_id,
                        trigger_slot: tx.slot,
                        detected_at_us,
                        venue: buy.venue,
                        mint: buy.mint,
                        quote_mint: buy.quote_mint,
                        pool_pda: buy.pool_pda,
                        dex_accounts,
                        kind: TriggerKind::KolBuy {
                            kol_id: kol.id.clone(),
                            kol_wallet: kol.pubkey,
                            watched_balance_account: watched_account,
                            is_spl_quote: kol.is_spl_quote,
                            pre_balance,
                            balance_threshold,
                            estimated_kol_spend: buy.estimated_spend,
                            buy_size_lamports: kol.buy_size_lamports,
                            max_curve_reserve_lamports: kol.max_curve_reserve_lamports,
                        },
                    });
                }
            }

            // Router-wrapped KOL buys (Axiom / Photon / Bloom / Trojan / Jupiter):
            // Scan outer router accounts for contiguous Pump.fun or PumpSwap CPI account slices at 0-block.
            let before_router = out.len();
            if !found_direct {
                'router_scan: for ix in &tx.instructions {
                    let accs = &ix.accounts;
                    if accs.len() >= 16 && accs.contains(&PUMPFUN_PROGRAM_ID) {
                        for start in 0..=accs.len().saturating_sub(16) {
                            if accs[start] == PUMP_GLOBAL {
                                // Layout B (2026 Pump.fun V2 / FlashX / Axiom / Padre routers):
                                // [0]=Global, [1]=Mint, [2]=WSOL, [3]=TokenProg, [5]=Curve, [6]=AssocCurve, [10]=CreatorVault
                                if accs[start + 2] == WSOL_MINT && accs[start + 1] != [0u8; 32] && accs[start + 1] != WSOL_MINT {
                                    let cand_mint = accs[start + 1];
                                    let cand_curve = accs[start + 5];
                                    if find_pda(&[b"bonding-curve", &cand_mint], &PUMPFUN_PROGRAM_ID) == cand_curve {
                                        let is_t22 = accs[start + 3] == crate::executor::TOKEN_2022_PROGRAM_ID;
                                        let is_mayhem = accs.contains(&PUMP_FEE_MAYHEM);
                                        let mut dex_slice = build_local_pumpfun_buy_accounts(
                                            cand_mint,
                                            cand_curve,
                                            kol.pubkey,
                                            is_t22,
                                            is_mayhem,
                                            &[],
                                        );
                                        // Require resolved creator_vault [start + 10] (if ALT is still warming up it's [0u8; 32],
                                        // and falling back to kol.pubkey guarantees Custom(2006) ConstraintSeeds on-chain).
                                        if accs[start + 10] == [0u8; 32] {
                                            continue;
                                        }
                                        // Override exact associated_bonding_curve [4] & creator_vault [9] from FlashX/V2 slice
                                        if accs[start + 6] != [0u8; 32] {
                                            dex_slice[4] = accs[start + 6];
                                        }
                                        dex_slice[9] = accs[start + 10];
                                        let watched_account = kol.quote_token_account.unwrap_or(kol.pubkey);
                                        let pre_balance = self.balances.get(&watched_account).unwrap_or(0);
                                        let balance_threshold = pre_balance.saturating_sub(kol.margin_lamports);
                                        let opportunity_key =
                                            compute_opportunity_key(&cand_mint, tx.slot, StrategyId::KolBackrun);
                                        info!(
                                            slot = tx.slot,
                                            kol = %kol.label,
                                            is_token_2022 = is_t22,
                                            "LocalShred 0-block KOL buy detected (router-wrapped Pump.fun V2 / FlashX)"
                                        );
                                        out.push(TriggerEvent {
                                            opportunity_key,
                                            strategy_id: StrategyId::KolBackrun,
                                            trigger_slot: tx.slot,
                                            detected_at_us,
                                            venue: Venue::PumpFun,
                                            mint: cand_mint,
                                            quote_mint: WSOL_MINT,
                                            pool_pda: cand_curve,
                                            dex_accounts: dex_slice,
                                            kind: TriggerKind::KolBuy {
                                                kol_id: kol.id.clone(),
                                                kol_wallet: kol.pubkey,
                                                watched_balance_account: watched_account,
                                                is_spl_quote: kol.is_spl_quote,
                                                pre_balance,
                                                balance_threshold,
                                                estimated_kol_spend: kol.buy_size_lamports,
                                                buy_size_lamports: kol.buy_size_lamports,
                                                max_curve_reserve_lamports: kol.max_curve_reserve_lamports,
                                            },
                                        });
                                        break 'router_scan;
                                    }
                                }
                                // Layout A (Classic Pump.fun router): [0]=Global, [1]=Fee, [2]=Mint, [3]=Curve
                                let cand_mint = accs[start + 2];
                                let cand_curve = accs[start + 3];
                                if cand_mint != [0u8; 32]
                                    && cand_mint != WSOL_MINT
                                    && find_pda(&[b"bonding-curve", &cand_mint], &PUMPFUN_PROGRAM_ID) == cand_curve
                                {
                                    let slice_end = (start + 18).min(accs.len());
                                    let mut dex_slice = accs[start..slice_end].to_vec();
                                    let is_t22 = dex_slice.get(8) == Some(&crate::executor::TOKEN_2022_PROGRAM_ID);
                                    let is_mayhem = dex_slice.get(1) == Some(&PUMP_FEE_MAYHEM);
                                    if dex_slice.len() < 18 {
                                        dex_slice = build_local_pumpfun_buy_accounts(
                                            cand_mint,
                                            cand_curve,
                                            kol.pubkey,
                                            is_t22,
                                            is_mayhem,
                                            &dex_slice,
                                        );
                                    }
                                    let watched_account = kol.quote_token_account.unwrap_or(kol.pubkey);
                                    let pre_balance = self.balances.get(&watched_account).unwrap_or(0);
                                    let balance_threshold = pre_balance.saturating_sub(kol.margin_lamports);
                                    let opportunity_key =
                                        compute_opportunity_key(&cand_mint, tx.slot, StrategyId::KolBackrun);
                                    info!(
                                        slot = tx.slot,
                                        kol = %kol.label,
                                        is_token_2022 = is_t22,
                                        "LocalShred 0-block KOL buy detected (router-wrapped Pump.fun)"
                                    );
                                    out.push(TriggerEvent {
                                        opportunity_key,
                                        strategy_id: StrategyId::KolBackrun,
                                        trigger_slot: tx.slot,
                                        detected_at_us,
                                        venue: Venue::PumpFun,
                                        mint: cand_mint,
                                        quote_mint: WSOL_MINT,
                                        pool_pda: cand_curve,
                                        dex_accounts: dex_slice,
                                        kind: TriggerKind::KolBuy {
                                            kol_id: kol.id.clone(),
                                            kol_wallet: kol.pubkey,
                                            watched_balance_account: watched_account,
                                            is_spl_quote: kol.is_spl_quote,
                                            pre_balance,
                                            balance_threshold,
                                            estimated_kol_spend: kol.buy_size_lamports,
                                            buy_size_lamports: kol.buy_size_lamports,
                                            max_curve_reserve_lamports: kol.max_curve_reserve_lamports,
                                        },
                                    });
                                    break 'router_scan;
                                }
                            }
                        }
                    }
                    if accs.len() >= 17 && accs.contains(&PUMPSWAP_PROGRAM_ID) {
                        for start in 0..=accs.len().saturating_sub(17) {
                            if accs[start + 4] == WSOL_MINT
                                && accs[start + 3] != WSOL_MINT
                                && accs[start + 3] != [0u8; 32]
                                && (accs[start + 12] == crate::executor::SPL_TOKEN_PROGRAM_ID
                                    || accs[start + 12] == crate::executor::TOKEN_2022_PROGRAM_ID)
                            {
                                let pool_pda = accs[start];
                                let cand_mint = accs[start + 3];
                                let slice_end = (start + 23).min(accs.len());
                                let dex_slice = accs[start..slice_end].to_vec();
                                let watched_account = kol.quote_token_account.unwrap_or(kol.pubkey);
                                let pre_balance = self.balances.get(&watched_account).unwrap_or(0);
                                let balance_threshold = pre_balance.saturating_sub(kol.margin_lamports);
                                let opportunity_key =
                                    compute_opportunity_key(&cand_mint, tx.slot, StrategyId::KolBackrun);
                                info!(
                                    slot = tx.slot,
                                    kol = %kol.label,
                                    "LocalShred 0-block KOL buy detected (router-wrapped PumpSwap)"
                                );
                                out.push(TriggerEvent {
                                    opportunity_key,
                                    strategy_id: StrategyId::KolBackrun,
                                    trigger_slot: tx.slot,
                                    detected_at_us,
                                    venue: Venue::PumpSwap,
                                    mint: cand_mint,
                                    quote_mint: WSOL_MINT,
                                    pool_pda,
                                    dex_accounts: dex_slice,
                                    kind: TriggerKind::KolBuy {
                                        kol_id: kol.id.clone(),
                                        kol_wallet: kol.pubkey,
                                        watched_balance_account: watched_account,
                                        is_spl_quote: kol.is_spl_quote,
                                        pre_balance,
                                        balance_threshold,
                                        estimated_kol_spend: kol.buy_size_lamports,
                                        buy_size_lamports: kol.buy_size_lamports,
                                        max_curve_reserve_lamports: kol.max_curve_reserve_lamports,
                                    },
                                });
                                break 'router_scan;
                            }
                        }
                        // Terminal / Padre (`term9YPb...`) compressed PumpSwap router layout:
                        // [i-1]=pool, [i]=PUMPSWAP_GLOBAL_CONFIG, [i+1]=base_mint, [i+2]=WSOL,
                        // [i+3]=user_quote_ata, [i+4]=user_base_ata, [i+5]=base_vault, [i+6]=quote_vault,
                        // [i+7]=fee_recip, [i+8]=fee_ata, [i+10]=event_auth
                        for i in 1..accs.len().saturating_sub(10) {
                            if accs[i] == PUMPSWAP_GLOBAL_CONFIG
                                && accs[i - 1] != kol.pubkey
                                && accs[i + 2] == WSOL_MINT
                                && accs[i + 1] != WSOL_MINT
                                && accs[i + 1] != [0u8; 32]
                            {
                                let pool_pda = accs[i - 1];
                                let cand_mint = accs[i + 1];
                                let base_tok_prog = if accs.contains(&crate::executor::TOKEN_2022_PROGRAM_ID) {
                                    crate::executor::TOKEN_2022_PROGRAM_ID
                                } else {
                                    crate::executor::SPL_TOKEN_PROGRAM_ID
                                };
                                let dex_slice = vec![
                                    pool_pda,                               // [0] pool
                                    kol.pubkey,                             // [1] user (replaced with hot_wallet)
                                    accs[i],                                // [2] global_config
                                    cand_mint,                              // [3] base_mint
                                    WSOL_MINT,                              // [4] quote_mint
                                    accs[i + 4],                            // [5] user_base_ata (replaced with ephemeral_ata)
                                    accs[i + 3],                            // [6] user_quote_ata (replaced with wsol_ata)
                                    accs[i + 5],                            // [7] pool_base_vault
                                    accs[i + 6],                            // [8] pool_quote_vault
                                    accs[i + 7],                            // [9] protocol_fee_recipient
                                    accs[i + 8],                            // [10] protocol_fee_recipient_ata
                                    base_tok_prog,                          // [11] base_token_program
                                    crate::executor::SPL_TOKEN_PROGRAM_ID,  // [12] quote_token_program
                                    SYSTEM_PROGRAM_ID,                      // [13] system_program
                                    ATA_PROGRAM_ID,                         // [14] associated_token_program
                                    accs[i + 10],                           // [15] event_authority
                                    PUMPSWAP_PROGRAM_ID,                    // [16] program
                                ];
                                let watched_account = kol.quote_token_account.unwrap_or(kol.pubkey);
                                let pre_balance = self.balances.get(&watched_account).unwrap_or(0);
                                let balance_threshold = pre_balance.saturating_sub(kol.margin_lamports);
                                let opportunity_key =
                                    compute_opportunity_key(&cand_mint, tx.slot, StrategyId::KolBackrun);
                                info!(
                                    slot = tx.slot,
                                    kol = %kol.label,
                                    "LocalShred 0-block KOL buy detected (Terminal/Padre PumpSwap)"
                                );
                                out.push(TriggerEvent {
                                    opportunity_key,
                                    strategy_id: StrategyId::KolBackrun,
                                    trigger_slot: tx.slot,
                                    detected_at_us,
                                    venue: Venue::PumpSwap,
                                    mint: cand_mint,
                                    quote_mint: WSOL_MINT,
                                    pool_pda,
                                    dex_accounts: dex_slice,
                                    kind: TriggerKind::KolBuy {
                                        kol_id: kol.id.clone(),
                                        kol_wallet: kol.pubkey,
                                        watched_balance_account: watched_account,
                                        is_spl_quote: kol.is_spl_quote,
                                        pre_balance,
                                        balance_threshold,
                                        estimated_kol_spend: kol.buy_size_lamports,
                                        buy_size_lamports: kol.buy_size_lamports,
                                        max_curve_reserve_lamports: kol.max_curve_reserve_lamports,
                                    },
                                });
                                break 'router_scan;
                            }
                        }
                    }
                }
                if out.len() == before_router {
                    let progs: Vec<String> = tx
                        .instructions
                        .iter()
                        .map(|ix| bs58::encode(&ix.program_id).into_string())
                        .collect();
                    info!(
                        slot = tx.slot,
                        kol = %kol.label,
                        progs = ?progs,
                        "LocalShred saw watched KOL tx (not matched by direct/router Pump.fun/PumpSwap)"
                    );
                }
            }
        }

        // 2. 0-Block Launch Detector (Pump.fun CreateV2 / Raydium LaunchLab Initialize).
        let filter = self
            .launch_filter
            .read()
            .map(|g| g.clone())
            .unwrap_or_default();
        if filter.enabled {
            // Check if a bundled dev buy accompanies the launch instruction in the same tx.
            let bundled_buys: Vec<ParsedBuy> = tx
                .instructions
                .iter()
                .filter_map(parse_buy_ix)
                .collect();
            let bundled_dev_buy: u64 = bundled_buys.iter().map(|b| b.estimated_spend).sum();
            let bundled_buy_accounts = bundled_buys
                .first()
                .map(|b| b.ix_accounts.clone())
                .unwrap_or_default();

            for ix in &tx.instructions {
                if let Some(launch) = parse_launch_ix(ix) {
                    // Block known ruggers permanently + serial deployers (>=2/hr).
                    if self.rugged_creators.contains(&launch.creator)
                        || self.is_serial_deployer(launch.creator, detected_at_us)
                    {
                        continue;
                    }
                    if !self.matches_launch_filter(&launch, bundled_dev_buy, &filter) {
                        continue;
                    }
                    if self.is_duplicate_symbol_clone(&launch.symbol, detected_at_us) {
                        continue;
                    }
                    let opportunity_key =
                        compute_opportunity_key(&launch.mint, tx.slot, launch.strategy_id);
                    // For Pump.fun, build all 18 mainnet Buy accounts locally in 0.1ms (works even on v0 ALT shreds!).
                    let dex_accounts = if launch.venue == Venue::PumpFun {
                        info!(
                            slot = tx.slot,
                            symbol = %launch.symbol,
                            dev_buy_lamports = bundled_dev_buy,
                            is_token_2022 = launch.is_token_2022,
                            is_mayhem = launch.is_mayhem,
                            "LocalShred 0-block Pump.fun launch detected -> firing 18-acc local buy"
                        );
                        build_local_pumpfun_buy_accounts(
                            launch.mint,
                            launch.pool_pda,
                            launch.creator,
                            launch.is_token_2022,
                            launch.is_mayhem,
                            &bundled_buy_accounts,
                        )
                    } else if !bundled_buy_accounts.is_empty() {
                        bundled_buy_accounts.clone()
                    } else {
                        launch.ix_accounts
                    };
                    out.push(TriggerEvent {
                        opportunity_key,
                        strategy_id: launch.strategy_id,
                        trigger_slot: tx.slot,
                        detected_at_us,
                        venue: launch.venue,
                        mint: launch.mint,
                        quote_mint: launch.quote_mint,
                        pool_pda: launch.pool_pda,
                        dex_accounts,
                        kind: TriggerKind::ZeroBlockLaunch {
                            creator: launch.creator,
                            dev_buy_lamports: filter.default_buy_lamports,
                            initial_dev_buy_lamports: bundled_dev_buy,
                            name: launch.name,
                            symbol: launch.symbol,
                            uri: launch.uri,
                        },
                    });
                }
            }
        }

        out
    }

    fn matches_launch_filter(
        &self,
        launch: &ParsedLaunch,
        dev_buy: u64,
        filter: &LaunchFilterConfig,
    ) -> bool {
        // Filter 3 (Fhbh1D CreateV2 parity): allow low-to-moderate dev buys (0.20–2.50 SOL),
        // reject heavy dev-bundled traps (> 2.50 SOL), and skip automated 0.99–1.02 SOL farm-bot templates.
        if launch.is_mayhem {
            return false;
        }
        let effective_min = 500_000_000u64;
        let effective_max = filter.max_dev_buy_lamports;
        if dev_buy < effective_min || dev_buy > effective_max {
            return false;
        }
        if (990_000_000..=1_020_000_000).contains(&dev_buy) {
            return false;
        }
        if !filter.allowed_quote_mints.is_empty()
            && !filter.allowed_quote_mints.contains(&launch.quote_mint)
        {
            return false;
        }
        if filter.require_metadata_uri
            && (launch.uri.trim().is_empty() || launch.name.trim().is_empty() || launch.symbol.trim().is_empty())
        {
            return false;
        }
        let hay = format!("{} {}", launch.name, launch.symbol).to_ascii_lowercase();
        for kw in &filter.blocked_keywords {
            if !kw.is_empty() && hay.contains(&kw.to_ascii_lowercase()) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::bus::OpportunityBus;
    use crate::ingest::types::KolRule;
    use std::time::Instant;

    fn encode_borsh_str(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    #[test]
    fn detects_kol_pumpfun_buy_and_computes_threshold_and_key() {
        let watchlist = WatchlistCache::new();
        let balances = BalanceCache::new();
        let kol_pk = [0x11; 32];
        let mint = [0x22; 32];
        let curve = [0x33; 32];

        watchlist.upsert(KolRule {
            id: "kol_1".into(),
            label: "Harpoon".into(),
            pubkey: kol_pk,
            quote_token_account: None,
            is_spl_quote: false,
            margin_lamports: 20_000_000, // 0.02 SOL
            buy_size_lamports: 3_000_000_000,
            max_curve_reserve_lamports: 85_000_000_000,
            enabled: true,
        });
        balances.set(kol_pk, 10_000_000_000); // 10 SOL pre-slot balance

        let mut ix_data = Vec::new();
        ix_data.extend_from_slice(&DISC_PUMPFUN_BUY_EXACT_QUOTE_V3);
        ix_data.extend_from_slice(&3_000_000_000u64.to_le_bytes()); // 3 SOL spend
        ix_data.extend_from_slice(&1_000u64.to_le_bytes());

        let tx = DecodedTx {
            slot: 383_000_500,
            received_at_us: 0,
            signature: [0u8; 64],
            signers: vec![kol_pk],
            instructions: vec![DecodedInstruction {
                program_id: PUMPFUN_PROGRAM_ID,
                accounts: vec![[0; 32], [0; 32], mint, curve],
                data: ix_data,
            }],
        };

        let detector = SignalDetector::new(watchlist, balances, LaunchFilterConfig::default());
        let bus = OpportunityBus::new(64);
        let mut rx = bus.subscribe();

        let start = Instant::now();
        let events = detector.inspect_tx(&tx);
        assert_eq!(events.len(), 1);
        bus.publish(events[0].clone());
        let received = rx.try_recv().unwrap();
        let elapsed_us = start.elapsed().as_micros();

        assert!(elapsed_us < 2_000, "Detection + publish took {}us (>= 2ms)", elapsed_us);
        assert_eq!(
            received.opportunity_key,
            compute_opportunity_key(&mint, 383_000_500, StrategyId::KolBackrun)
        );
        match received.kind {
            TriggerKind::KolBuy {
                pre_balance,
                balance_threshold,
                estimated_kol_spend,
                ..
            } => {
                assert_eq!(pre_balance, 10_000_000_000);
                assert_eq!(balance_threshold, 9_980_000_000);
                assert_eq!(estimated_kol_spend, 3_000_000_000);
            }
            _ => panic!("expected KolBuy"),
        }
    }

    #[test]
    fn detects_zero_block_pumpfun_create_v2_with_dev_buy_filter() {
        let watchlist = WatchlistCache::new();
        let balances = BalanceCache::new();
        let mut filter = LaunchFilterConfig::default();
        filter.min_dev_buy_lamports = 500_000_000;
        filter.blocked_keywords = vec!["rug".into()];
        let detector = SignalDetector::new(watchlist, balances, filter);

        let mint = [0x44; 32];
        let curve = [0x55; 32];
        let creator = [0x66; 32];

        let mut create_data = Vec::new();
        create_data.extend_from_slice(&DISC_PUMPFUN_CREATE_V2);
        encode_borsh_str(&mut create_data, "Orca Fin");
        encode_borsh_str(&mut create_data, "ORCA");
        encode_borsh_str(&mut create_data, "https://meta.example/orca.json");

        let mut dev_buy_data = Vec::new();
        dev_buy_data.extend_from_slice(&DISC_PUMPFUN_BUY_EXACT_QUOTE_V3);
        dev_buy_data.extend_from_slice(&1_500_000_000u64.to_le_bytes());
        dev_buy_data.extend_from_slice(&1u64.to_le_bytes());

        let tx = DecodedTx {
            slot: 383_000_999,
            received_at_us: 0,
            signature: [0u8; 64],
            signers: vec![creator],
            instructions: vec![
                DecodedInstruction {
                    program_id: PUMPFUN_PROGRAM_ID,
                    accounts: vec![mint, [0; 32], curve, [0; 32], [0; 32], [0; 32], [0; 32], creator],
                    data: create_data,
                },
                DecodedInstruction {
                    program_id: PUMPFUN_PROGRAM_ID,
                    accounts: vec![[0; 32], [0; 32], mint, curve, [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32], [0; 32]],
                    data: dev_buy_data,
                },
            ],
        };

        let events = detector.inspect_tx(&tx);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].strategy_id, StrategyId::PumpFunCreateV2Snipe);
        match &events[0].kind {
            TriggerKind::ZeroBlockLaunch { symbol, dev_buy_lamports, .. } => {
                assert_eq!(symbol, "ORCA");
                assert_eq!(*dev_buy_lamports, 50_000_000);
            }
            _ => panic!("expected ZeroBlockLaunch"),
        }

        // Duplicate symbol clone from a different creator within 15 min must be blocked.
        let mut tx_clone = tx.clone();
        tx_clone.signers = vec![[0x77; 32]];
        tx_clone.instructions[0].accounts[7] = [0x77; 32];
        assert!(detector.inspect_tx(&tx_clone).is_empty(), "duplicate symbol clone should be blocked");

        // Serial deployer launching a different symbol within 60 min must be blocked.
        let mut create_data2 = Vec::new();
        create_data2.extend_from_slice(&DISC_PUMPFUN_CREATE_V2);
        encode_borsh_str(&mut create_data2, "Second Coin");
        encode_borsh_str(&mut create_data2, "SEC");
        encode_borsh_str(&mut create_data2, "https://meta.example/sec.json");
        let mut tx_serial = tx.clone();
        tx_serial.instructions[0].data = create_data2;
        assert!(detector.inspect_tx(&tx_serial).is_empty(), "serial deployer should be blocked");
    }
}
