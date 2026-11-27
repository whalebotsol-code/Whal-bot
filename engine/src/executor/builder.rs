//! Solana v1 (native transactionConfig) & v0 (ALT + ComputeBudget) Transaction Builder.
//!
//! Enforces the critical zero-waste revert instruction order:
//!   ix 0    : SystemProgram::AdvanceNonceAccount (rotated from Durable Nonce Pool)
//!   ix 1    : Program #1 (whal-dedupe CheckAndLock: opportunity_key + max_slot = slot + ttl)
//!   ix 2    : ComputeBudget (v0 only; native transactionConfig carries these in v1)
//!   ix 3    : SystemProgram::CreateAccountWithSeed (8-char hex seed, 165B, Layer-2 atomic mutex)
//!   ix 4    : Token::InitializeAccount3
//!   ix 5    : (Optional, non-SOL pools) MeteoraDLMM::SwapExactOut2 JIT quote funding
//!   ix 6    : Program #2/#3 (KOL guard + DEX buy CPI) OR Standalone 115-CU Guard + native PumpSwap buy
//!   ix LAST : SystemProgram::Transfer(hot_wallet -> lander_tip_account, tip_lamports)

use sha2::{Digest, Sha256};

use crate::ingest::{PubkeyBytes, TriggerEvent, TriggerKind, Venue, WSOL_MINT};

/// Well-known Solana program IDs.
pub const SYSTEM_PROGRAM_ID: PubkeyBytes = [0u8; 32];
pub const COMPUTE_BUDGET_PROGRAM_ID: PubkeyBytes = [
    0x03, 0x06, 0x46, 0x6f, 0xe5, 0x21, 0x17, 0x32,
    0xff, 0xec, 0xad, 0xba, 0x72, 0xc3, 0x9b, 0xe7,
    0xbc, 0x8c, 0xe5, 0xbb, 0xc5, 0xf7, 0x12, 0x6b,
    0x2c, 0x43, 0x9b, 0x3a, 0x40, 0x00, 0x00, 0x00,
];
pub const SPL_TOKEN_PROGRAM_ID: PubkeyBytes = [
    0x06, 0xdd, 0xf6, 0xe1, 0xd7, 0x65, 0xa1, 0x93,
    0xd9, 0xcb, 0xe1, 0x46, 0xce, 0xeb, 0x79, 0xac,
    0x1c, 0xb4, 0x85, 0xed, 0x5f, 0x5b, 0x37, 0x91,
    0x3a, 0x8c, 0xf5, 0x85, 0x7e, 0xff, 0x00, 0xa9,
];
pub const TOKEN_2022_PROGRAM_ID: PubkeyBytes = [
    0x06, 0xdd, 0xf6, 0xe1, 0xee, 0x75, 0x8f, 0xde,
    0x18, 0x42, 0x5d, 0xbc, 0xe4, 0x6c, 0xcd, 0xda,
    0xb6, 0x1a, 0xfc, 0x4d, 0x83, 0xb9, 0x0d, 0x27,
    0xfe, 0xbd, 0xf9, 0x28, 0xd8, 0xa1, 0x8b, 0xfc,
];
pub const METEORA_DLMM_PROGRAM_ID: PubkeyBytes = [
    0x04, 0xe9, 0xe1, 0x2f, 0xbc, 0x84, 0xe8, 0x26,
    0xc9, 0x32, 0xcc, 0xe9, 0xe2, 0x64, 0x0c, 0xce,
    0x15, 0x59, 0x0c, 0x1c, 0x62, 0x73, 0xb0, 0x92,
    0x57, 0x08, 0xba, 0x3b, 0x85, 0x20, 0xb0, 0xbc,
];
/// Associated Token Account Program ID (`ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL`).
pub const ASSOCIATED_TOKEN_PROGRAM_ID: PubkeyBytes = [
    0x8c, 0x97, 0x25, 0x8f, 0x4e, 0x24, 0x89, 0xf1,
    0xbb, 0x3d, 0x10, 0x29, 0x14, 0x8e, 0x0d, 0x83,
    0x0b, 0x5a, 0x13, 0x99, 0xda, 0xff, 0x10, 0x84,
    0x04, 0x8e, 0x7b, 0xd8, 0xdb, 0xe9, 0xf8, 0x59,
];

/// Derives the canonical SPL Associated Token Account PDA (`[owner, token_program, mint]`).
pub fn derive_associated_token_account(
    owner: &PubkeyBytes,
    mint: &PubkeyBytes,
    token_program: &PubkeyBytes,
) -> PubkeyBytes {
    const PDA_MARKER: &[u8] = b"ProgramDerivedAddress";
    for bump in (0u8..=255).rev() {
        let mut h = Sha256::new();
        h.update(owner);
        h.update(token_program);
        h.update(mint);
        h.update([bump]);
        h.update(ASSOCIATED_TOKEN_PROGRAM_ID);
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

/// Rent-exempt minimum for a 165-byte SPL token account (~0.00203928 SOL).
pub const TOKEN_ACCOUNT_RENT_LAMPORTS: u64 = 2_039_280;
pub const TOKEN_ACCOUNT_SPACE: u64 = 165;

/// Meteora DLMM `swap_exact_out2` 8-byte Anchor discriminator.
pub const DISC_METEORA_SWAP_EXACT_OUT2: [u8; 8] = [0x2b, 0xd7, 0xf7, 0x84, 0x89, 0x3c, 0xf3, 0x51];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxVersion {
    V1,
    V0,
}

/// Native Solana v1 `transactionConfig` header fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V1TransactionConfig {
    pub compute_unit_limit: u32,
    pub loaded_accounts_data_size_limit: u32,
    pub priority_fee_lamports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountMetaSpec {
    pub pubkey: PubkeyBytes,
    pub is_signer: bool,
    pub is_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionSpec {
    pub label: &'static str,
    pub program_id: PubkeyBytes,
    pub accounts: Vec<AccountMetaSpec>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ProgramIds {
    pub dedupe_program: PubkeyBytes,
    pub dedupe_state_account: PubkeyBytes,
    pub router_program: PubkeyBytes,
    pub generic_quote_program: PubkeyBytes,
    pub sell_router_program: PubkeyBytes,
    pub pumpfun_program: PubkeyBytes,
    pub pumpswap_program: PubkeyBytes,
    pub launchlab_program: PubkeyBytes,
}

#[derive(Debug, Clone)]
pub struct BuiltTransaction {
    pub version: TxVersion,
    pub v1_config: Option<V1TransactionConfig>,
    pub lookup_tables: Vec<PubkeyBytes>,
    pub recent_blockhash: [u8; 32],
    pub fee_payer: PubkeyBytes,
    pub ephemeral_token_account: PubkeyBytes,
    pub ephemeral_seed: String,
    pub instructions: Vec<InstructionSpec>,
}

#[derive(Debug, Clone)]
pub struct BuildVariantParams {
    pub version: TxVersion,
    pub v1_config: V1TransactionConfig,
    pub lookup_tables: Vec<PubkeyBytes>,
    pub hot_wallet: PubkeyBytes,
    pub nonce_account: PubkeyBytes,
    pub nonce_hash: [u8; 32],
    pub tip_account: PubkeyBytes,
    pub tip_lamports: u64,
    pub buy_slot_ttl: u64,
    pub is_token_2022_mint: bool,
    pub use_standalone_guard_for_pumpswap: bool,
    pub min_tokens_out: u64,
}

/// Derives a deterministic 8-character lowercase hex seed from `(mint, opportunity_key)`.
/// Used for `CreateAccountWithSeed` (Layer-2 atomic mutex).
#[inline]
pub fn derive_ephemeral_seed(mint: &PubkeyBytes, opportunity_key: &[u8; 32]) -> String {
    let mut h = Sha256::new();
    h.update(mint);
    h.update(opportunity_key);
    let digest = h.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    )
}

/// Matches Solana's `Pubkey::create_with_seed(base, seed, owner) = sha256(base || seed || owner)`.
#[inline]
pub fn derive_seeded_pubkey(base: &PubkeyBytes, seed: &str, owner: &PubkeyBytes) -> PubkeyBytes {
    let mut h = Sha256::new();
    h.update(base);
    h.update(seed.as_bytes());
    h.update(owner);
    let digest = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Derives Solana PDA `["user_volume_accumulator", user]` under Pump.fun (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`).
/// Required on every Pump.fun Buy (`accounts[13]`) so LocalShred triggers never pass the dev's PDA (`Custom(2006)`).
pub fn derive_pumpfun_user_volume_accumulator(user: &PubkeyBytes) -> PubkeyBytes {
    const PDA_MARKER: &[u8] = b"ProgramDerivedAddress";
    for bump in (0u8..=255).rev() {
        let mut h = Sha256::new();
        h.update(b"user_volume_accumulator");
        h.update(user);
        h.update([bump]);
        h.update(crate::ingest::detector::PUMPFUN_PROGRAM_ID);
        h.update(PDA_MARKER);
        let digest = h.finalize();
        let mut candidate = [0u8; 32];
        candidate.copy_from_slice(&digest);
        if ed25519_dalek::VerifyingKey::from_bytes(&candidate).is_err() {
            return candidate;
        }
    }
    *user
}

/// Builds full mainnet DEX CPI account metas from the triggering KOL/Dev transaction's
/// captured `dex_accounts`, substituting `hot_wallet` and `ephemeral_ata` into the venue's
/// exact user & token-account slots while preserving all pool/vault/fee/authority PDAs.
pub fn build_venue_cpi_accounts(
    venue: Venue,
    dex_accounts: &[PubkeyBytes],
    hot_wallet: PubkeyBytes,
    ephemeral_ata: PubkeyBytes,
    quote_ata: Option<PubkeyBytes>,
    token_program: PubkeyBytes,
) -> Vec<AccountMetaSpec> {
    if dex_accounts.is_empty() {
        return Vec::new();
    }
    match venue {
        Venue::PumpFun => {
            // Pump.fun buy layout (12-16 accounts):
            // [0: global(ro), 1: fee_recipient(w), 2: mint(ro), 3: bonding_curve(w),
            //  4: assoc_bonding_curve(w), 5: assoc_user(w -> ephemeral_ata),
            //  6: user(sw -> hot_wallet), 7: system(ro), 8: token_prog(ro),
            //  9: creator_vault(w), 10: event_authority(ro), 11: program(ro), ...]
            let user_vol_pda = derive_pumpfun_user_volume_accumulator(&hot_wallet);
            dex_accounts
                .iter()
                .enumerate()
                .map(|(i, &pk)| match i {
                    1 | 3 | 4 | 9 | 12 | 17 => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: true },
                    5 => AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                    6 => AccountMetaSpec { pubkey: hot_wallet, is_signer: true, is_writable: true },
                    8 => AccountMetaSpec { pubkey: token_program, is_signer: false, is_writable: false },
                    13 => AccountMetaSpec { pubkey: user_vol_pda, is_signer: false, is_writable: true },
                    _ => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: false },
                })
                .collect()
        }
        Venue::PumpSwap => {
            // PumpSwap buy layout (17+ accounts):
            // [0: pool(w), 1: user(sw -> hot_wallet), 2: global_config(ro), 3: base_mint(ro),
            //  4: quote_mint(ro), 5: user_base_ata(w -> ephemeral_ata),
            //  6: user_quote_ata(w -> quote_ata), 7: pool_base_vault(w), 8: pool_quote_vault(w),
            //  9: protocol_fee_recipient(ro), 10: protocol_fee_recipient_ata(w), ...]
            dex_accounts
                .iter()
                .enumerate()
                .map(|(i, &pk)| match i {
                    0 | 7 | 8 | 10 => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: true },
                    1 => AccountMetaSpec { pubkey: hot_wallet, is_signer: true, is_writable: true },
                    5 => AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                    6 => AccountMetaSpec {
                        pubkey: quote_ata.unwrap_or(pk),
                        is_signer: false,
                        is_writable: true,
                    },
                    _ => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: false },
                })
                .collect()
        }
        Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => {
            // Raydium LaunchLab BuyExactIn layout (15+ accounts):
            // [0: payer(sw -> hot_wallet), 1: authority(ro), 2: global_config(ro),
            //  3: platform_config(ro), 4: pool_state(w), 5: user_base_token(w -> ephemeral_ata),
            //  6: user_quote_token(w -> quote_ata), 7: base_vault(w), 8: quote_vault(w),
            //  9: base_mint(ro), 10: quote_mint(ro), 11: base_token_prog(ro),
            //  12: quote_token_prog(ro), 13: event_authority(ro), 14: program(ro)]
            dex_accounts
                .iter()
                .enumerate()
                .map(|(i, &pk)| match i {
                    0 => AccountMetaSpec { pubkey: hot_wallet, is_signer: true, is_writable: true },
                    4 | 7 | 8 => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: true },
                    5 => AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                    6 => AccountMetaSpec {
                        pubkey: quote_ata.unwrap_or(pk),
                        is_signer: false,
                        is_writable: true,
                    },
                    _ => AccountMetaSpec { pubkey: pk, is_signer: false, is_writable: false },
                })
                .collect()
        }
    }
}

pub struct TxBuilder {
    pub programs: ProgramIds,
}

impl TxBuilder {
    pub fn new(programs: ProgramIds) -> Self {
        Self { programs }
    }

    pub fn programs(&self) -> &ProgramIds {
        &self.programs
    }

    /// Assembles a complete Solana v1 or v0 transaction in the exact zero-waste revert order.
    pub fn build_buy_tx(&self, trigger: &TriggerEvent, p: &BuildVariantParams) -> BuiltTransaction {
        let mut ixs = Vec::with_capacity(9);

        // ix 0: SystemProgram::AdvanceNonceAccount (instruction enum index = 4u32 LE) — omitted when using recent blockhash fallback
        if p.nonce_account != [0u8; 32] {
            ixs.push(InstructionSpec {
                label: "advance_nonce",
                program_id: SYSTEM_PROGRAM_ID,
                accounts: vec![
                    AccountMetaSpec { pubkey: p.nonce_account, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: [0x06, 0xa7, 0xd5, 0x17, 0x19, 0x2c, 0x56, 0x8e, 0xe0, 0x8a, 0x84, 0x5f, 0x73, 0xd2, 0x97, 0x88, 0xcf, 0x03, 0x5c, 0x31, 0x45, 0xb2, 0x1a, 0xb3, 0x44, 0xd8, 0x06, 0x2e, 0xa9, 0x40, 0x00, 0x00], is_signer: false, is_writable: false }, // SysvarRecentB1ockHashes11111111111111111111
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: false },
                ],
                data: 4u32.to_le_bytes().to_vec(),
            });
        }

        let direct_native_dex = std::env::var("WHAL_DIRECT_NATIVE_DEX")
            .ok()
            .map(|v| matches!(v.trim(), "1" | "true" | "TRUE"))
            .unwrap_or(false);

        // ix 1: Slot-TTL guard BEFORE swap & tip so late txs abort early (~2.4k CU) and pay ZERO SWQoS tip.
        if !direct_native_dex {
            let max_slot = trigger.trigger_slot.saturating_add(p.buy_slot_ttl);
            let mut dedupe_data = Vec::with_capacity(41);
            dedupe_data.push(0x01);
            dedupe_data.extend_from_slice(&trigger.opportunity_key);
            dedupe_data.extend_from_slice(&max_slot.to_le_bytes());
            ixs.push(InstructionSpec {
                label: "dedupe_check_and_lock",
                program_id: self.programs.dedupe_program,
                accounts: vec![
                    AccountMetaSpec { pubkey: self.programs.dedupe_state_account, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: false },
                ],
                data: dedupe_data,
            });
        } else if trigger.trigger_slot > 0 {
            // Direct-native mode: enforce on-chain Lighthouse AssertSysvarClock (disc=15, op=5 [<=]).
            // Respects Two-Tier per-variant TTL: Tier A = +0 (strict Slot 0), Tier B = +1..+4 safety net.
            let ttl = match trigger.kind {
                TriggerKind::KolBuy { .. } => p.buy_slot_ttl.min(2),
                TriggerKind::ZeroBlockLaunch { .. } => p.buy_slot_ttl.min(7),
            };
            let max_slot = trigger.trigger_slot.saturating_add(ttl);
            ixs.push(crate::exits::assert_clock_slot_lte(max_slot));
            // Shield 2 (Fhbh1D DpBm3e parity at $0 deploy cost via mainnet Lighthouse):
            // Abort with 0 SOL tip if our tx lands BEFORE the KOL or if the KOL tx reverted.
            if let TriggerKind::KolBuy {
                watched_balance_account,
                is_spl_quote,
                pre_balance,
                balance_threshold,
                max_curve_reserve_lamports,
                ..
            } = trigger.kind
            {
                if balance_threshold > 0 && pre_balance > balance_threshold {
                    if is_spl_quote {
                        ixs.push(crate::exits::assert_account_data_u64(
                            "lighthouse_kol_spl_guard",
                            watched_balance_account,
                            crate::exits::SPL_TOKEN_AMOUNT_OFFSET,
                            crate::exits::OP_LESS_THAN_OR_EQUAL,
                            balance_threshold,
                        ));
                    } else {
                        // On PumpSwap, require >= 0.05 SOL balance drop (50_000_000 lamports) so a router-wrapped
                        // KOL SELL (which only pays a tiny fee) can NEVER pass Shield 2 and trigger a buy into a dump.
                        let effective_threshold = if trigger.venue == Venue::PumpSwap {
                            pre_balance.saturating_sub(50_000_000)
                        } else {
                            balance_threshold
                        };
                        ixs.push(crate::exits::assert_account_lamports_lte(
                            watched_balance_account,
                            effective_threshold,
                        ));
                    }
                }
                // Shield 2b: On-chain Pump.fun bonding-curve reserve ceiling (offset 0x10 = virtual_sol_reserves).
                // Aborts with 0 SOL tip if snipers/KOL pushed the curve above max_curve_reserve_lamports.
                if trigger.venue == Venue::PumpFun && max_curve_reserve_lamports > 0 {
                    ixs.push(crate::exits::assert_account_data_u64(
                        "lighthouse_pumpfun_curve_cap",
                        trigger.pool_pda,
                        0x10,
                        crate::exits::OP_LESS_THAN_OR_EQUAL,
                        max_curve_reserve_lamports,
                    ));
                }
            }
        }

        // ix 2: ComputeBudget (always included so priority fee & CU limit are never dropped on wire)
        {
            let mut cu_limit = Vec::with_capacity(5);
            cu_limit.push(2u8); // SetComputeUnitLimit
            cu_limit.extend_from_slice(&p.v1_config.compute_unit_limit.to_le_bytes());
            ixs.push(InstructionSpec {
                label: "compute_budget_limit",
                program_id: COMPUTE_BUDGET_PROGRAM_ID,
                accounts: vec![],
                data: cu_limit,
            });

            let mut cu_price = Vec::with_capacity(9);
            cu_price.push(3u8); // SetComputeUnitPrice (micro-lamports)
            cu_price.extend_from_slice(&p.v1_config.priority_fee_lamports.to_le_bytes());
            ixs.push(InstructionSpec {
                label: "compute_budget_price",
                program_id: COMPUTE_BUDGET_PROGRAM_ID,
                accounts: vec![],
                data: cu_price,
            });
        }

        // ix 3: SystemProgram::CreateAccountWithSeed (Layer-2 atomic mutex, 8-char hex seed, 165B)
        let token_program = if p.is_token_2022_mint {
            TOKEN_2022_PROGRAM_ID
        } else {
            SPL_TOKEN_PROGRAM_ID
        };
        let seed = derive_ephemeral_seed(&trigger.mint, &trigger.opportunity_key);
        let ephemeral_ata = derive_seeded_pubkey(&p.hot_wallet, &seed, &token_program);
        let mut create_seed_data = Vec::with_capacity(4 + 32 + 8 + seed.len() + 8 + 8 + 32);
        create_seed_data.extend_from_slice(&3u32.to_le_bytes()); // CreateAccountWithSeed
        create_seed_data.extend_from_slice(&p.hot_wallet);
        create_seed_data.extend_from_slice(&(seed.len() as u64).to_le_bytes());
        create_seed_data.extend_from_slice(seed.as_bytes());
        create_seed_data.extend_from_slice(&TOKEN_ACCOUNT_RENT_LAMPORTS.to_le_bytes());
        create_seed_data.extend_from_slice(&TOKEN_ACCOUNT_SPACE.to_le_bytes());
        create_seed_data.extend_from_slice(&token_program);
        ixs.push(InstructionSpec {
            label: "create_account_with_seed_mutex",
            program_id: SYSTEM_PROGRAM_ID,
            accounts: vec![
                AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
            ],
            data: create_seed_data,
        });

        // ix 4: Token::InitializeAccount3 (opcode 18, [18u8 | owner_pubkey(32)])
        let mut init_acc3 = Vec::with_capacity(33);
        init_acc3.push(18u8);
        init_acc3.extend_from_slice(&p.hot_wallet);
        ixs.push(InstructionSpec {
            label: "token_initialize_account3",
            program_id: token_program,
            accounts: vec![
                AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                AccountMetaSpec { pubkey: trigger.mint, is_signer: false, is_writable: false },
            ],
            data: init_acc3,
        });

        // ix 5: (Optional for BONK / non-SOL pools) MeteoraDLMM::SwapExactOut2 for JIT quote funding
        let is_generic_quote = trigger.quote_mint != WSOL_MINT;
        let (buy_size, max_curve, kol_watched, kol_threshold, kol_is_spl) = match &trigger.kind {
            TriggerKind::KolBuy {
                watched_balance_account,
                balance_threshold,
                buy_size_lamports,
                max_curve_reserve_lamports,
                is_spl_quote,
                ..
            } => (
                *buy_size_lamports,
                *max_curve_reserve_lamports,
                Some(*watched_balance_account),
                *balance_threshold,
                *is_spl_quote,
            ),
            TriggerKind::ZeroBlockLaunch { dev_buy_lamports, .. } => {
                ((*dev_buy_lamports).clamp(10_000_000, 50_000_000_000), 85_000_000_000, None, 0, false)
            }
        };

        if is_generic_quote {
            // ix 5a: ATA::CreateIdempotent (opcode 1) for the quote token ATA before MeteoraDLMM::SwapExactOut2
            let quote_ata = derive_seeded_pubkey(&p.hot_wallet, "quote_ata", &token_program);
            ixs.push(InstructionSpec {
                label: "ata_create_idempotent_quote",
                program_id: ASSOCIATED_TOKEN_PROGRAM_ID,
                accounts: vec![
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                    AccountMetaSpec { pubkey: quote_ata, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: false, is_writable: false },
                    AccountMetaSpec { pubkey: trigger.quote_mint, is_signer: false, is_writable: false },
                    AccountMetaSpec { pubkey: SYSTEM_PROGRAM_ID, is_signer: false, is_writable: false },
                    AccountMetaSpec { pubkey: token_program, is_signer: false, is_writable: false },
                ],
                data: vec![1u8], // AssociatedTokenAccountInstruction::CreateIdempotent
            });

            // ix 5b: MeteoraDLMM::SwapExactOut2 (SOL -> exact quote token e.g. BONK / xStocks)
            let mut meteora_data = Vec::with_capacity(24);
            meteora_data.extend_from_slice(&DISC_METEORA_SWAP_EXACT_OUT2);
            meteora_data.extend_from_slice(&buy_size.to_le_bytes()); // max_in_amount (SOL)
            meteora_data.extend_from_slice(&p.min_tokens_out.max(1).to_le_bytes()); // exact_out_amount (quote)
            ixs.push(InstructionSpec {
                label: "meteora_dlmm_swap_exact_out2",
                program_id: METEORA_DLMM_PROGRAM_ID,
                accounts: vec![
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                    AccountMetaSpec { pubkey: quote_ata, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: trigger.quote_mint, is_signer: false, is_writable: false },
                ],
                data: meteora_data,
            });
        }

        // ix 6: Buy Execution — Program #3 (generic quote), Standalone 115-CU Guard + PumpSwap, or Program #2 Router
        if is_generic_quote {
            let mut gq_data = Vec::with_capacity(36);
            gq_data.push(0x01); // OP_GENERIC_QUOTE_BUY
            gq_data.push(1);    // kol_count = 1
            gq_data.push(5);    // quote_decimals
            gq_data.push(1);    // FLAG_CAP_QUOTE_IN
            gq_data.extend_from_slice(&buy_size.to_le_bytes());
            gq_data.extend_from_slice(&p.min_tokens_out.to_le_bytes());
            gq_data.extend_from_slice(&0u64.to_le_bytes());
            gq_data.extend_from_slice(&kol_threshold.to_le_bytes());
            let mut accs = vec![
                AccountMetaSpec { pubkey: self.programs.launchlab_program, is_signer: false, is_writable: false },
                AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                AccountMetaSpec { pubkey: trigger.quote_mint, is_signer: false, is_writable: false },
                AccountMetaSpec { pubkey: token_program, is_signer: false, is_writable: false },
                AccountMetaSpec { pubkey: trigger.pool_pda, is_signer: false, is_writable: true },
            ];
            if let Some(kol_acc) = kol_watched {
                accs.push(AccountMetaSpec { pubkey: kol_acc, is_signer: false, is_writable: false });
            }
            let quote_ata = derive_seeded_pubkey(&p.hot_wallet, "quote_ata", &token_program);
            accs.extend(build_venue_cpi_accounts(
                trigger.venue,
                &trigger.dex_accounts,
                p.hot_wallet,
                ephemeral_ata,
                Some(quote_ata),
                token_program,
            ));
            ixs.push(InstructionSpec {
                label: "program3_generic_quote_buy",
                program_id: self.programs.generic_quote_program,
                accounts: accs,
                data: gq_data,
            });
        } else if !direct_native_dex && trigger.venue == Venue::PumpSwap && p.use_standalone_guard_for_pumpswap {
            // Standalone 115-CU Guard (Program #2 Mode A) + native PumpSwap Buy
            if let Some(kol_acc) = kol_watched {
                let mut guard_data = Vec::with_capacity(15);
                guard_data.push(0x01); // OP_STANDALONE_GUARD
                guard_data.push(if kol_is_spl { 1 } else { 0 });
                guard_data.extend_from_slice(&[0u8; 5]);
                guard_data.extend_from_slice(&kol_threshold.to_le_bytes());
                ixs.push(InstructionSpec {
                    label: "program2_standalone_guard",
                    program_id: self.programs.router_program,
                    accounts: vec![AccountMetaSpec { pubkey: kol_acc, is_signer: false, is_writable: false }],
                    data: guard_data,
                });
            }
            let mut ps_data = Vec::with_capacity(24);
            ps_data.extend_from_slice(&[0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea]);
            ps_data.extend_from_slice(&p.min_tokens_out.to_le_bytes());
            ps_data.extend_from_slice(&buy_size.to_le_bytes());
            let cpi_accs = build_venue_cpi_accounts(
                Venue::PumpSwap,
                &trigger.dex_accounts,
                p.hot_wallet,
                ephemeral_ata,
                None,
                token_program,
            );
            let ps_accounts = if cpi_accs.is_empty() {
                vec![
                    AccountMetaSpec { pubkey: trigger.pool_pda, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                    AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                ]
            } else {
                cpi_accs
            };
            ixs.push(InstructionSpec {
                label: "native_pumpswap_buy",
                program_id: self.programs.pumpswap_program,
                accounts: ps_accounts,
                data: ps_data,
            });
        } else if direct_native_dex {
            // Direct native DEX buy (Pump.fun `buy_exact_sol_in` or PumpSwap `buy_exact_quote_in` with atomic WSOL wrap/unwrap)
            let target_prog = match trigger.venue {
                Venue::PumpFun => self.programs.pumpfun_program,
                Venue::PumpSwap => self.programs.pumpswap_program,
                Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => self.programs.launchlab_program,
            };
            let wsol_ata = if trigger.venue == Venue::PumpSwap {
                let ata = derive_associated_token_account(&p.hot_wallet, &WSOL_MINT, &SPL_TOKEN_PROGRAM_ID);
                // 1. CreateIdempotent WSOL ATA
                ixs.push(InstructionSpec {
                    label: "create_wsol_ata_idempotent",
                    program_id: ASSOCIATED_TOKEN_PROGRAM_ID,
                    accounts: vec![
                        AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                        AccountMetaSpec { pubkey: ata, is_signer: false, is_writable: true },
                        AccountMetaSpec { pubkey: p.hot_wallet, is_signer: false, is_writable: false },
                        AccountMetaSpec { pubkey: WSOL_MINT, is_signer: false, is_writable: false },
                        AccountMetaSpec { pubkey: SYSTEM_PROGRAM_ID, is_signer: false, is_writable: false },
                        AccountMetaSpec { pubkey: SPL_TOKEN_PROGRAM_ID, is_signer: false, is_writable: false },
                    ],
                    data: vec![1u8],
                });
                // 2. Fund WSOL ATA with buy_size lamports (+2% headroom for PumpSwap LP+protocol fee)
                let wrap_lamports = buy_size.saturating_add(buy_size / 50);
                let mut xfer_data = Vec::with_capacity(12);
                xfer_data.extend_from_slice(&2u32.to_le_bytes());
                xfer_data.extend_from_slice(&wrap_lamports.to_le_bytes());
                ixs.push(InstructionSpec {
                    label: "transfer_sol_to_wsol_ata",
                    program_id: SYSTEM_PROGRAM_ID,
                    accounts: vec![
                        AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                        AccountMetaSpec { pubkey: ata, is_signer: false, is_writable: true },
                    ],
                    data: xfer_data,
                });
                // 3. Token::SyncNative(wsol_ata)
                ixs.push(InstructionSpec {
                    label: "sync_native_wsol",
                    program_id: SPL_TOKEN_PROGRAM_ID,
                    accounts: vec![
                        AccountMetaSpec { pubkey: ata, is_signer: false, is_writable: true },
                    ],
                    data: vec![17u8],
                });
                Some(ata)
            } else {
                None
            };
            let mut buy_data = Vec::with_capacity(24);
            if trigger.venue == Venue::PumpFun {
                // Pump.fun `buy_exact_sol_in(spendable_sol_in, min_tokens_out)` — spends exact configured buy_size SOL.
                buy_data.extend_from_slice(&[0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f]);
                buy_data.extend_from_slice(&buy_size.to_le_bytes());
                buy_data.extend_from_slice(&p.min_tokens_out.max(1).to_le_bytes());
            } else if trigger.venue == Venue::PumpSwap {
                // PumpSwap `buy_exact_quote_in(spendable_quote_in, min_base_amount_out)`
                buy_data.extend_from_slice(&[0xc6, 0x2e, 0x15, 0x52, 0xb4, 0xd9, 0xe8, 0x70]);
                buy_data.extend_from_slice(&buy_size.to_le_bytes());
                buy_data.extend_from_slice(&p.min_tokens_out.max(1).to_le_bytes());
            } else {
                buy_data.extend_from_slice(&[0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea]);
                buy_data.extend_from_slice(&p.min_tokens_out.max(1).to_le_bytes());
                buy_data.extend_from_slice(&buy_size.to_le_bytes());
            }
            let cpi_accs = build_venue_cpi_accounts(
                trigger.venue,
                &trigger.dex_accounts,
                p.hot_wallet,
                ephemeral_ata,
                wsol_ata,
                token_program,
            );
            let native_accs = if cpi_accs.is_empty() {
                vec![
                    AccountMetaSpec { pubkey: trigger.pool_pda, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                    AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
                ]
            } else {
                cpi_accs
            };
            ixs.push(InstructionSpec {
                label: "direct_native_dex_buy",
                program_id: target_prog,
                accounts: native_accs,
                data: buy_data,
            });
            // Unwrap any unused WSOL headroom + reclaim rent back to hot_wallet atomically
            if let Some(ata) = wsol_ata {
                ixs.push(InstructionSpec {
                    label: "close_wsol_ata_unwrap",
                    program_id: SPL_TOKEN_PROGRAM_ID,
                    accounts: vec![
                        AccountMetaSpec { pubkey: ata, is_signer: false, is_writable: true },
                        AccountMetaSpec { pubkey: p.hot_wallet, is_signer: false, is_writable: true },
                        AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: false },
                    ],
                    data: vec![9u8],
                });
            }
        } else {
            // Program #2 Mode B: Multi-KOL Guard + Venue CPI Buy Router
            let (target_prog, venue_byte) = match trigger.venue {
                Venue::PumpFun => (self.programs.pumpfun_program, 0u8),
                Venue::PumpSwap => (self.programs.pumpswap_program, 1u8),
                Venue::RaydiumLaunchLab | Venue::RaydiumCpmm => (self.programs.launchlab_program, 2u8),
            };
            let mut router_data = Vec::with_capacity(55);
            router_data.push(0x02); // OP_MULTI_KOL_ROUTER
            router_data.push(venue_byte);
            router_data.push(1);    // kol_count = 1
            router_data.push(if kol_is_spl { 1 } else { 0 });
            router_data.push(0);    // reserve_flags = lamports
            router_data.extend_from_slice(&0u16.to_le_bytes());
            router_data.extend_from_slice(&0u64.to_le_bytes()); // min_reserve
            router_data.extend_from_slice(&max_curve.to_le_bytes());
            router_data.extend_from_slice(&buy_size.to_le_bytes()); // base_buy
            router_data.extend_from_slice(&buy_size.to_le_bytes()); // max_buy
            router_data.extend_from_slice(&p.min_tokens_out.to_le_bytes());
            router_data.extend_from_slice(&kol_threshold.to_le_bytes());

            let mut accs = vec![
                AccountMetaSpec { pubkey: target_prog, is_signer: false, is_writable: false },
                AccountMetaSpec { pubkey: trigger.pool_pda, is_signer: false, is_writable: true },
                AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                AccountMetaSpec { pubkey: ephemeral_ata, is_signer: false, is_writable: true },
            ];
            if let Some(kol_acc) = kol_watched {
                accs.push(AccountMetaSpec { pubkey: kol_acc, is_signer: false, is_writable: false });
            }
            accs.extend(build_venue_cpi_accounts(
                trigger.venue,
                &trigger.dex_accounts,
                p.hot_wallet,
                ephemeral_ata,
                None,
                token_program,
            ));
            ixs.push(InstructionSpec {
                label: "program2_router_buy",
                program_id: self.programs.router_program,
                accounts: accs,
                data: router_data,
            });
        }

        // ix LAST: SystemProgram::Transfer(hot_wallet -> lander_tip_account, tip_lamports)
        // Placing the tip LAST guarantees reverted duplicates pay only ~6,000 lamports base fee and ZERO tip!
        let mut tip_data = Vec::with_capacity(12);
        tip_data.extend_from_slice(&2u32.to_le_bytes()); // SystemInstruction::Transfer
        tip_data.extend_from_slice(&p.tip_lamports.to_le_bytes());
        ixs.push(InstructionSpec {
            label: "lander_tip_transfer",
            program_id: SYSTEM_PROGRAM_ID,
            accounts: vec![
                AccountMetaSpec { pubkey: p.hot_wallet, is_signer: true, is_writable: true },
                AccountMetaSpec { pubkey: p.tip_account, is_signer: false, is_writable: true },
            ],
            data: tip_data,
        });

        BuiltTransaction {
            version: p.version,
            v1_config: if p.version == TxVersion::V1 { Some(p.v1_config) } else { None },
            lookup_tables: if p.version == TxVersion::V0 { p.lookup_tables.clone() } else { vec![] },
            recent_blockhash: p.nonce_hash,
            fee_payer: p.hot_wallet,
            ephemeral_token_account: ephemeral_ata,
            ephemeral_seed: seed,
            instructions: ixs,
        }
    }

    /// Wraps Lighthouse-protected `whal-sell-router` instructions with `AdvanceNonceAccount` (ix 0),
    /// optional v0 `ComputeBudget`, and `lander_tip_transfer` (ix LAST) for zero-waste sell fan-out.
    #[allow(clippy::too_many_arguments)]
    pub fn build_sell_tx(
        &self,
        sell_ixs: Vec<InstructionSpec>,
        hot_wallet: PubkeyBytes,
        bot_token_account: PubkeyBytes,
        nonce_account: PubkeyBytes,
        nonce_hash: [u8; 32],
        tip_account: PubkeyBytes,
        tip_lamports: u64,
        version: TxVersion,
        v1_config: V1TransactionConfig,
        lookup_tables: &[PubkeyBytes],
    ) -> BuiltTransaction {
        let mut ixs = Vec::with_capacity(sell_ixs.len() + 4);
        if nonce_account != [0u8; 32] {
            ixs.push(InstructionSpec {
                label: "advance_nonce",
                program_id: SYSTEM_PROGRAM_ID,
                accounts: vec![
                    AccountMetaSpec { pubkey: nonce_account, is_signer: false, is_writable: true },
                    AccountMetaSpec { pubkey: [0x06, 0xa7, 0xd5, 0x17, 0x19, 0x2c, 0x56, 0x8e, 0xe0, 0x8a, 0x84, 0x5f, 0x73, 0xd2, 0x97, 0x88, 0xcf, 0x03, 0x5c, 0x31, 0x45, 0xb2, 0x1a, 0xb3, 0x44, 0xd8, 0x06, 0x2e, 0xa9, 0x40, 0x00, 0x00], is_signer: false, is_writable: false },
                    AccountMetaSpec { pubkey: hot_wallet, is_signer: true, is_writable: false },
                ],
                data: 4u32.to_le_bytes().to_vec(),
            });
        }
        // Always emit ComputeBudget (limit + priority fee) on sells regardless of V0/V1 so sells never land with 0 priority fee.
        {
            let mut cu_limit = Vec::with_capacity(5);
            cu_limit.push(2u8);
            cu_limit.extend_from_slice(&v1_config.compute_unit_limit.to_le_bytes());
            ixs.push(InstructionSpec {
                label: "compute_budget_limit",
                program_id: COMPUTE_BUDGET_PROGRAM_ID,
                accounts: vec![],
                data: cu_limit,
            });
            let mut cu_price = Vec::with_capacity(9);
            cu_price.push(3u8);
            cu_price.extend_from_slice(&v1_config.priority_fee_lamports.to_le_bytes());
            ixs.push(InstructionSpec {
                label: "compute_budget_price",
                program_id: COMPUTE_BUDGET_PROGRAM_ID,
                accounts: vec![],
                data: cu_price,
            });
        }
        ixs.extend(sell_ixs);

        let mut tip_data = Vec::with_capacity(12);
        tip_data.extend_from_slice(&2u32.to_le_bytes());
        tip_data.extend_from_slice(&tip_lamports.to_le_bytes());
        ixs.push(InstructionSpec {
            label: "lander_tip_transfer",
            program_id: SYSTEM_PROGRAM_ID,
            accounts: vec![
                AccountMetaSpec { pubkey: hot_wallet, is_signer: true, is_writable: true },
                AccountMetaSpec { pubkey: tip_account, is_signer: false, is_writable: true },
            ],
            data: tip_data,
        });

        BuiltTransaction {
            version,
            v1_config: if version == TxVersion::V1 { Some(v1_config) } else { None },
            lookup_tables: if version == TxVersion::V0 { lookup_tables.to_vec() } else { vec![] },
            recent_blockhash: nonce_hash,
            fee_payer: hot_wallet,
            ephemeral_token_account: bot_token_account,
            ephemeral_seed: String::new(),
            instructions: ixs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{compute_opportunity_key, StrategyId};

    fn sample_programs() -> ProgramIds {
        ProgramIds {
            dedupe_program: [0x10; 32],
            dedupe_state_account: [0x11; 32],
            router_program: [0x20; 32],
            generic_quote_program: [0x30; 32],
            sell_router_program: [0x35; 32],
            pumpfun_program: [0x40; 32],
            pumpswap_program: [0x50; 32],
            launchlab_program: [0x60; 32],
        }
    }

    fn sample_params(version: TxVersion) -> BuildVariantParams {
        BuildVariantParams {
            version,
            v1_config: V1TransactionConfig {
                compute_unit_limit: 200_000,
                loaded_accounts_data_size_limit: 256 * 1024,
                priority_fee_lamports: 100_000,
            },
            lookup_tables: vec![[0xAA; 32]],
            hot_wallet: [0x01; 32],
            nonce_account: [0x02; 32],
            nonce_hash: [0x03; 32],
            tip_account: [0x04; 32],
            tip_lamports: 5_000_000,
            buy_slot_ttl: 4,
            is_token_2022_mint: false,
            use_standalone_guard_for_pumpswap: false,
            min_tokens_out: 1_000,
        }
    }

    #[test]
    fn v1_tx_enforces_exact_zero_waste_instruction_order_and_tip_last() {
        let mint = [0x99; 32];
        let trigger = TriggerEvent {
            opportunity_key: compute_opportunity_key(&mint, 500, StrategyId::KolBackrun),
            strategy_id: StrategyId::KolBackrun,
            trigger_slot: 500,
            detected_at_us: 0,
            venue: Venue::PumpFun,
            mint,
            quote_mint: WSOL_MINT,
            pool_pda: [0x88; 32],
            dex_accounts: vec![],
            kind: TriggerKind::KolBuy {
                kol_id: "kol_1".into(),
                kol_wallet: [0x77; 32],
                watched_balance_account: [0x77; 32],
                is_spl_quote: false,
                pre_balance: 10_000_000_000,
                balance_threshold: 9_980_000_000,
                estimated_kol_spend: 3_000_000_000,
                buy_size_lamports: 2_000_000_000,
                max_curve_reserve_lamports: 85_000_000_000,
            },
        };

        let builder = TxBuilder::new(sample_programs());
        let tx = builder.build_buy_tx(&trigger, &sample_params(TxVersion::V1));
        let labels: Vec<&str> = tx.instructions.iter().map(|i| i.label).collect();
        assert_eq!(
            labels,
            vec![
                "advance_nonce",
                "dedupe_check_and_lock",
                "create_account_with_seed_mutex",
                "token_initialize_account3",
                "program2_router_buy",
                "lander_tip_transfer",
            ]
        );
        assert_eq!(tx.ephemeral_seed.len(), 8);
        assert!(tx.v1_config.is_some());
    }

    #[test]
    fn generic_quote_v0_inserts_compute_budget_and_meteora_jit_swap() {
        let mint = [0x99; 32];
        let bonk_mint = [0xBB; 32];
        let trigger = TriggerEvent {
            opportunity_key: compute_opportunity_key(&mint, 500, StrategyId::GenericQuoteSnipe),
            strategy_id: StrategyId::GenericQuoteSnipe,
            trigger_slot: 500,
            detected_at_us: 0,
            venue: Venue::RaydiumLaunchLab,
            mint,
            quote_mint: bonk_mint,
            pool_pda: [0x88; 32],
            dex_accounts: vec![],
            kind: TriggerKind::KolBuy {
                kol_id: "kol_6".into(),
                kol_wallet: [0x77; 32],
                watched_balance_account: [0x76; 32],
                is_spl_quote: true,
                pre_balance: 1_000_000,
                balance_threshold: 750_000,
                estimated_kol_spend: 500_000,
                buy_size_lamports: 2_000_000_000,
                max_curve_reserve_lamports: 50_000_000_000,
            },
        };

        let builder = TxBuilder::new(sample_programs());
        let tx = builder.build_buy_tx(&trigger, &sample_params(TxVersion::V0));
        let labels: Vec<&str> = tx.instructions.iter().map(|i| i.label).collect();
        assert_eq!(
            labels,
            vec![
                "advance_nonce",
                "dedupe_check_and_lock",
                "compute_budget_limit",
                "compute_budget_price",
                "create_account_with_seed_mutex",
                "token_initialize_account3",
                "ata_create_idempotent_quote",
                "meteora_dlmm_swap_exact_out2",
                "program3_generic_quote_buy",
                "lander_tip_transfer",
            ]
        );
        assert!(tx.v1_config.is_none());
        assert_eq!(tx.lookup_tables.len(), 1);
    }
}
