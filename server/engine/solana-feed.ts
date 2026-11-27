// Read-only Solana Mainnet live feed (zero fake data, zero signing, zero transactions).
// - Real mainnet slots via Solana RPC WebSocket (Helius if available, automatic fallback to api.mainnet-beta.solana.com on 429)
// - Real mainnet Pump.fun new token launches (exact mint, symbol, dev buy SOL, signature) via PumpPortal live stream
// - Real SOL/USD price + real hot-wallet SOL balance via RPC getBalance
// Automatically yields whenever the Rust whal-engine connects to /internal/engine.
import crypto from 'node:crypto';
import WebSocket from 'ws';
import type { ClosedPosition, FeedEvent, Position } from '../../lib/types';
import type { ControlPlane } from '../control';
import type { SecretStore } from '../secrets';

const PUBLIC_SOLANA_WS = 'wss://api.mainnet-beta.solana.com';
const PUBLIC_SOLANA_HTTP = 'https://api.mainnet-beta.solana.com';
const PUMPPORTAL_WS = 'wss://pumpportal.fun/api/data';
const PUMPFUN_LATEST_API = 'https://frontend-api-v3.pump.fun/coins?offset=0&limit=10&sort=created_timestamp&order=DESC&includeNsfw=false';
const PUMPFUN_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const B58_ALPHA = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const ED25519_P = (1n << 255n) - 19n;
const edMod = (a: bigint) => ((a % ED25519_P) + ED25519_P) % ED25519_P;
const edPow = (b: bigint, e: bigint) => { let r = 1n; b = edMod(b); while (e > 0n) { if (e & 1n) r = edMod(r * b); b = edMod(b * b); e >>= 1n; } return r; };
const ED25519_D = edMod(-121665n * edPow(121666n, ED25519_P - 2n));
function b58Decode(s: string): Buffer {
  let n = 0n;
  for (const c of s) { const i = B58_ALPHA.indexOf(c); if (i < 0) return Buffer.alloc(0); n = n * 58n + BigInt(i); }
  const bytes: number[] = [];
  while (n > 0n) { bytes.push(Number(n & 0xffn)); n >>= 8n; }
  for (const c of s) { if (c === '1') bytes.push(0); else break; }
  const raw = Buffer.from(bytes.reverse());
  return raw.length < 32 ? Buffer.concat([Buffer.alloc(32 - raw.length), raw]) : raw;
}
function b58Encode(buf: Buffer): string {
  let n = 0n;
  for (const b of buf) n = (n << 8n) + BigInt(b);
  let out = '';
  while (n > 0n) { out = B58_ALPHA[Number(n % 58n)] + out; n /= 58n; }
  for (const b of buf) { if (b === 0) out = '1' + out; else break; }
  return out || '1';
}
function isOnEd25519Curve(buf: Buffer): boolean {
  const b = Buffer.from(buf);
  b[31] &= 0x7f;
  let y = 0n;
  for (let i = 31; i >= 0; i--) y = (y << 8n) + BigInt(b[i]);
  if (y >= ED25519_P) return false;
  const y2 = edMod(y * y);
  const u = edMod(y2 - 1n);
  const v = edMod(ED25519_D * y2 + 1n);
  const x2 = edMod(u * edPow(v, ED25519_P - 2n));
  if (x2 === 0n) return (buf[31] & 0x80) === 0;
  return edPow(x2, (ED25519_P - 1n) >> 1n) === 1n;
}
const ATA_PROGRAM = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL';
const PUMP_GLOBAL = '4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf';
const PUMP_FEE_RECIPIENT = '2aSWZ3QXsNLVVBr1UB1HxSw7sqMRR2pv2qa5ohsTi5Gp';
const PUMP_MAYHEM_FEE_RECIPIENT = 'GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS';
const SYSTEM_PROGRAM = '11111111111111111111111111111111';
const PUMP_EVENT_AUTH = 'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1';
const PUMP_GLOBAL_VOL = 'Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y';
const PUMP_FEE_CONFIG = '8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt';
const PUMP_FEE_PROGRAM = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ';
const PUMP_TOKEN_2022 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
// Verified global buyback_fee_recipients from Pump.fun Global account (4wTV1Ymi... offsets 741, 837, 869, 965).
const PUMP_BUYBACK_RECIPIENTS = [
  '5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD',
  '3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR',
  '5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6',
  'A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW',
];

function derivePda(seeds: Buffer[], programId: string): string | null {
  const progBuf = b58Decode(programId);
  if (progBuf.length !== 32) return null;
  const pdaMarker = Buffer.from('ProgramDerivedAddress');
  for (let bump = 255; bump >= 0; bump--) {
    const h = crypto.createHash('sha256');
    for (const s of seeds) h.update(s);
    h.update(Buffer.from([bump]));
    h.update(progBuf);
    h.update(pdaMarker);
    const d = h.digest();
    if (!isOnEd25519Curve(d)) return b58Encode(d);
  }
  return null;
}
function deriveUserVolumeAccumulator(hotWallet: string): string | null {
  if (!hotWallet) return null;
  const userBuf = b58Decode(hotWallet);
  if (userBuf.length !== 32) return null;
  return derivePda([Buffer.from('user_volume_accumulator'), userBuf], PUMPFUN_PROGRAM);
}
function deriveBondingCurveV2(mint: string): string | null {
  const mintBuf = b58Decode(mint);
  if (mintBuf.length !== 32) return null;
  return derivePda([Buffer.from('bonding-curve-v2'), mintBuf], PUMPFUN_PROGRAM);
}
function deriveCreatorVault(creatorBuf: Buffer): string | null {
  if (creatorBuf.length !== 32) return null;
  return derivePda([Buffer.from('creator-vault'), creatorBuf], PUMPFUN_PROGRAM);
}
function deriveAta(owner: string, tokenProgram: string, mint: string): string | null {
  const o = b58Decode(owner);
  const tp = b58Decode(tokenProgram);
  const m = b58Decode(mint);
  if (o.length !== 32 || tp.length !== 32 || m.length !== 32) return null;
  return derivePda([o, tp, m], ATA_PROGRAM);
}
function deriveBondingCurve(mint: string): string | null {
  const m = b58Decode(mint);
  if (m.length !== 32) return null;
  return derivePda([Buffer.from('bonding-curve'), m], PUMPFUN_PROGRAM);
}
const PUMP_CREATE_DISC = Buffer.from([0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77]);
const PUMP_CREATE_V2_DISC = Buffer.from([0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4]);
function decodePumpCreateMeta(b58Data?: string): { name: string; symbol: string; uri: string } | null {
  if (!b58Data) return null;
  const buf = b58Decode(b58Data);
  if (buf.length < 20) return null;
  const disc = buf.subarray(0, 8);
  if (!disc.equals(PUMP_CREATE_DISC) && !disc.equals(PUMP_CREATE_V2_DISC)) return null;
  let off = 8;
  const readStr = (): string | null => {
    if (off + 4 > buf.length) return null;
    const len = buf.readUInt32LE(off);
    off += 4;
    if (len > 512 || off + len > buf.length) return null;
    const s = buf.subarray(off, off + len).toString('utf8');
    off += len;
    return s;
  };
  const name = readStr();
  const symbol = readStr();
  const uri = readStr();
  if (name === null || symbol === null || uri === null) return null;
  return { name, symbol, uri };
}

const ZERO_REVERTS = { duplicate: 0, mutex: 0, kolNotLanded: 0, slippage: 0, expired: 0 };

const IGNORED_MINTS = new Set([
  'So11111111111111111111111111111111111111112', // WSOL
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
  'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

const DEX_PROGRAM_VENUES: Array<[string, FeedEvent['venue']]> = [
  ['6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', 'Pump.fun'],
  ['pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA', 'PumpSwap'],
  ['LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj', 'Raydium LaunchLab'],
  ['675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', 'Raydium CPMM'],
  ['CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', 'Raydium CPMM'],
  ['LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', 'Raydium CPMM'],
  ['JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', 'Raydium CPMM'],
];

function detectVenueFromLogs(logs: string[]): FeedEvent['venue'] | null {
  const joined = logs.join('\n');
  for (const [programId, venue] of DEX_PROGRAM_VENUES) {
    if (joined.includes(programId)) return venue;
  }
  return null;
}

function buildRpcEndpoints(rawUrl: string, apiKey: string | null): { httpUrl: string; wsUrl: string } {
  let base = rawUrl.trim();
  if (!base || base.includes('example-provider.com') || base.includes('example.net')) {
    base = apiKey ? 'https://mainnet.helius-rpc.com' : PUBLIC_SOLANA_HTTP;
  }
  try {
    const u = new URL(base);
    if (apiKey && !u.searchParams.has('api-key') && u.hostname.includes('helius')) {
      u.searchParams.set('api-key', apiKey);
    }
    const httpUrl = u.toString();
    const ws = new URL(httpUrl);
    ws.protocol = ws.protocol === 'https:' ? 'wss:' : 'ws:';
    return { httpUrl, wsUrl: ws.toString() };
  } catch {
    return { httpUrl: PUBLIC_SOLANA_HTTP, wsUrl: PUBLIC_SOLANA_WS };
  }
}

export class SolanaLiveFeed {
  private slotWs: WebSocket | null = null;
  private pumpWs: WebSocket | null = null;
  private stopped = false;
  private slotRetry = 0;
  private pumpRetry = 0;
  private usePublicSlotFallback = false;
  private slotTimer: ReturnType<typeof setTimeout> | null = null;
  private pumpTimer: ReturnType<typeof setTimeout> | null = null;
  private priceTimer: ReturnType<typeof setInterval> | null = null;
  private balTimer: ReturnType<typeof setInterval> | null = null;
  private paperTimer: ReturnType<typeof setInterval> | null = null;
  private paperPriceTimer: ReturnType<typeof setInterval> | null = null;
  private curvePriceTimer: ReturnType<typeof setInterval> | null = null;
  private watchdogTimer: ReturnType<typeof setInterval> | null = null;
  private pumpFallbackTimer: ReturnType<typeof setInterval> | null = null;
  private inFlightFallback = false;
  private inFlightCurvePoll = false;
  private lastPumpMsgAt = 0;
  private lastSlotMsgAt = 0;
  private paper = new Map<string, { pos: Position; peakPrice: number; vSol: number; bondingCurveKey?: string; creator?: string; hasLiveTick: boolean; breakevenArmed: boolean; instantTpFired: boolean; lastTwapAt: number; firedSteps: Set<number> }>();
  private watchedKolKeys = new Set<string>();
  private pumpSubscribedKols = new Set<string>();
  private rpcKolReqToPubkey = new Map<number, string>();
  private rpcKolPubkeyToSubId = new Map<string, number>();
  private rpcKolSubIdToPubkey = new Map<number, string>();
  private rpcPumpCreateReqId: number | null = null;
  private rpcPumpCreateSubId: number | null = null;
  private seenPumpCreateSigs = new Set<string>();
  private nextRpcSubReqId = 100;
  private seenKolSigs = new Set<string>();
  private creatorLaunches = new Map<string, number[]>();
  private ruggedCreators = new Set<string>();
  private liveCreators = new Map<string, string>();
  private solUsd = 0;
  private solBalance = 0;
  private wsolBalance = 0;
  private usdcBalance = 0;
  private lastSlot = 0;
  private lastSlotAt = 0;
  private seenMints = new Set<string>();
  private recentBoughtSymbols = new Map<string, number>();
  private endpoints: { httpUrl: string; wsUrl: string } = { httpUrl: PUBLIC_SOLANA_HTTP, wsUrl: PUBLIC_SOLANA_WS };

  constructor(private cp: ControlPlane, private secrets: SecretStore) {}

  start() {
    this.stopped = false;
    this.reload();
    this.connectPumpStream();
    this.priceTimer = setInterval(() => void this.refreshMarketAndBalance(), 30_000);
    this.priceTimer.unref();
    // Fast 4s on-chain SOL balance refresh (always active so deposits & live trades reflect immediately)
    this.balTimer = setInterval(() => {
      void this.refreshSolBalanceFast();
    }, 4_000);
    this.balTimer.unref();
    this.paperTimer = setInterval(() => this.sweepExpiredPaperPositions(), 2_000);
    this.paperTimer.unref();
    // Sub-second on-chain Pump.fun bonding-curve price poller (800ms, processed commitment)
    this.curvePriceTimer = setInterval(() => void this.pollBondingCurvesOnChain(), 800);
    this.curvePriceTimer.unref();
    // Fast DexScreener fallback (1.5s) for graduated PumpSwap/Raydium tokens
    this.paperPriceTimer = setInterval(() => void this.pollPaperPricesFromDexScreener(), 1_500);
    this.paperPriceTimer.unref();
    // Zombie WebSocket watchdog: auto-reconnects if PumpPortal or Solana slot WS goes silent
    this.watchdogTimer = setInterval(() => this.checkStaleStreams(), 15_000);
    this.watchdogTimer.unref();
    // Automatic Pump.fun v3 REST fallback (1s) whenever PumpPortal WS is down (e.g., 502) or silent >12s
    this.pumpFallbackTimer = setInterval(() => void this.pollPumpFunFallback(), 1_000);
    this.pumpFallbackTimer.unref();
    this.cp.on('telemetry', (e: { type: string; positions?: Position[]; closed?: ClosedPosition }) => {
      if (e.type === 'config_updated') this.reload();
      if (e.type === 'kols_updated') this.syncKolSubscriptions();
      if (e.type === 'position_updated') {
        if (e.positions) {
          const liveMints = new Set(e.positions.map((p) => p.mint));
          for (const mint of this.paper.keys()) if (!liveMints.has(mint)) this.paper.delete(mint);
        }
        // Learn rugged creators from live Stop-Loss exits identically to Paper Mode.
        if (e.closed && !e.closed.id.startsWith('cl_paper_') && e.closed.proceedsSol < e.closed.entrySol * 0.94) {
          const c = this.liveCreators.get(e.closed.mint);
          if (c) this.ruggedCreators.add(c);
        }
      }
    });
  }

  stop() {
    this.stopped = true;
    if (this.slotTimer) clearTimeout(this.slotTimer);
    if (this.pumpTimer) clearTimeout(this.pumpTimer);
    if (this.priceTimer) clearInterval(this.priceTimer);
    if (this.balTimer) clearInterval(this.balTimer);
    if (this.paperTimer) clearInterval(this.paperTimer);
    if (this.curvePriceTimer) clearInterval(this.curvePriceTimer);
    if (this.paperPriceTimer) clearInterval(this.paperPriceTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    if (this.pumpFallbackTimer) clearInterval(this.pumpFallbackTimer);
    this.slotWs?.close();
    this.pumpWs?.close();
    this.slotWs = null;
    this.pumpWs = null;
  }

  reload() {
    let apiKey: string | null = null;
    try { apiKey = this.secrets.reveal('network.rpc_api_key').toString('utf8').trim() || null; } catch { /* unset */ }
    const rpcUrl = this.cp.configState().config.network.rpc_primary_url;
    const next = buildRpcEndpoints(rpcUrl, apiKey);
    if (next.wsUrl === this.endpoints.wsUrl && this.slotWs?.readyState === WebSocket.OPEN) return;

    this.endpoints = next;
    this.usePublicSlotFallback = false;
    this.slotRetry = 0;
    this.slotWs?.close();
    this.slotWs = null;
    if (this.slotTimer) clearTimeout(this.slotTimer);
    void this.refreshMarketAndBalance();
    this.connectSlotStream();
  }

  // ---------- 1. Live Solana Mainnet Slot Stream ----------
  private connectSlotStream() {
    if (this.stopped) return;
    const targetUrl = this.usePublicSlotFallback ? PUBLIC_SOLANA_WS : this.endpoints.wsUrl;
    const ws = new WebSocket(targetUrl);
    this.slotWs = ws;

    ws.on('open', () => {
      this.slotRetry = 0;
      this.lastSlotMsgAt = Date.now();
      this.rpcKolReqToPubkey.clear();
      this.rpcKolPubkeyToSubId.clear();
      this.rpcKolSubIdToPubkey.clear();
      this.rpcPumpCreateReqId = null;
      this.rpcPumpCreateSubId = null;
      console.log(`[solana-feed] Slot stream connected (${this.usePublicSlotFallback ? 'public mainnet-beta' : 'configured RPC'})`);
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'slotSubscribe', params: [] }));
      if (!this.usePublicSlotFallback) {
        const reqId = this.nextRpcSubReqId++;
        this.rpcPumpCreateReqId = reqId;
        ws.send(JSON.stringify({
          jsonrpc: '2.0',
          id: reqId,
          method: 'logsSubscribe',
          params: [{ mentions: [PUMPFUN_PROGRAM] }, { commitment: 'processed' }],
        }));
      }
      this.syncKolSubscriptions();
    });

    ws.on('message', (raw) => {
      this.lastSlotMsgAt = Date.now();
      let msg: Record<string, unknown>;
      try { msg = JSON.parse(raw.toString()) as Record<string, unknown>; } catch { return; }
      if (typeof msg.id === 'number' && typeof msg.result === 'number' && msg.id === this.rpcPumpCreateReqId) {
        this.rpcPumpCreateReqId = null;
        this.rpcPumpCreateSubId = msg.result;
        console.log('[solana-feed] Paid Helius real-time Pump.fun Create/CreateV2 stream subscribed');
        return;
      }
      if (typeof msg.id === 'number' && typeof msg.result === 'number' && this.rpcKolReqToPubkey.has(msg.id)) {
        const pubkey = this.rpcKolReqToPubkey.get(msg.id)!;
        this.rpcKolReqToPubkey.delete(msg.id);
        this.rpcKolPubkeyToSubId.set(pubkey, msg.result);
        this.rpcKolSubIdToPubkey.set(msg.result, pubkey);
        return;
      }
      if (msg.method === 'slotNotification') {
        const slot = (msg.params as { result?: { slot?: number } })?.result?.slot;
        if (typeof slot === 'number') this.onSlot(slot);
        return;
      }
      if (msg.method === 'logsNotification') {
        const params = msg.params as {
          subscription?: number;
          result?: { context?: { slot?: number }; value?: { signature?: string; err?: unknown; logs?: string[] } };
        };
        const subId = params?.subscription;
        const val = params?.result?.value;
        const slot = params?.result?.context?.slot ?? this.lastSlot;
        if (typeof subId !== 'number' || !val?.signature || val.err !== null) return;
        const logs = Array.isArray(val.logs) ? val.logs : [];
        if (subId === this.rpcPumpCreateSubId) {
          // Only inspect Helius Create logs when PumpPortal WS is silent >3s so we never saturate Helius getTransaction.
          const pumpSilent = this.pumpWs?.readyState !== WebSocket.OPEN || Date.now() - this.lastPumpMsgAt > 3_000;
          if (pumpSilent && logs.some((l) => l.includes('Instruction: Create') || l.includes('Instruction: CreateV2'))) {
            if (!this.seenPumpCreateSigs.has(val.signature)) {
              this.seenPumpCreateSigs.add(val.signature);
              if (this.seenPumpCreateSigs.size > 500) {
                const first = this.seenPumpCreateSigs.values().next().value;
                if (first) this.seenPumpCreateSigs.delete(first);
              }
              void this.inspectHeliusPumpCreate(val.signature, slot);
            }
          }
          return;
        }
        const kolPubkey = this.rpcKolSubIdToPubkey.get(subId);
        if (!kolPubkey) return;
        const venue = detectVenueFromLogs(logs);
        if (!venue) return;
        if (this.seenKolSigs.has(val.signature)) return;
        this.markKolSigSeen(val.signature);
        void this.inspectKolRpcTransaction(kolPubkey, val.signature, slot, venue);
      }
    });

    ws.on('error', (err) => {
      if (!this.usePublicSlotFallback && /429|401|403/.test(err.message)) {
        console.warn(`[solana-feed] Primary RPC WS rejected (${err.message}); falling back to public mainnet slot stream`);
        this.usePublicSlotFallback = true;
      }
    });

    ws.on('close', () => {
      if (this.stopped || this.slotWs !== ws) return;
      const wait = Math.min(20_000, 1000 * 2 ** this.slotRetry++);
      this.slotTimer = setTimeout(() => this.connectSlotStream(), wait);
    });
  }

  private onSlot(slot: number) {
    const now = Date.now();
    const delta = this.lastSlotAt ? Math.max(5, Math.min(400, now - this.lastSlotAt)) : 18;
    this.lastSlot = slot;
    this.lastSlotAt = now;
    const nonces = this.cp.configState().config.wallets.nonce_accounts.length;
    this.cp.ingestExternalEvent({
      type: 'slot_tick',
      ts: now,
      chain: {
        slot,
        shredLatencyMs: delta,
        nonces: { ready: nonces, total: nonces },
        ring: { used: 0, size: 64 },
      },
      balances: {
        sol: this.solBalance,
        wsol: this.wsolBalance,
        usdc: this.usdcBalance,
        solUsd: this.solUsd,
      },
    });
  }

  // ---------- 2. Live Mainnet New Token Launch Stream (Pump.fun) ----------
  private connectPumpStream() {
    if (this.stopped) return;
    const ws = new WebSocket(PUMPPORTAL_WS);
    this.pumpWs = ws;

    ws.on('open', () => {
      this.pumpRetry = 0;
      this.lastPumpMsgAt = Date.now();
      this.pumpSubscribedKols.clear();
      console.log('[solana-feed] Connected to live mainnet Pump.fun new-token stream');
      ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
      this.syncKolSubscriptions();
      if (this.paper.size > 0) {
        ws.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [...this.paper.keys()] }));
      }
    });

    ws.on('message', (raw) => {
      this.lastPumpMsgAt = Date.now();
      let msg: {
        signature?: string;
        mint?: string;
        traderPublicKey?: string;
        bondingCurveKey?: string;
        symbol?: string;
        name?: string;
        uri?: string;
        solAmount?: number;
        vSolInBondingCurve?: number;
        vTokensInBondingCurve?: number;
        txType?: string;
        pool?: string;
      };
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (!msg.mint) return;

      // 1. Live mainnet trade update for an open Shadow paper position -> real mark-to-market PnL + full exit engine.
      if ((msg.txType === 'buy' || msg.txType === 'sell') && this.paper.has(msg.mint)) {
        this.onPaperTrade(msg.mint, msg.vSolInBondingCurve, msg.vTokensInBondingCurve);
        // 1b. Instant exit on dev dump or mirrored KOL sell (if position is still open after tick)
        if (msg.txType === 'sell' && msg.traderPublicKey && this.paper.has(msg.mint)) {
          const entry = this.paper.get(msg.mint)!;
          if (entry.creator && msg.traderPublicKey === entry.creator) {
            this.ruggedCreators.add(entry.creator);
            this.closePaperPosition(msg.mint, 'Dev dump instant exit');
            return;
          }
          if (entry.pos.kolId) {
            const kol = this.cp.snapshot().kols.find((k) => k.id === entry.pos.kolId && k.pubkey === msg.traderPublicKey);
            if (kol) {
              this.closePaperPosition(msg.mint, `Mirrored KOL sell (${kol.label})`);
              return;
            }
          }
        }
      }

      // 1c. Live Mode (RUNNING): instant Dev-Dump Exit + ruggedCreators learning + PumpPortal KOL Mirror Sell
      if (msg.txType === 'sell' && msg.traderPublicKey && this.cp.engineStatus === 'RUNNING') {
        const livePos = this.cp.snapshot().positions.find((p) => !p.id.startsWith('paper_') && p.mint === msg.mint);
        if (livePos) {
          const creator = this.liveCreators.get(msg.mint);
          if (creator && msg.traderPublicKey === creator) {
            this.ruggedCreators.add(creator);
            this.cp.dispatchLiveMirrorSell(msg.mint, 'Dev dump');
            return;
          }
          if (this.watchedKolKeys.has(msg.traderPublicKey)) {
            const kol = this.cp.snapshot().kols.find((k) => k.pubkey === msg.traderPublicKey);
            if (kol && livePos.kolId === kol.id) {
              this.cp.dispatchLiveMirrorSell(msg.mint, kol.label);
              return;
            }
          }
        }
      }

      // 2. Live KOL Copy-Trade detection (same-slot copy using KOL's exact buySizeSol & maxCurveReserveSol).
      if (msg.txType === 'buy' && msg.traderPublicKey && this.watchedKolKeys.has(msg.traderPublicKey)) {
        this.onKolBuySignal(msg, ws);
        return;
      }

      if (msg.txType !== 'create') return;
      this.handleNewLaunch({
        mint: msg.mint,
        signature: msg.signature,
        symbol: msg.symbol,
        name: msg.name,
        uri: msg.uri,
        solAmount: msg.solAmount,
        vSolInBondingCurve: msg.vSolInBondingCurve,
        vTokensInBondingCurve: msg.vTokensInBondingCurve,
        bondingCurveKey: msg.bondingCurveKey,
        creator: msg.traderPublicKey,
        pool: msg.pool,
      }, ws);
    });

    ws.on('error', (err) => {
      console.warn('[solana-feed] New-token stream error:', err.message);
    });

    ws.on('close', () => {
      if (this.stopped || this.pumpWs !== ws) return;
      const wait = Math.min(20_000, 1000 * 2 ** this.pumpRetry++);
      this.pumpTimer = setTimeout(() => this.connectPumpStream(), wait);
    });
  }

  /** Automatic live launch fallback when PumpPortal WebSocket is down (502) or silent >12s. */
  private async pollPumpFunFallback() {
    if (this.stopped || this.inFlightFallback) return;
    // Watchdog: terminate & immediately reconnect half-open zombie PumpPortal WS sockets silent >15s.
    if (this.pumpWs?.readyState === WebSocket.OPEN && this.lastPumpMsgAt > 0 && Date.now() - this.lastPumpMsgAt > 15_000) {
      console.warn('[solana-feed] PumpPortal WS silent >15s (zombie socket); terminating and reconnecting...');
      this.pumpRetry = 0;
      this.lastPumpMsgAt = Date.now();
      try { this.pumpWs.terminate(); } catch { /* ignore */ }
    }
    const pumpHealthy = this.pumpWs?.readyState === WebSocket.OPEN && Date.now() - this.lastPumpMsgAt < 12_000;
    if (pumpHealthy) return;
    this.inFlightFallback = true;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 3000);
      const r = await fetch(PUMPFUN_LATEST_API, {
        signal: ctrl.signal,
        headers: { 'user-agent': 'Mozilla/5.0' },
      });
      clearTimeout(t);
      if (!r.ok) return;
      const coins = await r.json() as Array<{
        mint?: string;
        name?: string;
        symbol?: string;
        image_uri?: string;
        metadata_uri?: string;
        bonding_curve?: string;
        creator?: string;
        created_timestamp?: number;
        virtual_sol_reserves?: number;
        virtual_token_reserves?: number;
      }>;
      if (!Array.isArray(coins)) return;
      const cutoff = Date.now() - 60_000;
      const fresh = [...coins].reverse().filter(
        (c): c is typeof c & { mint: string } =>
          Boolean(c.mint) && !this.seenMints.has(c.mint!) && (typeof c.created_timestamp !== 'number' || c.created_timestamp >= cutoff),
      );
      if (fresh.length === 0) return;

      // Batch-read real on-chain bonding-curve reserves (vSol, vTokens) in 1 RPC call since Pump.fun v3 omits them.
      const curveReserves = new Map<string, { vSol: number; vTokens: number }>();
      const curveCoins = fresh.filter((c) => typeof c.bonding_curve === 'string' && c.bonding_curve.length >= 32);
      if (curveCoins.length > 0) {
        try {
          const rpcCtrl = new AbortController();
          const rpcTimer = setTimeout(() => rpcCtrl.abort(), 2000);
          const rpcRes = await fetch(this.endpoints.httpUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            signal: rpcCtrl.signal,
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getMultipleAccounts',
              params: [curveCoins.map((c) => c.bonding_curve), { encoding: 'base64', commitment: 'processed' }],
            }),
          });
          clearTimeout(rpcTimer);
          if (rpcRes.ok) {
            const rpcJson = (await rpcRes.json()) as { result?: { value?: ({ data?: [string, string] } | null)[] } };
            const vals = rpcJson.result?.value;
            if (Array.isArray(vals)) {
              for (let i = 0; i < curveCoins.length; i++) {
                const b64 = vals[i]?.data?.[0];
                if (!b64) continue;
                const buf = Buffer.from(b64, 'base64');
                if (buf.length < 49 || buf[48] === 1) continue;
                const vTokens = Number(buf.readBigUInt64LE(8)) / 1e6;
                const vSol = Number(buf.readBigUInt64LE(16)) / 1e9;
                if (vTokens > 0 && vSol > 0) curveReserves.set(curveCoins[i].mint, { vSol, vTokens });
              }
            }
          }
        } catch { /* fallback to metadata-only launch if RPC times out */ }
      }

      for (const c of fresh) {
        const onChain = curveReserves.get(c.mint);
        const vSol = onChain?.vSol ?? (typeof c.virtual_sol_reserves === 'number' && c.virtual_sol_reserves > 0
          ? (c.virtual_sol_reserves > 1e6 ? c.virtual_sol_reserves / 1e9 : c.virtual_sol_reserves)
          : undefined);
        const vTokens = onChain?.vTokens ?? (typeof c.virtual_token_reserves === 'number' && c.virtual_token_reserves > 0
          ? (c.virtual_token_reserves > 1e12 ? c.virtual_token_reserves / 1e6 : c.virtual_token_reserves)
          : undefined);
        const estDevBuy = vSol && vSol > 30 ? +(vSol - 30).toFixed(4) : 0;
        this.handleNewLaunch({
          mint: c.mint,
          symbol: c.symbol,
          name: c.name,
          uri: c.metadata_uri,
          image: c.image_uri,
          solAmount: estDevBuy,
          vSolInBondingCurve: vSol,
          vTokensInBondingCurve: vTokens,
          bondingCurveKey: c.bonding_curve,
          creator: c.creator,
          pool: 'pump',
          fromFallback: true,
          createdAtMs: typeof c.created_timestamp === 'number' ? c.created_timestamp : undefined,
        });
      }
    } catch { /* ignore transient fallback error */ }
    finally { this.inFlightFallback = false; }
  }

  /** Real-time paid Helius WS Pump.fun Create/CreateV2 launch resolver (zero PumpPortal dependency). */
  private async inspectHeliusPumpCreate(signature: string, slot: number) {
    if (this.stopped) return;
    const rpcUrl = this.endpoints.httpUrl;
    const waitsMs = [0, 200, 350, 500, 700];
    try {
      for (const wait of waitsMs) {
        if (wait > 0) await new Promise((r) => setTimeout(r, wait));
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), 1600);
        try {
          const res = await fetch(rpcUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            signal: ctrl.signal,
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getTransaction',
              params: [signature, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1, commitment: 'confirmed' }],
            }),
          });
          if (!res.ok) continue;
          const j = await res.json() as {
            result?: {
              meta?: {
                err?: unknown;
                preBalances?: number[];
                postBalances?: number[];
                innerInstructions?: Array<{ instructions?: Array<{ programId?: string; accounts?: string[]; data?: string }> }>;
              };
              transaction?: { message?: { instructions?: Array<{ programId?: string; accounts?: string[]; data?: string }> } };
            } | null;
          };
          const tx = j.result;
          if (!tx || tx.meta?.err) return;
          const allIx = [
            ...(tx.transaction?.message?.instructions ?? []),
            ...(tx.meta?.innerInstructions ?? []).flatMap((g) => g.instructions ?? []),
          ];
          const pumpIx = [...allIx].reverse().find(
            (ix) =>
              ix.programId === PUMPFUN_PROGRAM &&
              Array.isArray(ix.accounts) &&
              ix.accounts.length >= 16 &&
              ix.accounts[15] === PUMP_FEE_PROGRAM,
          );
          if (!pumpIx?.accounts) return;
          const mint = pumpIx.accounts[2];
          const bondingCurveKey = pumpIx.accounts[3];
          const creator = pumpIx.accounts[6];
          if (!mint || !bondingCurveKey || this.seenMints.has(mint)) return;
          let meta: { name: string; symbol: string; uri: string } | null = null;
          for (const ix of allIx) {
            if (ix.programId === PUMPFUN_PROGRAM && typeof ix.data === 'string') {
              meta = decodePumpCreateMeta(ix.data);
              if (meta) break;
            }
          }
          const pre = tx.meta?.preBalances?.[0] ?? 0;
          const post = tx.meta?.postBalances?.[0] ?? 0;
          const devBuySol = Math.max(0, +(((pre - post) / 1e9) - 0.025).toFixed(4));
          this.handleNewLaunch({
            mint,
            signature,
            symbol: meta?.symbol ?? '',
            name: meta?.name ?? '',
            uri: meta?.uri ?? '',
            solAmount: devBuySol,
            bondingCurveKey,
            creator,
            pool: 'pump',
            prefetchedDexAccounts: pumpIx.accounts,
            slotOverride: slot,
          });
          return;
        } finally {
          clearTimeout(t);
        }
      }
    } catch { /* ignore transient RPC timeout */ }
  }

  private handleNewLaunch(
    msg: {
      mint: string;
      signature?: string;
      symbol?: string;
      name?: string;
      uri?: string;
      image?: string;
      solAmount?: number;
      vSolInBondingCurve?: number;
      vTokensInBondingCurve?: number;
      bondingCurveKey?: string;
      creator?: string;
      pool?: string;
      fromFallback?: boolean;
      createdAtMs?: number;
      prefetchedDexAccounts?: string[];
      slotOverride?: number;
    },
    ws?: WebSocket,
  ) {
    if (this.seenMints.has(msg.mint)) return;
    this.seenMints.add(msg.mint);
    if (this.seenMints.size > 400) {
      const first = this.seenMints.values().next().value;
      if (first) this.seenMints.delete(first);
    }

    const ts = Date.now();
    const slot = msg.slotOverride || this.cp.currentSlot() || this.lastSlot;
    if (msg.signature) this.cp.recordLaunchSig(msg.mint, msg.signature);
    const symbol = (msg.symbol || msg.mint.slice(0, 4)).trim().slice(0, 16);
    const sizeSol = typeof msg.solAmount === 'number' && Number.isFinite(msg.solAmount) ? +msg.solAmount.toFixed(4) : 0;
    const normImg = msg.image?.trim()
      ? (msg.image.trim().startsWith('ipfs://') ? `https://ipfs.io/ipfs/${msg.image.trim().slice(7)}` : msg.image.trim())
      : undefined;
    const ev: FeedEvent = {
      id: `live_${(msg.signature || msg.mint).slice(0, 16)}`,
      ts,
      slot,
      kind: 'launch',
      kolId: null,
      mint: msg.mint,
      symbol,
      image: normImg && /^https?:\/\//i.test(normImg) ? normImg : undefined,
      venue: msg.pool === 'raydium' ? 'Raydium LaunchLab' : 'Pump.fun',
      sizeSol,
      fanout: 0,
      winner: null,
      cuUsed: 0,
      reverts: ZERO_REVERTS,
      outcome: this.cp.engineStatus === 'SHADOW' ? 'shadow' : 'missed',
      note: msg.name ? `${msg.name.slice(0, 28)} · live mainnet` : 'Live mainnet launch',
    };

    this.cp.ingestExternalEvent({ type: 'kol_shred_detected', ts, slot, kolId: null, mint: ev.mint, symbol, venue: ev.venue });
    this.cp.ingestExternalEvent({ type: 'burst_dispatched', ts, burst: ev }, msg.signature ?? null);
    const cfg = this.cp.configState().config;
    const withinCurve = !msg.vSolInBondingCurve || msg.vSolInBondingCurve <= cfg.strategy.max_curve_reserve_sol;
    // Selective quality launch filter: 0.5–5.0 SOL dev buy sweet spot + non-empty metadata (name, symbol, uri)
    const hasMeta = Boolean(msg.name?.trim() && msg.symbol?.trim() && (msg.uri?.trim() || ev.image));
    // Never auto-buy stale REST fallback launches in LIVE mode (Pump.fun REST caches 8-15s = +35..60 slots late).
    // Allow REST fallback up to 20s only in SHADOW paper mode; LIVE mode requires real-time WS or Paid Helius WS.
    const isFreshForAutoBuy = !msg.fromFallback
      || (this.cp.engineStatus === 'SHADOW' && typeof msg.createdAtMs === 'number' && ts - msg.createdAtMs <= 20_000);
    // 15-minute duplicate-symbol cooldown so copycat/vamp clones (e.g., 3x $TOAD) are never bought back-to-back.
    const normSym = symbol.toUpperCase().replace(/[^A-Z0-9]/g, '');
    const lastSymBuy = normSym ? (this.recentBoughtSymbols.get(normSym) ?? 0) : 0;
    const notDuplicateClone = !normSym || ts - lastSymBuy > 900_000;
    // Serial-rugger & multi-deployer filter: block creators who rugged before or launched >=2 tokens in 60 min
    let creatorClean = true;
    if (msg.creator) {
      const prev = (this.creatorLaunches.get(msg.creator) ?? []).filter((t) => ts - t < 3_600_000);
      prev.push(ts);
      this.creatorLaunches.set(msg.creator, prev);
      if (this.creatorLaunches.size > 1000) {
        const oldest = this.creatorLaunches.keys().next().value;
        if (oldest) this.creatorLaunches.delete(oldest);
      }
      if (this.ruggedCreators.has(msg.creator) || prev.length >= 2) creatorClean = false;
    }
    if (
      (this.cp.engineStatus === 'SHADOW' || this.cp.engineStatus === 'RUNNING') &&
      cfg.strategy.modes.pumpfun_create_snipe &&
      isFreshForAutoBuy &&
      notDuplicateClone &&
      creatorClean &&
      withinCurve &&
      hasMeta &&
      sizeSol >= 0.5 &&
      sizeSol <= 5.0
    ) {
      if (this.cp.engineStatus === 'SHADOW' && this.paper.size < 5 && !this.paper.has(msg.mint)) {
        if (normSym) {
          this.recentBoughtSymbols.set(normSym, ts);
          if (this.recentBoughtSymbols.size > 300) {
            const oldest = this.recentBoughtSymbols.keys().next().value;
            if (oldest) this.recentBoughtSymbols.delete(oldest);
          }
        }
        this.openPaperPosition(ev, cfg.strategy.default_buy_sol, msg.vSolInBondingCurve, msg.vTokensInBondingCurve, ws, msg.bondingCurveKey, undefined, msg.creator);
      } else if (this.cp.engineStatus === 'RUNNING') {
        // Strict Slot +0 mode: 100% of live launch snipes must come from Amsterdam LocalShred (0-block).
        // Skip slow Railway bridge launches (which arrive +2..+4 slots late and only burn base fees on Custom(6001)).
        if (cfg.strategy.buy_slot_ttl.max === 0) return;
        const curveKey = msg.bondingCurveKey || deriveBondingCurve(msg.mint);
        if (!curveKey) return;
        const realOpen = this.cp.snapshot().positions.filter((p) => !p.id.startsWith('paper_'));
        if (realOpen.length >= 5 || realOpen.some((p) => p.mint === msg.mint)) return;
        void this.dispatchPumpLaunchLive({
          venue: ev.venue,
          mint: msg.mint,
          bondingCurveKey: curveKey,
          signature: msg.signature,
          normSym,
          slot: msg.slotOverride ?? slot,
          buySizeSol: cfg.strategy.default_buy_sol || 0.1,
          maxCurveReserveSol: cfg.strategy.max_curve_reserve_sol,
          creator: msg.creator,
          devBuySol: sizeSol,
          symbol,
          name: msg.name,
          uri: msg.uri,
          prefetchedDexAccounts: msg.prefetchedDexAccounts,
        });
      }
    }
    if (!ev.image && msg.uri) void this.hydrateTokenImage(ev, msg.uri, msg.signature ?? null);
  }

  /** Fetches the exact 16-18 mainnet Pump.fun buy accounts (including Token-2022 vs SPL Token at [8]) before dispatching live. */
  private async dispatchPumpLaunchLive(p: {
    venue: FeedEvent['venue'];
    mint: string;
    bondingCurveKey: string;
    signature?: string;
    normSym?: string;
    slot: number;
    buySizeSol: number;
    maxCurveReserveSol: number;
    creator?: string;
    devBuySol: number;
    symbol: string;
    name?: string;
    uri?: string;
    kolId?: string;
    kolWallet?: string;
    prefetchedDexAccounts?: string[];
  }) {
    // Always use primary paid Helius HTTP RPC for live account lookup (never fall back to rate-limited public mainnet-beta HTTP).
    const rpcUrl = this.endpoints.httpUrl;
    const fetchSigs = async (): Promise<string[]> => {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 1800);
      try {
        const sigRes = await fetch(rpcUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          signal: ctrl.signal,
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'getSignaturesForAddress',
            params: [p.bondingCurveKey, { limit: 4, commitment: 'confirmed' }],
          }),
        });
        if (!sigRes.ok) return [];
        const sigJson = await sigRes.json() as { result?: Array<{ signature?: string; err?: unknown }> };
        return (sigJson.result ?? []).filter((s) => s.signature && s.err == null).map((s) => s.signature!);
      } catch {
        return [];
      } finally {
        clearTimeout(t);
      }
    };

    const fetchPumpBuyAccounts = async (sig: string): Promise<string[] | null> => {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 1800);
      try {
        const txRes = await fetch(rpcUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          signal: ctrl.signal,
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'getTransaction',
            params: [sig, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1, commitment: 'confirmed' }],
          }),
        });
        if (!txRes.ok) return null;
        const txJson = await txRes.json() as {
          result?: {
            meta?: { innerInstructions?: Array<{ instructions?: Array<{ programId?: string; accounts?: string[] }> }> };
            transaction?: { message?: { instructions?: Array<{ programId?: string; accounts?: string[] }> } };
          } | null;
        };
        if (!txJson.result) return null;
        const allIx = [
          ...(txJson.result.transaction?.message?.instructions ?? []),
          ...(txJson.result.meta?.innerInstructions ?? []).flatMap((g) => g.instructions ?? []),
        ];
        const pumpIx = [...allIx].reverse().find(
          (ix) =>
            ix.programId === PUMPFUN_PROGRAM &&
            Array.isArray(ix.accounts) &&
            ix.accounts.length >= 16 &&
            ix.accounts[2] === p.mint &&
            ix.accounts[3] === p.bondingCurveKey &&
            ix.accounts[15] === 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ',
        );
        return pumpIx?.accounts ?? null;
      } catch {
        return null;
      } finally {
        clearTimeout(t);
      }
    };

    const hotWallet = this.cp.configState().config.wallets.hot_wallet_pubkey;
    const userVolPda = deriveUserVolumeAccumulator(hotWallet);
    const bcV2Pda = deriveBondingCurveV2(p.mint);

    // Fast path (~80ms, zero `confirmed` wait!): read exact on-chain mint owner, bonding_curve.creator [49..81],
    // and mayhem flag [81] at `processed` commitment, then build all 18 Pump.fun Buy accounts locally:
    // - exact on-chain creator_vault -> 0% Custom(2006)
    // - exact mayhem vs standard fee_recipient -> 0% Custom(6000)
    // - exact bonding_curve_v2 + verified Global buyback_fee_recipient -> 0% Custom(6062/6074)
    const tryFastLocalAccounts = async (): Promise<string[] | null> => {
      if (!hotWallet || !userVolPda || !bcV2Pda) return null;
      for (let attempt = 0; attempt < 2; attempt++) {
        if (attempt > 0) await new Promise((r) => setTimeout(r, 150));
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), 900);
        try {
          const res = await fetch(rpcUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            signal: ctrl.signal,
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getMultipleAccounts',
              params: [[p.mint, p.bondingCurveKey], { encoding: 'base64', commitment: 'processed' }],
            }),
          });
          if (!res.ok) continue;
          const j = await res.json() as {
            result?: { value?: Array<{ owner?: string; data?: [string, string] } | null> };
          };
          const [mintAcc, curveAcc] = j.result?.value ?? [];
          const tokenProg = mintAcc?.owner;
          if (
            tokenProg !== PUMP_TOKEN_2022 &&
            tokenProg !== 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
          ) {
            continue;
          }
          const curveB64 = curveAcc?.data?.[0];
          if (!curveB64) continue;
          const curveBuf = Buffer.from(curveB64, 'base64');
          if (curveBuf.length < 81) continue;
          const creatorVault = deriveCreatorVault(curveBuf.subarray(49, 81));
          const assocCurve = deriveAta(p.bondingCurveKey, tokenProg, p.mint);
          if (!creatorVault || !assocCurve) return null;
          const isMayhem = curveBuf.length > 81 && curveBuf[81] !== 0;
          const feeRecipient = isMayhem ? PUMP_MAYHEM_FEE_RECIPIENT : PUMP_FEE_RECIPIENT;
          const buybackRecip = PUMP_BUYBACK_RECIPIENTS[(p.slot || 0) % PUMP_BUYBACK_RECIPIENTS.length];
          return [
            PUMP_GLOBAL,
            feeRecipient,
            p.mint,
            p.bondingCurveKey,
            assocCurve,
            hotWallet,
            hotWallet,
            SYSTEM_PROGRAM,
            tokenProg,
            creatorVault,
            PUMP_EVENT_AUTH,
            PUMPFUN_PROGRAM,
            PUMP_GLOBAL_VOL,
            userVolPda,
            PUMP_FEE_CONFIG,
            PUMP_FEE_PROGRAM,
            bcV2Pda,
            buybackRecip,
          ];
        } catch {
          continue;
        } finally {
          clearTimeout(t);
        }
      }
      return null;
    };

    try {
      // 0. If paid Helius WS already extracted exact Pump.fun Buy accounts, dispatch in <1ms;
      // otherwise run the ~80ms processed curve read + local 18-account builder (keeps +1 to +2 slot speed with 0% 2006/6000/6062/6074!).
      let dexAccounts: string[] | null = null;
      let fastLocal = false;
      if (p.prefetchedDexAccounts && p.prefetchedDexAccounts.length >= 16) {
        dexAccounts = [...p.prefetchedDexAccounts];
        if (userVolPda && dexAccounts.length > 13) dexAccounts[13] = userVolPda;
        if (bcV2Pda && dexAccounts.length >= 17) dexAccounts[16] = bcV2Pda;
        fastLocal = true;
      } else {
        dexAccounts = await tryFastLocalAccounts();
        fastLocal = dexAccounts !== null;
      }

      // 2. Fallback for Buyback/Cashback/Mayhem coins (or not yet at processed): poll getTransaction up to 2.7s
      if (!dexAccounts) {
        let pumpIxAccounts: string[] | null = null;
        const waitsMs = [0, 200, 300, 400, 500, 600, 700];
        for (let idx = 0; idx < waitsMs.length; idx++) {
          const wait = waitsMs[idx];
          if (wait > 0) await new Promise((r) => setTimeout(r, wait));
          if (p.signature) {
            pumpIxAccounts = await fetchPumpBuyAccounts(p.signature);
            if (pumpIxAccounts) break;
          }
          if (!p.signature || idx >= 2) {
            const sigs = await fetchSigs();
            for (const sig of sigs.slice(0, 2)) {
              if (sig === p.signature) continue;
              pumpIxAccounts = await fetchPumpBuyAccounts(sig);
              if (pumpIxAccounts) break;
            }
            if (pumpIxAccounts) break;
          }
        }
        if (!pumpIxAccounts) return;
        dexAccounts = [...pumpIxAccounts];
        if (userVolPda && dexAccounts.length > 13) dexAccounts[13] = userVolPda;
        if (bcV2Pda && dexAccounts.length >= 17) dexAccounts[16] = bcV2Pda;
      }

      const realOpen = this.cp.snapshot().positions.filter((x) => !x.id.startsWith('paper_'));
      if (realOpen.length >= 5 || realOpen.some((x) => x.mint === p.mint)) return;
      if (p.normSym) this.recentBoughtSymbols.set(p.normSym, Date.now());
      if (p.creator) {
        this.liveCreators.set(p.mint, p.creator);
        if (this.liveCreators.size > 500) {
          const oldest = this.liveCreators.keys().next().value;
          if (oldest) this.liveCreators.delete(oldest);
        }
      }
      if (this.pumpWs?.readyState === WebSocket.OPEN) {
        this.pumpWs.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [p.mint] }));
      }

      console.log(`[solana-feed] Dispatching live Pump.fun buy (${fastLocal ? 'FAST-LOCAL' : 'TX-FALLBACK'}): $${p.symbol} (${p.buySizeSol} SOL, ${dexAccounts.length} accs, slot ${p.slot})`);
      this.cp.dispatchLiveTrigger({
        kind: p.kolId ? 'kol_buy' : 'launch',
        venue: p.venue,
        mint: p.mint,
        poolPda: p.bondingCurveKey,
        slot: p.slot,
        buySizeSol: p.buySizeSol,
        maxCurveReserveSol: p.maxCurveReserveSol,
        kolId: p.kolId,
        kolWallet: p.kolWallet,
        creator: p.creator,
        devBuySol: p.devBuySol,
        symbol: p.symbol,
        name: p.name,
        uri: p.uri,
        dexAccounts,
      });
    } catch { /* ignore transient RPC timeout */ }
  }

  private async hydrateTokenImage(ev: FeedEvent, rawUri: string, sig: string | null) {
    const norm = (u: string) =>
      u.startsWith('ipfs://') ? `https://ipfs.io/ipfs/${u.slice(7)}` : u;
    const url = norm(rawUri.trim());
    if (!/^https?:\/\//i.test(url)) return;
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 3500);
      const r = await fetch(url, { signal: ctrl.signal });
      clearTimeout(timer);
      if (!r.ok) return;
      const meta = await r.json() as { image?: string };
      if (typeof meta.image === 'string' && meta.image.trim()) {
        const imgUrl = norm(meta.image.trim());
        if (/^https?:\/\//i.test(imgUrl)) {
          this.cp.ingestExternalEvent(
            { type: 'burst_dispatched', ts: ev.ts, burst: { ...ev, image: imgUrl } },
            sig,
          );
          const p = this.paper.get(ev.mint);
          if (p) {
            p.pos = { ...p.pos, image: imgUrl };
            this.cp.updatePaperState(this.listPaperPositions());
          }
        }
      }
    } catch { /* ignore metadata timeout / broken IPFS link */ }
  }

  // ---------- 3. Shadow Mode Real-Price Paper Trader (1:1 parity with Live Mode) ----------
  private markKolSigSeen(sig: string) {
    this.seenKolSigs.add(sig);
    if (this.seenKolSigs.size > 500) {
      const first = this.seenKolSigs.values().next().value;
      if (first) this.seenKolSigs.delete(first);
    }
  }

  private syncKolSubscriptions() {
    const kols = this.cp.snapshot().kols.filter((k) => k.enabled);
    const next = new Set(kols.map((k) => k.pubkey));
    this.watchedKolKeys = next;

    // 1. PumpPortal account-trade subscriptions (re-subscribes cleanly after every reconnect)
    if (this.pumpWs?.readyState === WebSocket.OPEN) {
      const addedPump = [...next].filter((k) => !this.pumpSubscribedKols.has(k));
      const removedPump = [...this.pumpSubscribedKols].filter((k) => !next.has(k));
      if (addedPump.length) {
        this.pumpWs.send(JSON.stringify({ method: 'subscribeAccountTrade', keys: addedPump }));
        for (const k of addedPump) this.pumpSubscribedKols.add(k);
      }
      if (removedPump.length) {
        this.pumpWs.send(JSON.stringify({ method: 'unsubscribeAccountTrade', keys: removedPump }));
        for (const k of removedPump) this.pumpSubscribedKols.delete(k);
      }
    }

    // 2. Solana RPC WebSocket logsSubscribe (processed commitment, ~250-500ms across ALL DEXs)
    if (this.slotWs?.readyState === WebSocket.OPEN) {
      const pendingPubkeys = new Set(this.rpcKolReqToPubkey.values());
      for (const pubkey of next) {
        if (this.rpcKolPubkeyToSubId.has(pubkey) || pendingPubkeys.has(pubkey)) continue;
        const reqId = this.nextRpcSubReqId++;
        this.rpcKolReqToPubkey.set(reqId, pubkey);
        this.slotWs.send(JSON.stringify({
          jsonrpc: '2.0',
          id: reqId,
          method: 'logsSubscribe',
          params: [{ mentions: [pubkey] }, { commitment: 'processed' }],
        }));
      }
      for (const [pubkey, subId] of [...this.rpcKolPubkeyToSubId.entries()]) {
        if (next.has(pubkey)) continue;
        this.rpcKolPubkeyToSubId.delete(pubkey);
        this.rpcKolSubIdToPubkey.delete(subId);
        this.slotWs.send(JSON.stringify({
          jsonrpc: '2.0',
          id: this.nextRpcSubReqId++,
          method: 'logsUnsubscribe',
          params: [subId],
        }));
      }
    }
  }

  /** Parses a confirmed non-reverted KOL transaction detected via Solana RPC logsSubscribe. */
  private async inspectKolRpcTransaction(
    kolPubkey: string,
    signature: string,
    slot: number,
    venue: FeedEvent['venue'],
  ) {
    // Always use primary paid Helius HTTP RPC for live account lookup (never fall back to rate-limited public mainnet-beta HTTP).
    const rpcUrl = this.endpoints.httpUrl;
    const fetchTx = async () => {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 3500);
      try {
        const r = await fetch(rpcUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          signal: ctrl.signal,
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'getTransaction',
            params: [signature, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1, commitment: 'confirmed' }],
          }),
        });
        if (!r.ok) return null;
        const j = await r.json() as {
          result?: {
            meta?: {
              err: unknown;
              preBalances?: number[];
              postBalances?: number[];
              preTokenBalances?: Array<{ owner?: string; mint?: string; uiTokenAmount?: { uiAmount?: number | null } }>;
              postTokenBalances?: Array<{ owner?: string; mint?: string; uiTokenAmount?: { uiAmount?: number | null } }>;
              innerInstructions?: Array<{ instructions?: Array<{ programId?: string; accounts?: string[] }> }>;
            };
            transaction?: {
            message?: {
              accountKeys?: Array<string | { pubkey: string }>;
              instructions?: Array<{ programId?: string; accounts?: string[] }>;
            };
          };
          } | null;
        };
        return j.result ?? null;
      } finally {
        clearTimeout(t);
      }
    };

    try {
      let tx = await fetchTx();
      if (!tx) {
        await new Promise((res) => setTimeout(res, 500));
        tx = await fetchTx();
      }
      if (!tx?.meta || tx.meta.err !== null) return;

      const keys = (tx.transaction?.message?.accountKeys ?? []).map((k) => (typeof k === 'string' ? k : k.pubkey));
      const idx = keys.indexOf(kolPubkey);
      if (idx < 0) return;

      const preLamports = tx.meta.preBalances?.[idx] ?? 0;
      const postLamports = tx.meta.postBalances?.[idx] ?? 0;
      let solSpent = (preLamports - postLamports) / 1e9;

      const preTok = new Map<string, number>();
      for (const tb of tx.meta.preTokenBalances ?? []) {
        if (tb.owner === kolPubkey && tb.mint) preTok.set(tb.mint, tb.uiTokenAmount?.uiAmount ?? 0);
      }
      const postTok = new Map<string, number>();
      for (const tb of tx.meta.postTokenBalances ?? []) {
        if (tb.owner === kolPubkey && tb.mint) postTok.set(tb.mint, tb.uiTokenAmount?.uiAmount ?? 0);
      }

      // Include WSOL spend if the KOL bought using wrapped SOL
      const wsolMint = 'So11111111111111111111111111111111111111112';
      const wsolDelta = (preTok.get(wsolMint) ?? 0) - (postTok.get(wsolMint) ?? 0);
      if (wsolDelta > 0) solSpent += wsolDelta;

      // Check if the KOL SOLD a token we currently hold (Mirror KOL Exit via Solana RPC)
      const kolForExit = this.cp.snapshot().kols.find((k) => k.pubkey === kolPubkey);
      if (kolForExit) {
        for (const [mint, preAmt] of preTok.entries()) {
          if (IGNORED_MINTS.has(mint) || preAmt <= 0) continue;
          const postAmt = postTok.get(mint) ?? 0;
          if (preAmt - postAmt > preAmt * 0.25) {
            const held = this.paper.get(mint);
            if (held && held.pos.kolId === kolForExit.id) {
              this.closePaperPosition(mint, `Mirrored KOL sell (${kolForExit.label})`);
            }
            if (this.cp.engineStatus === 'RUNNING') {
              const liveHeld = this.cp.snapshot().positions.find(
                (p) => !p.id.startsWith('paper_') && p.mint === mint && p.kolId === kolForExit.id,
              );
              if (liveHeld) {
                this.cp.dispatchLiveMirrorSell(mint, kolForExit.label);
              }
            }
          }
        }
      }

      // Find the token the KOL acquired (positive net balance delta, ignoring WSOL/USDC/USDT)
      let boughtMint: string | null = null;
      let boughtTokens = 0;
      for (const [mint, postAmt] of postTok.entries()) {
        if (IGNORED_MINTS.has(mint)) continue;
        const delta = postAmt - (preTok.get(mint) ?? 0);
        if (delta > boughtTokens) {
          boughtTokens = delta;
          boughtMint = mint;
        }
      }
      if (!boughtMint || boughtTokens <= 0) return;

      const kol = this.cp.snapshot().kols.find((k) => k.enabled && k.pubkey === kolPubkey);
      if (!kol || solSpent < kol.triggerMargin) return;

      const allKolIx = [
        ...(tx.transaction?.message?.instructions ?? []),
        ...(tx.meta.innerInstructions ?? []).flatMap((g) => g.instructions ?? []),
      ];
      const dexIx = [...allKolIx].reverse().find((ix) => {
        if (!ix.programId || !Array.isArray(ix.accounts)) return false;
        if (ix.programId === PUMPFUN_PROGRAM) {
          // Only accept classic Pump.fun layout ([2] === boughtMint); V2/V3 ([2] === WSOL) falls through to 18-acc builder.
          return ix.accounts.length >= 16 && ix.accounts[2] === boughtMint;
        }
        if (ix.programId === 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA') {
          return ix.accounts.length >= 17 && ix.accounts[3] === boughtMint;
        }
        return false;
      });
      const dexAccounts = dexIx?.accounts ? [...dexIx.accounts] : [];
      if (dexIx?.programId === PUMPFUN_PROGRAM && dexAccounts.length > 13) {
        const userVolPda = deriveUserVolumeAccumulator(this.cp.configState().config.wallets.hot_wallet_pubkey);
        if (userVolPda) dexAccounts[13] = userVolPda;
      }
      const poolPda = dexIx?.programId === 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        ? (dexAccounts[0] ?? boughtMint)
        : (dexAccounts[3] ?? dexAccounts[0] ?? boughtMint);
      const entryPriceSol = solSpent / boughtTokens;
      this.onKolBuySignal(
        {
          signature,
          mint: boughtMint,
          traderPublicKey: kolPubkey,
          bondingCurveKey: poolPda,
          solAmount: +solSpent.toFixed(4),
          venueOverride: venue,
          slotOverride: slot,
          entryPriceSol,
          dexAccounts,
        },
        this.pumpWs ?? undefined,
      );
    } catch { /* ignore transient RPC fetch error */ }
  }

  private onKolBuySignal(
    msg: {
      signature?: string;
      mint?: string;
      traderPublicKey?: string;
      bondingCurveKey?: string;
      symbol?: string;
      solAmount?: number;
      vSolInBondingCurve?: number;
      vTokensInBondingCurve?: number;
      pool?: string;
      venueOverride?: FeedEvent['venue'];
      slotOverride?: number;
      entryPriceSol?: number;
      dexAccounts?: string[];
    },
    ws?: WebSocket,
  ) {
    const cfg = this.cp.configState().config;
    if (!cfg.strategy.modes.kol_backrun || !msg.mint || !msg.traderPublicKey) return;
    if (msg.signature) {
      if (this.seenKolSigs.has(msg.signature) && !msg.venueOverride) return;
      this.markKolSigSeen(msg.signature);
    }
    const kol = this.cp.snapshot().kols.find((k) => k.enabled && k.pubkey === msg.traderPublicKey);
    if (!kol) return;
    if (msg.vSolInBondingCurve && msg.vSolInBondingCurve > kol.maxCurveReserveSol) return;

    const ts = Date.now();
    const slot = msg.slotOverride ?? (this.cp.currentSlot() || this.lastSlot);
    const symbol = (msg.symbol || msg.mint.slice(0, 4)).trim().slice(0, 16);
    const venue: FeedEvent['venue'] = msg.venueOverride ?? (msg.pool === 'raydium' ? 'Raydium LaunchLab' : 'Pump.fun');
    const ev: FeedEvent = {
      id: `kol_${(msg.signature || msg.mint).slice(0, 16)}`,
      ts,
      slot,
      kind: 'kol_buy',
      kolId: kol.id,
      mint: msg.mint,
      symbol,
      venue,
      sizeSol: kol.buySizeSol,
      fanout: 0,
      winner: null,
      cuUsed: 0,
      reverts: ZERO_REVERTS,
      outcome: this.cp.engineStatus === 'SHADOW' ? 'shadow' : 'missed',
      note: `Copied ${kol.label} · ${msg.solAmount ? `${msg.solAmount} SOL · ` : ''}${msg.venueOverride ? 'RPC live' : 'live mainnet'}`,
    };
    this.cp.ingestExternalEvent({ type: 'kol_shred_detected', ts, slot, kolId: kol.id, mint: ev.mint, symbol, venue: ev.venue });
    this.cp.ingestExternalEvent({ type: 'burst_dispatched', ts, burst: ev }, msg.signature ?? null);
    if (this.cp.engineStatus === 'SHADOW' && this.paper.size < 5 && !this.paper.has(msg.mint)) {
      this.openPaperPosition(ev, kol.buySizeSol, msg.vSolInBondingCurve, msg.vTokensInBondingCurve, ws, msg.bondingCurveKey, msg.entryPriceSol);
    } else if (this.cp.engineStatus === 'RUNNING') {
      // Direct-native Rust executor supports Pump.fun bonding-curve + PumpSwap AMM (with atomic WSOL wrap/unwrap).
      if (ev.venue !== 'Pump.fun' && ev.venue !== 'PumpSwap') return;
      if (ev.venue === 'PumpSwap' && (!msg.dexAccounts || msg.dexAccounts.length < 17)) return;
      const realOpen = this.cp.snapshot().positions.filter((p) => !p.id.startsWith('paper_'));
      if (realOpen.length >= 5 || realOpen.some((p) => p.mint === msg.mint)) return;
      const maxAffordable = this.solBalance > 0.11 ? +(this.solBalance - 0.06).toFixed(4) : kol.buySizeSol;
      const liveBuySol = Math.max(0.05, Math.min(kol.buySizeSol, maxAffordable));
      if (this.pumpWs?.readyState === WebSocket.OPEN) {
        this.pumpWs.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [msg.mint] }));
      }
      if (ev.venue === 'Pump.fun' && (!msg.dexAccounts || msg.dexAccounts.length < 16)) {
        const mintBuf = b58Decode(msg.mint);
        const curveKey = msg.bondingCurveKey || (mintBuf.length === 32 ? derivePda([Buffer.from('bonding-curve'), mintBuf], PUMPFUN_PROGRAM) : null);
        if (!curveKey) return;
        void this.dispatchPumpLaunchLive({
          venue: ev.venue,
          mint: msg.mint,
          bondingCurveKey: curveKey,
          signature: msg.signature,
          slot,
          buySizeSol: liveBuySol,
          maxCurveReserveSol: kol.maxCurveReserveSol,
          devBuySol: msg.solAmount ?? liveBuySol,
          symbol,
          kolId: kol.id,
          kolWallet: kol.pubkey,
        });
        return;
      }
      console.log(`[solana-feed] Dispatching live KOL buy: $${symbol} via ${kol.label} (${liveBuySol} SOL [cfg ${kol.buySizeSol}], ${(msg.dexAccounts ?? []).length} accs)`);
      this.cp.dispatchLiveTrigger({
        kind: 'kol_buy',
        venue: ev.venue,
        mint: msg.mint,
        poolPda: msg.bondingCurveKey || msg.mint,
        slot,
        buySizeSol: liveBuySol,
        maxCurveReserveSol: kol.maxCurveReserveSol,
        kolId: kol.id,
        kolWallet: kol.pubkey,
        symbol,
        dexAccounts: msg.dexAccounts ?? [],
      });
    }
  }

  private listPaperPositions(): Position[] {
    return [...this.paper.values()].map((e) => e.pos);
  }

  private openPaperPosition(ev: FeedEvent, desiredBuySol: number, vSol?: number, vTokens?: number, ws?: WebSocket, bondingCurveKey?: string, overrideEntryPrice?: number, creator?: string) {
    const cfg = this.cp.configState().config;
    const buySol = Math.max(0.01, Math.min(100, desiredBuySol || cfg.strategy.default_buy_sol || 1));
    const buyFeeSol = this.cp.estimateExecutionFeeSol();
    const entrySol = +(buySol + buyFeeSol).toFixed(6);

    let tokens: number;
    let effectiveEntryPrice: number;
    let postVSol: number;
    let markPrice: number;

    if ((!vSol || !vTokens) && overrideEntryPrice && overrideEntryPrice > 0) {
      // Non-Pump.fun DEX KOL buy parsed via RPC: 1% fee + 1.5% realistic backrun slippage
      const netSolIn = buySol * 0.99;
      effectiveEntryPrice = overrideEntryPrice * 1.015;
      tokens = netSolIn / effectiveEntryPrice;
      markPrice = overrideEntryPrice;
      postVSol = 50;
    } else {
      // Exact Pump.fun constant-product AMM (x * y = k) + competing sniper front-run impact (+1 slot realism).
      const baseVSol = vSol && vSol > 0 ? vSol : 30;
      const baseVTokens = vTokens && vTokens > 0 ? vTokens : 1_073_000_000;
      const basePrice = baseVSol / baseVTokens;
      const competitionSol = ev.sizeSol >= 1.0 ? 0.5 : 0.25; // competing bots landing ahead in same/+1 slot
      const preVSol = baseVSol + competitionSol;
      const preVTokens = (baseVSol * baseVTokens) / preVSol;

      const netSolIn = buySol * 0.99; // Pump.fun 1% protocol fee
      tokens = (preVTokens * netSolIn) / (preVSol + netSolIn);
      effectiveEntryPrice = buySol / tokens;
      // Filter 3 (Fhbh1D strict slippage cap): abort rather than overpay if front-run + buy impact exceeds slippage_bps (default 10%)
      const maxSlipBps = Math.max(cfg.strategy.slippage_bps || 800, 1500);
      const actualSlipBps = ((effectiveEntryPrice - basePrice) / basePrice) * 10_000;
      if (actualSlipBps > maxSlipBps) {
        console.log(`[solana-feed] Skipped $${ev.symbol}: entry slippage ${(actualSlipBps / 100).toFixed(1)}% > ${(maxSlipBps / 100).toFixed(1)}% cap`);
        return;
      }
      postVSol = preVSol + netSolIn;
      const postVTokens = preVTokens - tokens;
      markPrice = postVSol / postVTokens;
    }

    const pos: Position = {
      id: `paper_${ev.mint.slice(0, 12)}`,
      mint: ev.mint,
      symbol: ev.symbol,
      image: ev.image,
      venue: ev.venue,
      kolId: ev.kolId,
      openedAt: ev.ts,
      openedSlot: ev.slot || this.cp.currentSlot() || this.lastSlot,
      entrySol,
      initialTokens: tokens,
      tokens,
      entryPrice: effectiveEntryPrice,
      price: markPrice,
      realizedSol: 0,
      proceedsSol: 0,
      pendingSell: null,
    };
    this.paper.set(ev.mint, { pos, peakPrice: markPrice, vSol: postVSol, bondingCurveKey, creator, hasLiveTick: false, breakevenArmed: false, instantTpFired: false, lastTwapAt: 0, firedSteps: new Set() });
    console.log(`[solana-feed] Shadow AMM buy: $${ev.symbol} (${buySol} SOL + ${buyFeeSol} fee, vSol=${postVSol.toFixed(1)}, curve=${bondingCurveKey ?? 'n/a'})`);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [ev.mint] }));
    }
    this.cp.updatePaperState(this.listPaperPositions(), undefined, true);
  }

  /** Sub-second on-chain Pump.fun bonding-curve reader via batch getMultipleAccounts (commitment: processed). */
  private async pollBondingCurvesOnChain() {
    if (this.inFlightCurvePoll) return;
    const targets: { mint: string; curve: string }[] = [];
    for (const [mint, e] of this.paper.entries()) {
      if (e.bondingCurveKey) targets.push({ mint, curve: e.bondingCurveKey });
    }
    if (targets.length === 0) return;
    this.inFlightCurvePoll = true;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 1500);
      const r = await fetch(this.endpoints.httpUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'getMultipleAccounts',
          params: [targets.map((x) => x.curve), { encoding: 'base64', commitment: 'processed' }],
        }),
        signal: ctrl.signal,
      });
      clearTimeout(t);
      if (!r.ok) return;
      const json = (await r.json()) as { result?: { value?: ({ data?: [string, string] } | null)[] } };
      const vals = json.result?.value;
      if (!Array.isArray(vals)) return;
      let anyChanged = false;
      for (let i = 0; i < targets.length; i++) {
        const b64 = vals[i]?.data?.[0];
        if (!b64) continue;
        const buf = Buffer.from(b64, 'base64');
        if (buf.length < 49) continue;
        // Pump.fun BondingCurve layout: disc(0..8) | virtual_token_reserves(8..16) | virtual_sol_reserves(16..24) | ... | complete(48)
        const complete = buf[48] === 1;
        if (complete) {
          // Graduated off bonding curve -> let DexScreener track PumpSwap/Raydium price
          const entry = this.paper.get(targets[i].mint);
          if (entry) entry.bondingCurveKey = undefined;
          continue;
        }
        const vTokens = Number(buf.readBigUInt64LE(8)) / 1e6;
        const vSol = Number(buf.readBigUInt64LE(16)) / 1e9;
        if (vTokens > 0 && vSol > 0) {
          if (this.applyPaperPrice(targets[i].mint, vSol / vTokens, vSol, true)) anyChanged = true;
        }
      }
      if (anyChanged) this.cp.updatePaperState(this.listPaperPositions());
    } catch { /* ignore transient RPC timeouts */ }
    finally { this.inFlightCurvePoll = false; }
  }

  /** Watchdog: terminates and reconnects zombie/half-open WebSockets that stop delivering messages. */
  private checkStaleStreams() {
    if (this.stopped) return;
    const now = Date.now();
    if (this.pumpWs?.readyState === WebSocket.OPEN && this.lastPumpMsgAt > 0 && now - this.lastPumpMsgAt > 30_000) {
      console.warn('[solana-feed] Pump.fun WS stale >30s; terminating and reconnecting');
      const old = this.pumpWs;
      this.pumpWs = null;
      try { old.terminate(); } catch { /* ignore */ }
      this.connectPumpStream();
    }
    if (this.slotWs?.readyState === WebSocket.OPEN && this.lastSlotMsgAt > 0 && now - this.lastSlotMsgAt > 20_000) {
      console.warn('[solana-feed] Slot WS stale >20s; terminating and reconnecting');
      const old = this.slotWs;
      this.slotWs = null;
      try { old.terminate(); } catch { /* ignore */ }
      this.connectSlotStream();
    }
  }

  /** Computes exact constant-product AMM sell proceeds after Pump.fun 1% fee & SWQoS execution fee. */
  private calcAmmSellProceeds(sellTokens: number, markPrice: number, vSol: number): number {
    const curveSol = Math.max(30, vSol);
    const curveTokens = curveSol / markPrice;
    const grossSolOut = Math.min(curveSol * 0.85, (curveSol * sellTokens) / (curveTokens + sellTokens));
    const sellFeeSol = this.cp.estimateExecutionFeeSol();
    return Math.max(0, grossSolOut * 0.99 - sellFeeSol);
  }

  private onPaperTrade(mint: string, vSol?: number, vTokens?: number) {
    if (!vSol || !vTokens || vSol <= 0 || vTokens <= 0) return;
    this.applyPaperPrice(mint, vSol / vTokens, vSol);
  }

  private async pollPaperPricesFromDexScreener() {
    // Only poll DexScreener for mints WITHOUT an active on-chain bondingCurveKey (prevents stale DexScreener cache from flickering against live RPC!).
    const fallbackMints: string[] = [];
    for (const [mint, e] of this.paper.entries()) {
      if (!e.bondingCurveKey) fallbackMints.push(mint);
    }
    if (fallbackMints.length === 0) return;
    const mints = fallbackMints.join(',');
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 3000);
      const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${mints}`, { signal: ctrl.signal });
      clearTimeout(t);
      if (!r.ok) return;
      const data = (await r.json()) as {
        pairs?: { chainId?: string; baseToken?: { address?: string }; priceNative?: string; liquidity?: { quote?: number; usd?: number } }[];
      };
      if (!Array.isArray(data.pairs)) return;
      const bestByMint = new Map<string, { price: number; quoteLiq?: number }>();
      for (const pair of data.pairs) {
        if (pair.chainId && pair.chainId !== 'solana') continue;
        if (pair.liquidity?.usd != null && pair.liquidity.usd < 1000) continue; // ignore spoofed micro-liquidity pools
        const mint = pair.baseToken?.address;
        const price = Number(pair.priceNative);
        if (!mint || !Number.isFinite(price) || price <= 0) continue;
        if (!bestByMint.has(mint)) bestByMint.set(mint, { price, quoteLiq: pair.liquidity?.quote });
      }
      let anyChanged = false;
      for (const [mint, info] of bestByMint) {
        if (this.applyPaperPrice(mint, info.price, info.quoteLiq, true)) anyChanged = true;
      }
      if (anyChanged) this.cp.updatePaperState(this.listPaperPositions());
    } catch { /* ignore transient DexScreener timeouts */ }
  }

  private applyPaperPrice(mint: string, price: number, vSol?: number, batch = false): boolean {
    const entry = this.paper.get(mint);
    if (!entry || !Number.isFinite(price) || price <= 0) return false;
    const prev = entry.pos.price;
    // Outlier / decimal-mismatch sanity guard: reject impossible single-tick >8x spikes or <0.05x glitches
    if (prev > 0 && (price / prev > 8 || price / prev < 0.05)) return false;
    if (entry.pos.entryPrice > 0 && price / entry.pos.entryPrice > 25 && (!vSol || vSol < 50)) return false;
    const changed = prev <= 0 || Math.abs(price - prev) / prev > 1e-6;
    entry.hasLiveTick = true;
    if (!changed) return false;
    entry.pos = { ...entry.pos, price };
    if (vSol && vSol > 0) entry.vSol = vSol;
    if (price > entry.peakPrice) entry.peakPrice = price;

    const now = Date.now();
    const heldSecs = (now - entry.pos.openedAt) / 1000;
    const mult = entry.pos.entryPrice > 0 ? price / entry.pos.entryPrice : 1;
    if (mult >= 1.25) entry.breakevenArmed = true;
    const exits = this.cp.configState().config.exits;
    const slPct = exits.stop_loss_pct ?? 30;

    // 1. Curve liquidity floor protection (same as live Lighthouse guard)
    if (exits.curve_floor.enabled && vSol != null && vSol > 0 && vSol < exits.curve_floor.min_real_sol_reserves_sol) {
      this.closePaperPosition(mint, 'Curve floor');
      return false;
    }

    // 1b. Fhbh1D 2–3s Fail-Fast Stop-Loss: dump 100% at -6% if momentum fails to follow through early
    if (heldSecs >= 2.5 && heldSecs <= 12 && mult <= 0.94 && entry.peakPrice < entry.pos.entryPrice * 1.10) {
      if (entry.creator) this.ruggedCreators.add(entry.creator);
      this.closePaperPosition(mint, 'Fail-fast SL (2-3s)');
      return false;
    }

    // 2. Hard stop-loss check
    if (slPct > 0 && mult <= 1 - slPct / 100) {
      if (entry.creator) this.ruggedCreators.add(entry.creator);
      this.closePaperPosition(mint, `Stop-loss (-${slPct}%)`);
      return false;
    }

    // 2b. Breakeven shield: once up >= +25%, protect entry + 3% fee buffer so winners never turn red
    if (entry.breakevenArmed && mult <= 1.03) {
      this.closePaperPosition(mint, 'Breakeven shield');
      return false;
    }

    // 3. Trailing stop-loss (only arms once peak >= +30% to avoid early launch wick stop-outs)
    if (exits.trailing_stop_pct && exits.trailing_stop_pct > 0 && entry.peakPrice >= entry.pos.entryPrice * 1.30) {
      const dropFromPeak = (entry.peakPrice - price) / entry.peakPrice;
      if (dropFromPeak >= exits.trailing_stop_pct / 100) {
        this.closePaperPosition(mint, `Trailing stop (-${exits.trailing_stop_pct}%)`);
        return false;
      }
    }

    // 3b. Fhbh1D Instant Opening Take-Profit (0–4s pop >= +18%)
    if (!entry.instantTpFired && exits.instant_tp_pct > 0 && mult >= 1.18) {
      entry.instantTpFired = true;
      entry.lastTwapAt = now;
      if (exits.instant_tp_pct >= 100) {
        this.closePaperPosition(mint, `Instant TP1 (${exits.instant_tp_pct}%)`);
        return false;
      }
      const sellTokens = entry.pos.tokens * (exits.instant_tp_pct / 100);
      const proceeds = this.calcAmmSellProceeds(sellTokens, price, entry.vSol);
      entry.pos = {
        ...entry.pos,
        tokens: entry.pos.tokens - sellTokens,
        proceedsSol: +((entry.pos.proceedsSol ?? 0) + proceeds).toFixed(6),
        realizedSol: +(entry.pos.realizedSol + proceeds).toFixed(6),
      };
    }

    // 3c. Fhbh1D Exponential TWAP Drip: sell 15% of REMAINING balance every 2s while >= +18%, leaving a ~30% moonbag to trail
    const remRatio = entry.pos.initialTokens > 0 ? entry.pos.tokens / entry.pos.initialTokens : 0;
    if (entry.instantTpFired && mult >= 1.18 && remRatio > 0.32 && now - entry.lastTwapAt >= 2_000) {
      entry.lastTwapAt = now;
      const sellTokens = entry.pos.tokens * 0.15;
      const proceeds = this.calcAmmSellProceeds(sellTokens, price, entry.vSol);
      entry.pos = {
        ...entry.pos,
        tokens: entry.pos.tokens - sellTokens,
        proceedsSol: +((entry.pos.proceedsSol ?? 0) + proceeds).toFixed(6),
        realizedSol: +(entry.pos.realizedSol + proceeds).toFixed(6),
      };
    }

    // 4. Exit ladder take-profit rungs (pnl_multiple: e.g. 15% @ 2x, 33% @ 3x, 100%)
    for (let i = 0; i < exits.ladder.length; i++) {
      const step = exits.ladder[i];
      if (entry.firedSteps.has(i) || step.trigger.kind !== 'pnl_multiple') continue;
      if (mult >= step.trigger.value) {
        entry.firedSteps.add(i);
        if (step.sell_pct >= 100) {
          this.closePaperPosition(mint, `TP ${step.trigger.value}x`);
          return false;
        }
        const sellTokens = entry.pos.tokens * (step.sell_pct / 100);
        const proceeds = this.calcAmmSellProceeds(sellTokens, price, entry.vSol);
        entry.pos = {
          ...entry.pos,
          tokens: entry.pos.tokens - sellTokens,
          proceedsSol: +((entry.pos.proceedsSol ?? 0) + proceeds).toFixed(6),
          realizedSol: +(entry.pos.realizedSol + proceeds).toFixed(6),
        };
      }
    }

    if (!batch) this.cp.updatePaperState(this.listPaperPositions());
    return true;
  }

  private closePaperPosition(mint: string, exitReason: string) {
    const entry = this.paper.get(mint);
    if (!entry) return;
    this.paper.delete(mint);
    const remainingProceeds = this.calcAmmSellProceeds(entry.pos.tokens, entry.pos.price, entry.vSol);
    const totalProceeds = +((entry.pos.proceedsSol ?? 0) + remainingProceeds).toFixed(6);
    const closed: ClosedPosition = {
      id: `cl_${entry.pos.id}_${Date.now().toString(36)}`,
      mint: entry.pos.mint,
      symbol: entry.pos.symbol,
      image: entry.pos.image,
      venue: entry.pos.venue,
      kolId: entry.pos.kolId,
      openedAt: entry.pos.openedAt,
      closedAt: Date.now(),
      entrySol: entry.pos.entrySol,
      proceedsSol: totalProceeds,
      solUsd: this.solUsd || 0,
      exitReason,
      buySlot: entry.pos.openedSlot > 0 ? entry.pos.openedSlot : null,
    };
    if (this.pumpWs?.readyState === WebSocket.OPEN) {
      this.pumpWs.send(JSON.stringify({ method: 'unsubscribeTokenTrade', keys: [mint] }));
    }
    this.cp.updatePaperState(this.listPaperPositions(), closed);
  }

  private sweepExpiredPaperPositions() {
    const now = Date.now();
    const exits = this.cp.configState().config.exits;
    for (const [mint, entry] of this.paper.entries()) {
      const heldSecs = (now - entry.pos.openedAt) / 1000;
      // Evaluate time_secs exit ladder rungs
      for (let i = 0; i < exits.ladder.length; i++) {
        const step = exits.ladder[i];
        if (entry.firedSteps.has(i) || step.trigger.kind !== 'time_secs') continue;
        if (heldSecs >= step.trigger.value) {
          entry.firedSteps.add(i);
          if (step.sell_pct >= 100) {
            this.closePaperPosition(mint, `Time rung (${step.trigger.value}s)`);
            break;
          }
          const sellTokens = entry.pos.tokens * (step.sell_pct / 100);
          const proceeds = this.calcAmmSellProceeds(sellTokens, entry.pos.price, entry.vSol);
          entry.pos = {
            ...entry.pos,
            tokens: entry.pos.tokens - sellTokens,
            proceedsSol: +((entry.pos.proceedsSol ?? 0) + proceeds).toFixed(6),
            realizedSol: +(entry.pos.realizedSol + proceeds).toFixed(6),
          };
          this.cp.updatePaperState(this.listPaperPositions());
        }
      }
      if (!this.paper.has(mint)) continue;
      const mult = entry.pos.entryPrice > 0 ? entry.pos.price / entry.pos.entryPrice : 1;
      // 45s dead-position time-stop (1:1 parity with 11-24 Paper Mode winner) for flat/dead positions (< +5% and never popped >= +18%)
      if (entry.hasLiveTick && heldSecs >= 45 && mult < 1.05 && entry.peakPrice < entry.pos.entryPrice * 1.18) {
        this.closePaperPosition(mint, 'Time-stop (45s)');
        continue;
      }
      // 5m time cap for non-runners (< +18%); let 30% moonbag runners ride up to 26m hard max
      if (heldSecs >= 300 && mult < 1.18 && !entry.breakevenArmed) {
        this.closePaperPosition(mint, 'Time cap (5m)');
        continue;
      }
      if (heldSecs >= 1560) {
        this.closePaperPosition(mint, 'Moonbag max hold (26m)');
      }
    }
  }

  private emitBalances() {
    const now = Date.now();
    const nonces = this.cp.configState().config.wallets.nonce_accounts.length;
    this.cp.ingestExternalEvent({
      type: 'slot_tick',
      ts: now,
      chain: {
        slot: this.lastSlot,
        shredLatencyMs: 0,
        nonces: { ready: nonces, total: nonces },
        ring: { used: 0, size: 64 },
      },
      balances: {
        sol: this.solBalance,
        wsol: this.wsolBalance,
        usdc: this.usdcBalance,
        solUsd: this.solUsd,
      },
    });
  }

  private async refreshSolBalanceFast() {
    const hotWallet = this.cp.configState().config.wallets.hot_wallet_pubkey;
    if (!hotWallet) return;
    try {
      const r = await fetch(this.endpoints.httpUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getBalance', params: [hotWallet, { commitment: 'processed' }] }),
      });
      if (!r.ok) return;
      const j = await r.json() as { result?: { value?: number } };
      if (typeof j.result?.value === 'number') {
        const nextSol = j.result.value / 1e9;
        if (Math.abs(nextSol - this.solBalance) > 1e-6) {
          this.solBalance = nextSol;
        }
        this.emitBalances();
      }
    } catch { /* ignore */ }
  }

  private async refreshMarketAndBalance() {
    try {
      const r = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd');
      if (r.ok) {
        const j = await r.json() as { solana?: { usd?: number } };
        if (typeof j.solana?.usd === 'number' && j.solana.usd > 0) this.solUsd = j.solana.usd;
      }
    } catch { /* ignore */ }

    const hotWallet = this.cp.configState().config.wallets.hot_wallet_pubkey;
    if (hotWallet) {
      // Always use primary paid Helius HTTP RPC for live account lookup (never fall back to rate-limited public mainnet-beta HTTP).
    const rpcUrl = this.endpoints.httpUrl;
      const sumSplMint = async (mint: string): Promise<number | null> => {
        try {
          const r = await fetch(rpcUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'getTokenAccountsByOwner',
              params: [hotWallet, { mint }, { encoding: 'jsonParsed', commitment: 'confirmed' }],
            }),
          });
          if (!r.ok) return null;
          const j = await r.json() as {
            result?: { value?: Array<{ account?: { data?: { parsed?: { info?: { tokenAmount?: { uiAmount?: number | null } } } } } }> };
          };
          const list = j.result?.value;
          if (!Array.isArray(list)) return null;
          return list.reduce((a, x) => a + (x.account?.data?.parsed?.info?.tokenAmount?.uiAmount ?? 0), 0);
        } catch {
          return null;
        }
      };
      try {
        const r = await fetch(rpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getBalance', params: [hotWallet] }),
        });
        const j = await r.json() as { result?: { value?: number } };
        if (typeof j.result?.value === 'number') this.solBalance = j.result.value / 1e9;
      } catch { /* ignore */ }
      const [wsol, usdc] = await Promise.all([
        sumSplMint('So11111111111111111111111111111111111111112'),
        sumSplMint('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
      ]);
      if (wsol !== null) this.wsolBalance = +wsol.toFixed(6);
      if (usdc !== null) this.usdcBalance = +usdc.toFixed(2);
    }
    this.emitBalances();
  }
}
