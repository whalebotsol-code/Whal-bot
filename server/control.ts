// Control-plane domain logic: owns config versions, KOL rules, history and audit; relays to the engine.
import { EventEmitter } from 'node:events';
import { z } from 'zod';
import { DEFAULT_CONFIG, LANDER_LABEL, diffConfig, validateConfig, type BotConfig, type FieldErrors } from '../lib/config/schema';
import { DEFAULT_KOLS } from '../lib/config/seed-kols';
import { isPubkey } from '../lib/pubkey';
import { SELL_PCTS, type EngineSource, type SellPct, type StateSnapshot, type TelemetryEvent } from '../lib/telemetry';
import type { ClosedPosition, ConfigState, Kol, LanderId, LanderStat, NonceHealth, Position } from '../lib/types';
import type { CloudSync } from './cloud-sync';
import type { Db } from './db';
import { HttpError } from './http';
import type { SecretStore } from './secrets';
import { EngineError, type AdapterEvent, type BridgeTriggerPayload, type EngineAdapter, type EngineRuntime } from './engine/types';

export interface Actor { actor: string; ip: string | null }

export const KolInput = z.object({
  label: z.string().trim().min(1, 'Give this wallet a label').max(32, '32 characters max'),
  pubkey: z.string().trim().refine(isPubkey, 'Not a valid Solana public key'),
  quote: z.enum(['SOL', 'WSOL', 'USDC', 'BONK']),
  triggerMargin: z.number().gt(0, 'Must be greater than 0'),
  buySizeSol: z.number().min(0.01).max(100),
  maxCurveReserveSol: z.number().gt(0),
  enabled: z.boolean(),
}).strict();
export const KolPatch = KolInput.partial().strict();
export const ControlInput = z.object({
  action: z.enum(['start', 'pause', 'shadow_mode', 'emergency_kill_switch']),
  reason: z.string().trim().max(200).optional(),
}).strict();
export const SellInput = z.object({ pct: z.number().refine((v): v is SellPct => (SELL_PCTS as readonly number[]).includes(v), 'pct must be one of 15, 25, 33, 50, 100'), positionId: z.string().min(1).max(128).optional() }).strict();
export const NonceInput = z.object({
  action: z.enum(['create', 'topup', 'advance', 'check']),
  count: z.number().int().min(1).max(32).optional(),
  pubkey: z.string().refine(isPubkey, 'Not a valid public key').optional(),
}).strict().refine((n) => n.action !== 'advance' || !!n.pubkey, { message: 'pubkey is required to advance a nonce', path: ['pubkey'] });
export const ConfigPut = z.object({ baseVersion: z.number().int().min(1), config: z.unknown() }).strict();

export function zodFields(e: z.ZodError): FieldErrors {
  const out: FieldErrors = {};
  for (const i of e.issues) out[i.path.join('.') || '_'] ??= i.message;
  return out;
}
export function parse<T>(schema: z.ZodType<T>, body: unknown): T {
  const r = schema.safeParse(body ?? {});
  if (!r.success) throw new HttpError(400, 'Validation failed', { fields: zodFields(r.error) });
  return r.data;
}

type Breaker = 'hourly_burn' | 'hot_wallet_ceiling' | 'engine_disconnected';

export class ControlPlane extends EventEmitter {
  private rt: EngineRuntime | null = null;
  private savedPaperPositions: Position[] = [];
  private savedSession: EngineRuntime['session'] | null = null;
  private liveChain = { slot: 0, shredLatencyMs: 0, nonces: { ready: 0, total: 0 }, ring: { used: 0, size: 64 } };
  private liveBalances = { sol: 0, wsol: 0, usdc: 0, solUsd: 0 };
  private cfg: { version: number; config: BotConfig; updatedAt: number };
  private burnSamples: [number, number][] = [];
  private tripped = new Set<Breaker>();
  private lastEngineSlotAt = 0;
  private lastEngineBurstAt = 0;
  private launchSigs = new Map<string, string>();
  readonly startedAt = Date.now();

  constructor(private db: Db, private secrets: SecretStore, private engine: EngineAdapter, seed?: { config: BotConfig; kols: Kol[] }, private cloud?: CloudSync) {
    super();
    this.setMaxListeners(0);
    let latest = db.latestConfig();
    if (!latest) {
      db.insertConfig(1, seed?.config ?? structuredClone(DEFAULT_CONFIG), 'bootstrap', []);
      latest = db.latestConfig()!;
    }
    this.cfg = { version: latest.version, config: latest.config, updatedAt: latest.createdAt };
    db.clearPendingRestart();
    if (db.listKols().length === 0) {
      const initialKols = seed?.kols.length ? seed.kols : DEFAULT_KOLS;
      for (const k of initialKols) db.upsertKol(k);
    }
    engine.onEvent((e) => this.onEngine(e));
  }

  get engineSource(): EngineSource { return this.rt ? this.engine.source : 'disconnected'; }
  get engineStatus(): EngineRuntime['engine']['status'] { return this.rt?.engine.status ?? 'PAUSED'; }
  currentSlot(): number { return Math.max(this.rt?.chain.slot ?? 0, this.liveChain.slot ?? 0); }
  recordLaunchSig(mint: string, sig?: string) {
    if (!mint || !sig) return;
    this.launchSigs.set(mint, sig);
    if (this.launchSigs.size > 1000) {
      const first = this.launchSigs.keys().next().value;
      if (first) this.launchSigs.delete(first);
    }
  }

  /** Estimates real Solana tx + priority + SWQoS lander tip + duplicate-revert fee in SOL from current Settings. */
  estimateExecutionFeeSol(): number {
    const l = this.cfg.config.landers;
    const enabled = Object.entries(l.providers).filter(([, p]) => p.enabled && p.fanout > 0);
    const totalFanout = enabled.reduce((a, [, p]) => a + p.fanout, 0);
    const tipped = enabled.filter(([id]) => id !== 'tpu').map(([, p]) => (p.min_tip_sol + p.max_tip_sol) / 2);
    // Use configured SWQoS tip if enabled, otherwise realistic 0.002 SOL baseline tip for live-parity simulation.
    const landerTip = tipped.length ? Math.min(...tipped) : 0.002;
    const prioritySol = (l.tx.priority_fee_lamports || 0) / 1e9;
    const baseFeeSol = 0.000_005;
    const dupRevertsSol = Math.max(0, totalFanout - 1) * 0.000_006;
    return +(baseFeeSol + prioritySol + landerTip + dupRevertsSol).toFixed(6);
  }

  /** Updates persisted KOL win/loss/PnL stats when a Paper Mode KOL position closes. */
  private recordPaperKolOutcome(closed: ClosedPosition) {
    if (!closed.kolId) return;
    const kol = this.db.getKol(closed.kolId);
    if (!kol) return;
    const net = closed.proceedsSol - closed.entrySol;
    const updated: Kol = {
      ...kol,
      stats: {
        wins: kol.stats.wins + (net > 0 ? 1 : 0),
        losses: kol.stats.losses + (net <= 0 ? 1 : 0),
        realizedSol: +(kol.stats.realizedSol + net).toFixed(6),
        lastSeenSlot: this.rt?.chain.slot || this.liveChain.slot || kol.stats.lastSeenSlot,
      },
    };
    this.db.updateKolStats(updated);
    this.cloud?.syncKol(updated);
    this.publish({ type: 'kols_updated', ts: Date.now(), kols: this.db.listKols() });
  }

  /** Forwards a live mainnet KOL or launch trigger over the bridge to the Rust engine when RUNNING. */
  dispatchLiveTrigger(trigger: BridgeTriggerPayload) {
    if (this.engineStatus !== 'RUNNING' || this.engineSource !== 'bridge') return;
    void this.engine.command({ type: 'trigger.dispatch', trigger }).catch((e) => {
      console.warn(`[control] trigger.dispatch rejected for $${trigger.symbol ?? trigger.mint.slice(0, 6)}: ${(e as Error).message}`);
    });
  }

  /** Forwards a live 100% forced mirror-exit sell over the bridge when a watched KOL dumps a held token. */
  dispatchLiveMirrorSell(mint: string, kolLabel: string) {
    if (this.engineStatus !== 'RUNNING' || this.engineSource !== 'bridge') return;
    console.log(`[control] Mirroring KOL sell (${kolLabel}) -> forcing 100% live exit for ${mint.slice(0, 8)}...`);
    void this.engine.command({ type: 'sell', mint, pct: 100 }).catch((e) => {
      console.warn(`[control] mirror sell failed for ${mint.slice(0, 8)}: ${(e as Error).message}`);
    });
  }

  /** Updates Paper-mode positions & optional closed trade using real mainnet prices. */
  updatePaperState(positions: Position[], closed?: ClosedPosition, openedNew = false) {
    if (!this.rt) return;
    const realPositions = this.rt.positions.filter((p) => !p.id.startsWith('paper_'));
    this.rt.positions = [...positions, ...realPositions];
    if (openedNew) {
      this.rt.session = { ...this.rt.session, fills: this.rt.session.fills + 1, bursts: this.rt.session.bursts + 1 };
    }
    if (closed) {
      this.rt.closed = [closed, ...this.rt.closed.filter((c) => c.id !== closed.id)].slice(0, 5000);
      this.db.insertClosed(closed);
      void this.resolveClosedLandingSlot(closed.id, closed.mint, closed.buySlot ?? null);
      this.cloud?.syncClosed(closed);
      const net = closed.proceedsSol - closed.entrySol;
      this.rt.session = { ...this.rt.session, realizedSol: +(this.rt.session.realizedSol + net).toFixed(6) };
      this.recordPaperKolOutcome(closed);
    }
    this.publish({ type: 'position_updated', ts: Date.now(), positions: this.rt.positions, closed });
    if (openedNew || closed) this.publish({ type: 'session_updated', ts: Date.now(), session: this.rt.session });
  }

  /** Push persisted config and KOL rules to a (re)connected engine. */
  async syncEngine() {
    await this.engine.command({ type: 'config.apply', version: this.cfg.version, config: this.cfg.config }).catch((e) => console.warn('[control] config sync failed:', e.message));
    this.db.clearPendingRestart();
    this.publish({ type: 'config_updated', ts: Date.now(), config: this.configState() });
    const rt = this.engine.runtime();
    const want = this.db.listKols();
    for (const k of rt?.kols ?? []) if (!want.some((w) => w.id === k.id)) await this.engine.command({ type: 'kol.remove', id: k.id }).catch(() => {});
    for (const k of want) await this.engine.command({ type: 'kol.upsert', kol: k }).catch(() => {});
    // Preserve existing paper positions & live status across bridge reconnects (including across disconnect -> reconnect).
    const prevPaperPositions = this.savedPaperPositions.length > 0
      ? this.savedPaperPositions
      : (this.rt?.positions ?? []).filter((p) => p.id.startsWith('paper_'));
    const prevSession = this.savedSession ?? this.rt?.session ?? null;
    // Auto-restore persisted engine mode (SHADOW or RUNNING) across restarts/redeploys so deploys never leave the bot stuck PAUSED.
    const savedMode = this.db.getMeta('engine_mode');
    const restoredMode = (savedMode === 'SHADOW' || savedMode === 'RUNNING') && rt?.engine.status === 'PAUSED' ? savedMode : null;
    if (restoredMode) {
      await this.engine.command({ type: 'control', status: restoredMode }).catch(() => {});
    }
    const freshRt = this.engine.runtime();
    if (freshRt) {
      if (restoredMode) {
        freshRt.engine = { status: restoredMode, changedAt: Date.now() };
      }
      if (prevPaperPositions.length > 0) {
        const liveOnly = freshRt.positions.filter((p) => !p.id.startsWith('paper_'));
        freshRt.positions = [...liveOnly, ...prevPaperPositions];
      }
      if (prevSession) {
        freshRt.session = prevSession;
      }
      if (this.liveBalances.sol > 0 || this.liveBalances.solUsd > 0) {
        freshRt.balances = { ...freshRt.balances, ...this.liveBalances };
      }
    }
    this.rt = freshRt;
    this.savedPaperPositions = [];
    this.savedSession = null;
    // Persist the engine's recent history (first run: simulator backfill).
    if (this.rt) {
      this.db.tx(() => {
        for (const e of this.rt!.feed) this.db.insertBurst(e, null);
        for (const c of this.rt!.closed) this.db.insertClosed(c);
        for (const s of this.rt!.sweeps) this.db.upsertSweep(s);
      });
    }
  }

  // ---------- engine events ----------
  private onEngine(e: AdapterEvent) {
    const rt = this.rt;
    if (e.type === 'connected') {
      const next = this.engine.runtime();
      if (next) {
        if (this.savedPaperPositions.length > 0) {
          next.positions = [...next.positions.filter((p) => !p.id.startsWith('paper_')), ...this.savedPaperPositions];
        }
        if (this.savedSession) next.session = this.savedSession;
        if (this.liveBalances.sol > 0 || this.liveBalances.solUsd > 0) {
          next.balances = { ...next.balances, ...this.liveBalances };
        }
      }
      this.rt = next;
      this.tripped.delete('engine_disconnected');
      return;
    }
    if (e.type === 'disconnected') {
      const papers = (this.rt?.positions ?? []).filter((p) => p.id.startsWith('paper_'));
      if (papers.length > 0) this.savedPaperPositions = papers;
      if (this.rt?.session) this.savedSession = this.rt.session;
      this.rt = null;
      this.trip('engine_disconnected', `Engine connection lost: ${e.reason}`);
      this.publish({ type: 'engine_status', ts: Date.now(), status: 'PAUSED', reason: 'Engine disconnected', source: 'disconnected' });
      return;
    }
    if (!rt) { if (e.type !== 'nonces') this.rt = this.engine.runtime(); if (!this.rt) return; }
    const r = this.rt!;
    switch (e.type) {
      case 'nonces': r.nonces = e.nonces; this.publishConfig(); return;
      case 'slot_tick': {
        const raw = e as unknown as { chain?: EngineRuntime['chain']; balances?: EngineRuntime['balances']; slot?: number; shredLatencyMs?: number };
        const nextSlot = raw.chain?.slot ?? (typeof raw.slot === 'number' ? raw.slot : 0);
        if (nextSlot > 0) this.lastEngineSlotAt = Date.now();
        if (raw.chain) {
          r.chain = { ...r.chain, ...raw.chain };
        } else if (nextSlot > 0) {
          r.chain = { ...r.chain, slot: nextSlot, shredLatencyMs: raw.shredLatencyMs ?? r.chain.shredLatencyMs };
        }
        if (raw.balances && (raw.balances.sol > 0 || raw.balances.solUsd > 0)) {
          r.balances = raw.balances;
          if (raw.balances.sol > this.cfg.config.wallets.max_hot_wallet_sol) {
            this.trip('hot_wallet_ceiling', `Hot wallet holds ${raw.balances.sol.toFixed(3)} SOL, above the ${this.cfg.config.wallets.max_hot_wallet_sol} SOL ceiling`);
          }
        }
        // Normalize outgoing UI telemetry so consumers always see valid chain + balances
        this.publish({ type: 'slot_tick', ts: e.ts, chain: r.chain, balances: r.balances });
        return;
      }
      case 'burst_dispatched':
        this.lastEngineBurstAt = Date.now();
        r.feed = [e.burst, ...r.feed.filter((x) => x.id !== e.burst.id)].slice(0, 200);
        this.db.insertBurst(e.burst, null);
        // Update live lander telemetry counters when a real fan-out burst fills over the bridge.
        if (e.burst.outcome === 'filled' && e.burst.fanout > 0 && r.landers.length) {
          const enabledLanders = r.landers.filter((l) => l.enabled);
          const perLander = Math.max(1, Math.round(e.burst.fanout / Math.max(1, enabledLanders.length)));
          r.landers = r.landers.map((l) => {
            if (!l.enabled && l.id !== e.burst.winner) return l;
            const won = l.id === e.burst.winner ? 1 : 0;
            return {
              ...l,
              sent: l.sent + perLander,
              wins: l.wins + won,
              slotDeltaSum: l.slotDeltaSum + (won ? 2 : 0),
            };
          });
          this.publish({ type: 'landers_updated', ts: e.ts, landers: r.landers });
        }
        break;
      case 'tx_landed':
        if (e.signature) this.db.raw.prepare('UPDATE bursts SET winning_sig = ? WHERE id = ?').run(e.signature, e.burstId);
        break;
      case 'position_updated': {
        const prevLive = r.positions.filter((p) => !p.id.startsWith('paper_'));
        const paperPositions = r.positions.filter((p) => p.id.startsWith('paper_'));
        const enriched = e.positions.map((p) => {
          const match = r.feed.find((f) => f.mint === p.mint && f.symbol && f.symbol !== 'TOKEN' && !p.mint.startsWith(f.symbol));
          const prev = prevLive.find((x) => x.mint === p.mint);
          // Accumulate realized SOL from on-chain partial sells (Instant TP1 + TWAP drips) so final closed PnL includes ALL sells!
          let proceedsSol = prev?.proceedsSol ?? p.proceedsSol ?? 0;
          if (prev && prev.tokens > p.tokens + 1e-3 && p.price > 0) {
            const soldTokens = prev.tokens - p.tokens;
            const partialProceeds = soldTokens * p.price * 0.99;
            proceedsSol = +(proceedsSol + partialProceeds).toFixed(6);
          }
          return {
            ...p,
            symbol: match?.symbol ?? prev?.symbol ?? p.symbol,
            image: p.image ?? prev?.image ?? match?.image,
            proceedsSol,
            realizedSol: proceedsSol,
          };
        });
        // Detect real live positions that just closed on-chain so Closed, Win Rate, Realized PnL & PnL Calendar update!
        const closedBatch: ClosedPosition[] = [];
        if (e.closed) {
          r.closed = [e.closed, ...r.closed].slice(0, 5000);
          this.db.insertClosed(e.closed);
          closedBatch.push(e.closed);
        } else {
          const nextMints = new Set(enriched.map((p) => p.mint));
          let liveNetDelta = 0;
          for (const prev of prevLive) {
            if (!nextMints.has(prev.mint)) {
              // Guard against duplicate close records for the same mint within 10s.
              if (r.closed.some((c) => !c.id.startsWith('cl_paper_') && c.mint === prev.mint && Date.now() - c.closedAt < 10_000)) {
                continue;
              }
              const proceedsSol = +(prev.tokens * prev.price + (prev.proceedsSol ?? 0)).toFixed(6);
              const cl: ClosedPosition = {
                id: `cl_live_${prev.mint.slice(0, 10)}_${Date.now().toString(36)}`,
                mint: prev.mint,
                symbol: prev.symbol,
                image: prev.image,
                venue: prev.venue,
                kolId: prev.kolId,
                openedAt: prev.openedAt,
                closedAt: Date.now(),
                entrySol: prev.entrySol,
                proceedsSol,
                solUsd: r.balances.solUsd || this.liveBalances.solUsd || 70,
                exitReason: 'Live exit',
              };
              r.closed = [cl, ...r.closed].slice(0, 5000);
              this.db.insertClosed(cl);
              void this.resolveClosedLandingSlot(cl.id, cl.mint);
              this.cloud?.syncClosed(cl);
              this.recordPaperKolOutcome(cl);
              liveNetDelta += cl.proceedsSol - cl.entrySol;
              closedBatch.push(cl);
            }
          }
          if (closedBatch.length > 0) {
            r.session = { ...r.session, realizedSol: +(r.session.realizedSol + liveNetDelta).toFixed(6) };
          }
        }
        r.positions = [...enriched, ...paperPositions];
        if (closedBatch.length <= 1) {
          this.publish({ ...e, positions: r.positions, closed: closedBatch[0] });
        } else {
          for (const cl of closedBatch) {
            this.publish({ ...e, positions: r.positions, closed: cl });
          }
        }
        if (closedBatch.length > 0) {
          this.publish({ type: 'session_updated', ts: Date.now(), session: r.session });
        }
        return;
      }
      case 'sweep_executed': this.db.upsertSweep(e.sweep); break;
      case 'treasury_updated':
        r.sweeps = e.sweeps; r.inventory = e.inventory; r.rent = e.rent;
        for (const s of e.sweeps.slice(0, 5)) this.db.upsertSweep(s);
        break;
      case 'engine_status': r.engine = { status: e.status, changedAt: e.ts, reason: e.reason }; break;
      case 'kols_updated': {
        r.kols = e.kols;
        for (const k of e.kols) if (this.db.getKol(k.id)) this.db.updateKolStats(k);
        this.publish({ type: 'kols_updated', ts: e.ts, kols: this.db.listKols() });
        return;
      }
      case 'landers_updated': {
        r.landers = e.landers;
        const burn = e.landers.reduce((a, l) => a + l.tipPaidSol + l.revertCostSol, 0);
        const now = Date.now();
        this.burnSamples.push([now, burn]);
        this.burnSamples = this.burnSamples.filter(([t]) => t >= now - 3_600_000);
        const hour = burn - this.burnSamples[0][1];
        const cap = this.cfg.config.landers.max_hourly_burn_sol;
        if (hour > cap) this.trip('hourly_burn', `Gas + tip burn ${hour.toFixed(4)} SOL in the last hour exceeds the ${cap} SOL cap`);
        break;
      }
      case 'session_updated': r.session = e.session; break;
    }
    this.publish(e as TelemetryEvent);
  }

  private trip(b: Breaker, reason: string) {
    if (this.tripped.has(b)) return;
    const st = this.rt?.engine.status;
    if (b !== 'engine_disconnected' && st !== 'RUNNING' && st !== 'SHADOW') return;
    this.tripped.add(b);
    this.db.audit({ actor: 'circuit-breaker', ip: null, action: 'breaker.trip', target: b, detail: { reason } });
    this.publish({ type: 'circuit_breaker_tripped', ts: Date.now(), breaker: b, reason });
    if (b !== 'engine_disconnected') this.engine.command({ type: 'control', status: 'PAUSED' }).catch((e) => console.error('[breaker] pause failed', e));
  }

  private publish(e: TelemetryEvent) { this.emit('telemetry', e); }
  private publishConfig() { this.publish({ type: 'config_updated', ts: Date.now(), config: this.configState() }); }

  /** Background non-blocking resolver: records exact real on-chain buySlot + slotDelta on closed live & paper trades. */
  private async resolveClosedLandingSlot(closedId: string, mint: string, knownBuySlot?: number | null) {
    const wallet = this.cfg.config.wallets.hot_wallet_pubkey;
    const rpcUrl = this.cfg.config.network.rpc_primary_url || 'https://api.mainnet-beta.solana.com';
    if (!mint) return;
    const isPaper = typeof knownBuySlot === 'number' && knownBuySlot > 0;
    if (!isPaper && !wallet) return;
    const rpc = async <T>(method: string, params: unknown[]): Promise<T | null> => {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 8_000);
      try {
        const res = await fetch(rpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: ctrl.signal,
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
        });
        if (!res.ok) return null;
        const j = (await res.json()) as { result?: T };
        return j.result ?? null;
      } catch {
        return null;
      } finally {
        clearTimeout(t);
      }
    };
    try {
      let buySlot: number | null = isPaper ? knownBuySlot! : null;
      if (buySlot === null) {
        // 1. Live mode: find earliest confirmed BUY tx for `mint` in our wallet's recent 150 signatures
        // (21-tx Slot-0 sprays push confirmed buys past limit=20).
        const wsigs = await rpc<Array<{ signature: string; slot: number; err: unknown }>>('getSignaturesForAddress', [wallet, { limit: 150 }]);
        if (!wsigs?.length) return;
        let checked = 0;
        for (const s of wsigs) {
          if (s.err !== null) continue;
          if (++checked > 30) break;
          const tx = await rpc<{
            slot?: number;
            meta?: {
              err?: unknown;
              preTokenBalances?: Array<{ owner?: string; mint?: string; uiTokenAmount?: { uiAmount?: number | null } }>;
              postTokenBalances?: Array<{ owner?: string; mint?: string; uiTokenAmount?: { uiAmount?: number | null } }>;
            };
          }>('getTransaction', [s.signature, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1 }]);
          if (!tx?.meta || tx.meta.err !== null) continue;
          const pre = (tx.meta.preTokenBalances ?? []).find((b) => b.owner === wallet && b.mint === mint)?.uiTokenAmount?.uiAmount ?? 0;
          const post = (tx.meta.postTokenBalances ?? []).find((b) => b.owner === wallet && b.mint === mint)?.uiTokenAmount?.uiAmount ?? 0;
          if (post > pre && typeof tx.slot === 'number') {
            buySlot = tx.slot;
            break;
          }
        }
      }
      if (buySlot === null) return;

      // 2. Find exact on-chain launch slot for `mint`.
      // Fast path (1 tiny RPC call, immune to high tx volume): if we captured the exact create signature, query getSignatureStatuses!
      let launchSlot: number | null = null;
      let exactLaunch = false;
      const launchSig = this.launchSigs.get(mint);
      if (launchSig) {
        const st = await rpc<{ value?: Array<{ slot?: number } | null> }>('getSignatureStatuses', [
          [launchSig],
          { searchTransactionHistory: true },
        ]);
        const s0 = st?.value?.[0]?.slot;
        if (typeof s0 === 'number' && s0 > 0) {
          launchSlot = s0;
          exactLaunch = true;
        }
      }
      // Fallback: walk up to 5 pages of 1000 sigs (5,000 txs) with 1 retry on transient RPC 429/timeout
      if (!exactLaunch) {
        let before: string | undefined;
        for (let page = 0; page < 5; page++) {
          const params: Record<string, unknown> = { limit: 1000 };
          if (before) params.before = before;
          let msigs = await rpc<Array<{ signature: string; slot: number }>>('getSignaturesForAddress', [mint, params]);
          if (!msigs) {
            await new Promise((res) => setTimeout(res, 600));
            msigs = await rpc<Array<{ signature: string; slot: number }>>('getSignaturesForAddress', [mint, params]);
          }
          if (!msigs?.length) break;
          launchSlot = msigs[msigs.length - 1].slot;
          if (msigs.length < 1000) {
            exactLaunch = true;
            break;
          }
          before = msigs[msigs.length - 1].signature;
        }
      }
      const finalBuySlot = exactLaunch && launchSlot !== null && isPaper ? Math.max(buySlot, launchSlot) : buySlot;
      const slotDelta = exactLaunch && launchSlot !== null && finalBuySlot >= launchSlot ? finalBuySlot - launchSlot : null;
      this.db.updateClosedSlot(closedId, finalBuySlot, slotDelta);
      if (this.rt) {
        const idx = this.rt.closed.findIndex((c) => c.id === closedId);
        if (idx >= 0) {
          const updated = { ...this.rt.closed[idx], buySlot: finalBuySlot, slotDelta };
          this.rt.closed[idx] = updated;
          this.publish({ type: 'position_updated', ts: Date.now(), positions: this.rt.positions, closed: updated });
        }
      }
    } catch { /* non-fatal background telemetry */ }
  }

  // ---------- reads ----------
  private needRt(): EngineRuntime {
    if (!this.rt) throw new HttpError(503, 'Engine not connected');
    return this.rt;
  }
  /** Accepts real read-only mainnet telemetry whenever the Rust engine is offline or has no active ShredStream/Geyser feed yet. */
  ingestExternalEvent(e: TelemetryEvent, winningSig: string | null = null) {
    const now = Date.now();
    if (e.type === 'slot_tick') {
      this.liveBalances = e.balances;
      if (this.rt) {
        this.rt.balances = {
          ...this.rt.balances,
          sol: e.balances.sol,
          wsol: e.balances.wsol,
          usdc: e.balances.usdc,
          solUsd: e.balances.solUsd,
        };
      }
      if (this.rt && now - this.lastEngineSlotAt < 5_000) return;
      this.liveChain = e.chain;
      if (this.rt) {
        this.rt.chain = { ...this.rt.chain, slot: e.chain.slot, shredLatencyMs: e.chain.shredLatencyMs };
      }
    } else if (e.type === 'burst_dispatched') {
      if (this.rt && now - this.lastEngineBurstAt < 10_000) return;
      this.db.insertBurst(e.burst, winningSig);
    }
    this.publish(e);
  }

  private emptyRuntime(): EngineRuntime {
    return {
      engine: { status: 'PAUSED', changedAt: this.startedAt, reason: 'Waiting for engine connection' },
      chain: this.liveChain.slot > 0 ? this.liveChain : { slot: 0, shredLatencyMs: 0, nonces: { ready: 0, total: this.cfg.config.wallets.nonce_accounts.length }, ring: { used: 0, size: 64 } },
      balances: this.liveBalances,
      feed: [],
      positions: [],
      closed: [],
      kols: [],
      landers: [],
      sweeps: [],
      inventory: [],
      rent: { accountsClosed: 0, reclaimedSol: 0, openSeededAccounts: 0, rentPerAccountSol: 0.00203928 },
      session: { startedAt: this.startedAt, bursts: 0, fills: 0, txSent: 0, realizedSol: 0 },
      nonces: [],
    };
  }
  configState(): ConfigState {
    return {
      version: this.cfg.version, config: this.cfg.config, secrets: this.secrets.status(), pendingRestart: this.db.pendingRestart(),
      updatedAt: this.cfg.updatedAt, nonces: this.rt?.nonces ?? ([] as NonceHealth[]),
    };
  }
  kols() { return this.db.listKols(); }
  positions() { const r = this.rt ?? this.emptyRuntime(); return { open: r.positions, closed: this.db.listClosed(200) }; }
  treasury() {
    const r = this.rt ?? this.emptyRuntime();
    return { balances: r.balances, inventory: r.inventory, rent: r.rent, sweeps: this.db.listSweeps(200) };
  }
  nonces() { return { pool: this.rt?.nonces ?? [], target: this.cfg.config.wallets.nonce_target_pool_size, accounts: this.cfg.config.wallets.nonce_accounts }; }
  private landersSnapshot(r: EngineRuntime): LanderStat[] {
    const byId = new Map(r.landers.map((l) => [l.id, l]));
    return (Object.entries(this.cfg.config.landers.providers) as [LanderId, { enabled: boolean }][]).map(([id, p]) => {
      const live = byId.get(id);
      if (live) return { ...live, enabled: p.enabled };
      return { id, name: LANDER_LABEL[id], enabled: p.enabled, sent: 0, wins: 0, slotDeltaSum: 0, tipPaidSol: 0, revertCostSol: 0 };
    });
  }
  snapshot(): StateSnapshot {
    const r = this.rt ?? this.emptyRuntime();
    const feed = this.db.listBursts({ limit: 200, kinds: ['kol_buy', 'launch', 'sell'] }).items.map(({ winningSig: _w, ...f }) => f);
    return {
      source: 'live', engineSource: this.engineSource, engine: r.engine, chain: r.chain, balances: r.balances, feed, positions: r.positions,
      closed: this.db.listClosed(5000), kols: this.db.listKols(), landers: this.landersSnapshot(r), sweeps: this.db.listSweeps(60), inventory: r.inventory,
      rent: r.rent, session: r.session, config: this.configState(),
    };
  }

  // ---------- writes (all audited) ----------
  async putConfig(baseVersion: number, input: unknown, a: Actor) {
    if (baseVersion !== this.cfg.version) throw new HttpError(409, `The config changed since you opened it (now v${this.cfg.version}). Reload to get the latest.`, { currentVersion: this.cfg.version });
    const v = validateConfig(input);
    if (!v.ok) throw new HttpError(400, 'Validation failed', { fields: v.errors });
    const changes = diffConfig(this.cfg.config, v.config);
    if (changes.length === 0) return { version: this.cfg.version, appliedLive: [], pendingRestart: this.db.pendingRestart() };
    const version = this.cfg.version + 1;
    if (this.rt) {
      try { await this.engine.command({ type: 'config.apply', version, config: v.config }); }
      catch (e) {
        const err = e as EngineError & { fields?: FieldErrors };
        throw new HttpError(err.fields ? 400 : 502, `Engine rejected the config: ${err.message}`, err.fields ? { fields: err.fields } : {});
      }
    }
    this.db.tx(() => {
      this.db.insertConfig(version, v.config, a.actor, changes);
      this.db.addPendingRestart(changes.filter((c) => c.restart).map((c) => c.path));
      this.db.audit({ ...a, action: 'config.update', target: `v${version}`, detail: { baseVersion, changes } });
    });
    this.cfg = { version, config: v.config, updatedAt: Date.now() };
    this.cloud?.syncConfig(version, v.config, a.actor);
    this.publishConfig();
    return { version, appliedLive: changes.filter((c) => !c.restart).map((c) => c.path), pendingRestart: this.db.pendingRestart() };
  }

  setSecret(id: string, value: string, a: Actor) {
    this.secrets.setFromApi(id, value);
    this.db.audit({ ...a, action: 'secret.set', target: id, detail: null }); // never the value
    this.publishConfig();
  }
  clearSecret(id: string, a: Actor) {
    this.secrets.clearFromApi(id);
    this.db.audit({ ...a, action: 'secret.clear', target: id, detail: null });
    this.publishConfig();
  }

  async createKol(input: z.infer<typeof KolInput>, a: Actor) {
    if (this.db.listKols().some((k) => k.pubkey === input.pubkey)) throw new HttpError(409, 'That wallet is already on the watchlist');
    const kol: Kol = { id: `kol_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, ...input, stats: { wins: 0, losses: 0, realizedSol: 0, lastSeenSlot: null } };
    this.db.upsertKol(kol);
    this.cloud?.syncKol(kol);
    this.db.audit({ ...a, action: 'kol.create', target: kol.id, detail: input });
    await this.engine.command({ type: 'kol.upsert', kol }).catch((e) => console.warn('[control] engine kol sync:', e.message));
    this.publish({ type: 'kols_updated', ts: Date.now(), kols: this.db.listKols() });
    return kol;
  }
  async patchKol(id: string, patch: z.infer<typeof KolPatch>, a: Actor) {
    const cur = this.db.getKol(id);
    if (!cur) throw new HttpError(404, 'Unknown KOL');
    if (patch.pubkey && this.db.listKols().some((k) => k.pubkey === patch.pubkey && k.id !== id)) throw new HttpError(409, 'That wallet is already on the watchlist');
    const next: Kol = { ...cur, ...patch };
    this.db.upsertKol(next);
    this.cloud?.syncKol(next);
    this.db.audit({ ...a, action: 'kol.update', target: id, detail: patch });
    await this.engine.command({ type: 'kol.upsert', kol: next }).catch((e) => console.warn('[control] engine kol sync:', e.message));
    this.publish({ type: 'kols_updated', ts: Date.now(), kols: this.db.listKols() });
    return next;
  }
  async deleteKol(id: string, a: Actor) {
    if (!this.db.deleteKol(id)) throw new HttpError(404, 'Unknown KOL');
    this.cloud?.deleteKol(id);
    this.db.audit({ ...a, action: 'kol.delete', target: id, detail: null });
    await this.engine.command({ type: 'kol.remove', id }).catch((e) => console.warn('[control] engine kol sync:', e.message));
    this.publish({ type: 'kols_updated', ts: Date.now(), kols: this.db.listKols() });
  }

  async control(input: z.infer<typeof ControlInput>, a: Actor) {
    const st = this.needRt().engine.status;
    if (input.action === 'emergency_kill_switch') {
      const reason = input.reason || 'Manual kill from terminal';
      await this.engine.command({ type: 'kill', reason });
      this.db.audit({ ...a, action: 'engine.kill', target: null, detail: { reason } });
      return { status: 'KILLED' as const };
    }
    const status = ({ start: 'RUNNING', pause: 'PAUSED', shadow_mode: 'SHADOW' } as const)[input.action];
    if (st === 'KILLED' && status !== 'PAUSED') throw new HttpError(409, 'Kill switch engaged: re-arm to PAUSED first');
    if (status !== 'PAUSED' && this.tripped.size && !this.tripped.has('engine_disconnected')) {
      // Operator explicitly resumes after a breaker trip: clear the latch, keep an audit trail.
      this.db.audit({ ...a, action: 'breaker.reset', target: [...this.tripped].join(','), detail: null });
      this.tripped.clear();
    }
    await this.engine.command({ type: 'control', status });
    this.db.setMeta('engine_mode', status);
    this.cloud?.syncMeta('engine_mode', status);
    this.db.audit({ ...a, action: `engine.${input.action}`, target: null, detail: { from: st, to: status, reason: input.reason ?? null } });
    return { status };
  }

  async sell(mint: string, pct: SellPct, a: Actor, positionId?: string) {
    const r = this.needRt();
    const pos = (positionId ? r.positions.find((p) => p.id === positionId) : undefined)
      ?? r.positions.find((p) => p.mint === mint);
    if (!pos) throw new HttpError(404, 'No open position for that mint');
    if (pos.id.startsWith('paper_')) {
      const sellTokens = pos.tokens * (pct / 100);
      const sellFeeSol = this.estimateExecutionFeeSol();
      // Exact constant-product AMM sell impact (assuming ~35 SOL curve depth) + Pump.fun 1% protocol fee + SWQoS/priority fees
      const estCurveSol = Math.max(30, sellTokens * pos.price * 12);
      const estCurveTokens = estCurveSol / pos.price;
      const grossSolOut = (estCurveSol * sellTokens) / (estCurveTokens + sellTokens);
      const proceeds = Math.max(0, grossSolOut * 0.99 - sellFeeSol);
      const totalProceeds = (pos.proceedsSol ?? 0) + proceeds;
      let closed: ClosedPosition | undefined;
      if (pct >= 100 || pos.tokens - sellTokens <= 1e-6) {
        r.positions = r.positions.filter((p) => p.id !== pos.id);
        closed = {
          id: `cl_${pos.id}_${Date.now().toString(36)}`,
          mint: pos.mint,
          symbol: pos.symbol,
          image: pos.image,
          venue: pos.venue,
          kolId: pos.kolId,
          openedAt: pos.openedAt,
          closedAt: Date.now(),
          entrySol: pos.entrySol,
          proceedsSol: +totalProceeds.toFixed(6),
          solUsd: r.balances.solUsd || this.liveBalances.solUsd || 0,
          exitReason: 'Manual paper sell (100%)',
          buySlot: pos.openedSlot > 0 ? pos.openedSlot : null,
        };
        r.closed = [closed, ...r.closed].slice(0, 5000);
        this.db.insertClosed(closed);
        void this.resolveClosedLandingSlot(closed.id, closed.mint, closed.buySlot ?? null);
        this.cloud?.syncClosed(closed);
        const net = closed.proceedsSol - closed.entrySol;
        r.session = { ...r.session, realizedSol: +(r.session.realizedSol + net).toFixed(6) };
        this.recordPaperKolOutcome(closed);
      } else {
        const updated: Position = {
          ...pos,
          tokens: pos.tokens - sellTokens,
          proceedsSol: +totalProceeds.toFixed(6),
          realizedSol: +(pos.realizedSol + proceeds).toFixed(6),
        };
        r.positions = r.positions.map((p) => (p.id === pos.id ? updated : p));
      }
      this.publish({ type: 'position_updated', ts: Date.now(), positions: r.positions, closed });
      if (closed) this.publish({ type: 'session_updated', ts: Date.now(), session: r.session });
      this.db.audit({ ...a, action: 'position.sell', target: mint, detail: { pct, paper: true } });
      return { accepted: true, mint, pct };
    }
    try { await this.engine.command({ type: 'sell', mint, pct }); }
    catch (e) { throw new HttpError((e as EngineError).status ?? 502, (e as Error).message); }
    this.db.audit({ ...a, action: 'position.sell', target: mint, detail: { pct } });
    return { accepted: true, mint, pct };
  }

  async nonce(input: z.infer<typeof NonceInput>, a: Actor) {
    const result = await this.engine.command({ type: 'nonce', action: input.action, count: input.count, pubkey: input.pubkey })
      .catch((e: Error) => { throw new HttpError(422, e.message); });
    if (input.action === 'create' && this.rt) {
      // The engine created accounts: record them in the config as a new version.
      const accounts = this.rt.nonces.map((n) => n.pubkey);
      const next = structuredClone(this.cfg.config);
      next.wallets.nonce_accounts = accounts;
      if (diffConfig(this.cfg.config, next).length) await this.putConfig(this.cfg.version, next, { ...a, actor: `${a.actor} (nonce.create)` });
    }
    this.db.audit({ ...a, action: `nonce.${input.action}`, target: input.pubkey ?? null, detail: { count: input.count ?? null, result } });
    return result ?? { ok: true };
  }
}
