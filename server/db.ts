// SQLite persistence via node:sqlite (Node >= 22.13, no native addon).
// Tables are small and append-mostly; WAL mode keeps reads non-blocking while the engine writes.
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { BotConfig } from '../lib/config/schema';
import type { ClosedPosition, FeedEvent, Kol, Sweep } from '../lib/types';

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS config_versions (
  version INTEGER PRIMARY KEY, config TEXT NOT NULL, created_at INTEGER NOT NULL, actor TEXT NOT NULL, changes TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_restart (path TEXT PRIMARY KEY, since INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS kols (
  id TEXT PRIMARY KEY, label TEXT NOT NULL, pubkey TEXT NOT NULL UNIQUE, quote TEXT NOT NULL,
  trigger_margin REAL NOT NULL, buy_size_sol REAL NOT NULL, max_curve_reserve_sol REAL NOT NULL, enabled INTEGER NOT NULL,
  wins INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0, realized_sol REAL NOT NULL DEFAULT 0, last_seen_slot INTEGER,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bursts (
  id TEXT PRIMARY KEY, ts INTEGER NOT NULL, slot INTEGER NOT NULL, kind TEXT NOT NULL, kol_id TEXT, mint TEXT NOT NULL, symbol TEXT NOT NULL,
  venue TEXT NOT NULL, size_sol REAL NOT NULL, fanout INTEGER NOT NULL, winner TEXT, winning_sig TEXT, cu_used INTEGER NOT NULL,
  reverts TEXT NOT NULL, outcome TEXT NOT NULL, note TEXT
);
CREATE INDEX IF NOT EXISTS bursts_ts ON bursts(ts DESC, id DESC);
CREATE TABLE IF NOT EXISTS closed_positions (
  id TEXT PRIMARY KEY, mint TEXT NOT NULL, symbol TEXT NOT NULL, venue TEXT NOT NULL, kol_id TEXT, opened_at INTEGER NOT NULL,
  closed_at INTEGER NOT NULL, entry_sol REAL NOT NULL, proceeds_sol REAL NOT NULL, sol_usd REAL NOT NULL, exit_reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS closed_at ON closed_positions(closed_at DESC);
CREATE TABLE IF NOT EXISTS sweeps (
  id TEXT PRIMARY KEY, ts INTEGER NOT NULL, slot INTEGER NOT NULL, wsol_in REAL NOT NULL, usdc_out REAL NOT NULL, route TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, actor TEXT NOT NULL, ip TEXT, action TEXT NOT NULL, target TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS audit_ts ON audit(id DESC);
CREATE TABLE IF NOT EXISTS revoked_tokens (jti TEXT PRIMARY KEY, exp INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS wallets (
  id TEXT PRIMARY KEY, label TEXT NOT NULL, pubkey TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, created_at INTEGER NOT NULL
);
`;

type Row = Record<string, unknown>;
const json = <T,>(v: unknown) => JSON.parse(String(v)) as T;

export type AuditEntry = { id: number; ts: number; actor: string; ip: string | null; action: string; target: string | null; detail: unknown };
export type SavedWallet = { id: string; label: string; pubkey: string; kind: 'generated' | 'imported' | 'watch'; createdAt: number };

export class Db {
  readonly raw: DatabaseSync;
  constructor(file: string) {
    if (file !== ':memory:') mkdirSync(path.dirname(file), { recursive: true });
    this.raw = new DatabaseSync(file);
    this.raw.exec(SCHEMA);
    // One-time idempotent fixes for pre-0aacb42 CATBAL row and 1s duplicate FUMCHI glitch row.
    this.raw.exec("UPDATE closed_positions SET proceeds_sol = 0.3374 WHERE id = 'cl_live_GZvvGyyjJo_mwvks6r8' AND proceeds_sol < 0.15;");
    this.raw.exec("DELETE FROM closed_positions WHERE symbol = 'FUMCHI' AND (closed_at - opened_at) < 3000;");
    for (const col of ['buy_slot INTEGER', 'slot_delta INTEGER']) {
      try { this.raw.exec(`ALTER TABLE closed_positions ADD COLUMN ${col};`); } catch { /* already exists */ }
    }
  }
  close() { this.raw.close(); }
  tx<T>(fn: () => T): T {
    this.raw.exec('BEGIN');
    try { const r = fn(); this.raw.exec('COMMIT'); return r; } catch (e) { this.raw.exec('ROLLBACK'); throw e; }
  }

  // ---------- config ----------
  latestConfig(): { version: number; config: BotConfig; createdAt: number } | null {
    const r = this.raw.prepare('SELECT version, config, created_at FROM config_versions ORDER BY version DESC LIMIT 1').get() as Row | undefined;
    return r ? { version: Number(r.version), config: json<BotConfig>(r.config), createdAt: Number(r.created_at) } : null;
  }
  insertConfig(version: number, config: BotConfig, actor: string, changes: unknown) {
    this.raw.prepare('INSERT INTO config_versions (version, config, created_at, actor, changes) VALUES (?, ?, ?, ?, ?)')
      .run(version, JSON.stringify(config), Date.now(), actor, JSON.stringify(changes));
  }
  pendingRestart(): string[] { return (this.raw.prepare('SELECT path FROM pending_restart ORDER BY since').all() as Row[]).map((r) => String(r.path)); }
  addPendingRestart(paths: string[]) { const s = this.raw.prepare('INSERT OR IGNORE INTO pending_restart (path, since) VALUES (?, ?)'); for (const p of paths) s.run(p, Date.now()); }
  clearPendingRestart() { this.raw.exec('DELETE FROM pending_restart'); }

  // ---------- kols ----------
  listKols(): Kol[] {
    return (this.raw.prepare('SELECT * FROM kols ORDER BY created_at').all() as Row[]).map((r) => ({
      id: String(r.id), label: String(r.label), pubkey: String(r.pubkey), quote: r.quote as Kol['quote'], triggerMargin: Number(r.trigger_margin),
      buySizeSol: Number(r.buy_size_sol), maxCurveReserveSol: Number(r.max_curve_reserve_sol), enabled: !!r.enabled,
      stats: { wins: Number(r.wins), losses: Number(r.losses), realizedSol: Number(r.realized_sol), lastSeenSlot: r.last_seen_slot == null ? null : Number(r.last_seen_slot) },
    }));
  }
  getKol(id: string) { return this.listKols().find((k) => k.id === id) ?? null; }
  upsertKol(k: Kol) {
    const now = Date.now();
    this.raw.prepare(`INSERT INTO kols (id, label, pubkey, quote, trigger_margin, buy_size_sol, max_curve_reserve_sol, enabled, wins, losses, realized_sol, last_seen_slot, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET label=excluded.label, pubkey=excluded.pubkey, quote=excluded.quote, trigger_margin=excluded.trigger_margin,
        buy_size_sol=excluded.buy_size_sol, max_curve_reserve_sol=excluded.max_curve_reserve_sol, enabled=excluded.enabled, updated_at=excluded.updated_at`)
      .run(k.id, k.label, k.pubkey, k.quote, k.triggerMargin, k.buySizeSol, k.maxCurveReserveSol, k.enabled ? 1 : 0,
        k.stats.wins, k.stats.losses, k.stats.realizedSol, k.stats.lastSeenSlot, now, now);
  }
  updateKolStats(k: Kol) {
    this.raw.prepare('UPDATE kols SET wins=?, losses=?, realized_sol=?, last_seen_slot=? WHERE id=?')
      .run(k.stats.wins, k.stats.losses, k.stats.realizedSol, k.stats.lastSeenSlot, k.id);
  }
  deleteKol(id: string) { return Number(this.raw.prepare('DELETE FROM kols WHERE id=?').run(id).changes) > 0; }

  // ---------- bursts ----------
  private burstWrites = 0;
  insertBurst(e: FeedEvent, winningSig: string | null) {
    this.ensureBurstImageCol();
    this.raw.prepare(`INSERT INTO bursts (id, ts, slot, kind, kol_id, mint, symbol, image, venue, size_sol, fanout, winner, winning_sig, cu_used, reverts, outcome, note)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET image = COALESCE(excluded.image, bursts.image)`)
      .run(e.id, e.ts, e.slot, e.kind, e.kolId, e.mint, e.symbol, e.image ?? null, e.venue, e.sizeSol, e.fanout, e.winner, winningSig, e.cuUsed, JSON.stringify(e.reverts), e.outcome, e.note ?? null);
    // Keep only the latest 200 live signals so SQLite never bloats or slows down the API.
    if (++this.burstWrites % 25 === 0) {
      this.raw.exec(`DELETE FROM bursts WHERE id NOT IN (SELECT id FROM bursts ORDER BY ts DESC, id DESC LIMIT 200)`);
    }
  }
  private burstImageColReady = false;
  private ensureBurstImageCol() {
    if (this.burstImageColReady) return;
    try { this.raw.exec('ALTER TABLE bursts ADD COLUMN image TEXT'); } catch { /* already exists */ }
    this.burstImageColReady = true;
  }
  listBursts(q: { limit: number; cursor?: { ts: number; id: string }; outcome?: string; kolId?: string; kinds: string[] }) {
    this.ensureBurstImageCol();
    const where: string[] = [`kind IN (${q.kinds.map(() => '?').join(',')})`];
    const args: (string | number)[] = [...q.kinds];
    if (q.outcome) { where.push('outcome = ?'); args.push(q.outcome); }
    if (q.kolId) { where.push('kol_id = ?'); args.push(q.kolId); }
    if (q.cursor) { where.push('(ts < ? OR (ts = ? AND id < ?))'); args.push(q.cursor.ts, q.cursor.ts, q.cursor.id); }
    const rows = this.raw.prepare(`SELECT * FROM bursts WHERE ${where.join(' AND ')} ORDER BY ts DESC, id DESC LIMIT ?`).all(...args, q.limit + 1) as Row[];
    const items = rows.slice(0, q.limit).map((r) => ({
      id: String(r.id), ts: Number(r.ts), slot: Number(r.slot), kind: r.kind as FeedEvent['kind'], kolId: (r.kol_id as string) ?? null, mint: String(r.mint),
      symbol: String(r.symbol), image: (r.image as string) ?? undefined, venue: r.venue as FeedEvent['venue'], sizeSol: Number(r.size_sol), fanout: Number(r.fanout), winner: (r.winner as FeedEvent['winner']) ?? null,
      winningSig: (r.winning_sig as string) ?? null, cuUsed: Number(r.cu_used), reverts: json<FeedEvent['reverts']>(r.reverts), outcome: r.outcome as FeedEvent['outcome'], note: (r.note as string) ?? undefined,
    }));
    const last = items[items.length - 1];
    return { items, nextCursor: rows.length > q.limit && last ? Buffer.from(JSON.stringify({ ts: last.ts, id: last.id })).toString('base64url') : null };
  }

  // ---------- positions / sweeps ----------
  insertClosed(c: ClosedPosition) {
    this.raw.prepare(`INSERT OR IGNORE INTO closed_positions (id, mint, symbol, venue, kol_id, opened_at, closed_at, entry_sol, proceeds_sol, sol_usd, exit_reason, buy_slot, slot_delta)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      c.id, c.mint, c.symbol, c.venue, c.kolId, c.openedAt, c.closedAt, c.entrySol, c.proceedsSol, c.solUsd, c.exitReason,
      c.buySlot ?? null, c.slotDelta ?? null,
    );
  }
  updateClosedSlot(id: string, buySlot: number, slotDelta: number | null) {
    this.raw.prepare('UPDATE closed_positions SET buy_slot = ?, slot_delta = ? WHERE id = ?').run(buySlot, slotDelta, id);
  }
  listClosed(limit: number): ClosedPosition[] {
    return (this.raw.prepare('SELECT * FROM closed_positions ORDER BY closed_at DESC LIMIT ?').all(limit) as Row[]).map((r) => ({
      id: String(r.id), mint: String(r.mint), symbol: String(r.symbol), venue: r.venue as ClosedPosition['venue'], kolId: (r.kol_id as string) ?? null,
      openedAt: Number(r.opened_at), closedAt: Number(r.closed_at), entrySol: Number(r.entry_sol), proceedsSol: Number(r.proceeds_sol), solUsd: Number(r.sol_usd), exitReason: String(r.exit_reason),
      buySlot: r.buy_slot != null ? Number(r.buy_slot) : null,
      slotDelta: r.slot_delta != null ? Number(r.slot_delta) : null,
    }));
  }
  upsertSweep(s: Sweep) {
    this.raw.prepare(`INSERT INTO sweeps (id, ts, slot, wsol_in, usdc_out, route, status) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET status=excluded.status`).run(s.id, s.ts, s.slot, s.wsolIn, s.usdcOut, s.route, s.status);
  }
  listSweeps(limit: number): Sweep[] {
    return (this.raw.prepare('SELECT * FROM sweeps ORDER BY ts DESC LIMIT ?').all(limit) as Row[]).map((r) => ({
      id: String(r.id), ts: Number(r.ts), slot: Number(r.slot), wsolIn: Number(r.wsol_in), usdcOut: Number(r.usdc_out), route: String(r.route), status: r.status as Sweep['status'],
    }));
  }

  // ---------- audit ----------
  audit(e: Omit<AuditEntry, 'id' | 'ts'>) {
    this.raw.prepare('INSERT INTO audit (ts, actor, ip, action, target, detail) VALUES (?, ?, ?, ?, ?, ?)')
      .run(Date.now(), e.actor, e.ip, e.action, e.target, e.detail == null ? null : JSON.stringify(e.detail));
  }
  listAudit(limit: number, beforeId?: number): AuditEntry[] {
    const rows = (beforeId
      ? this.raw.prepare('SELECT * FROM audit WHERE id < ? ORDER BY id DESC LIMIT ?').all(beforeId, limit)
      : this.raw.prepare('SELECT * FROM audit ORDER BY id DESC LIMIT ?').all(limit)) as Row[];
    return rows.map((r) => ({ id: Number(r.id), ts: Number(r.ts), actor: String(r.actor), ip: (r.ip as string) ?? null, action: String(r.action), target: (r.target as string) ?? null, detail: r.detail == null ? null : json(r.detail) }));
  }

  // ---------- token revocation ----------
  revoke(jti: string, exp: number) { this.raw.prepare('INSERT OR IGNORE INTO revoked_tokens (jti, exp) VALUES (?, ?)').run(jti, exp); }
  isRevoked(jti: string) { return !!this.raw.prepare('SELECT 1 FROM revoked_tokens WHERE jti=?').get(jti); }
  pruneRevoked() { this.raw.prepare('DELETE FROM revoked_tokens WHERE exp < ?').run(Math.floor(Date.now() / 1000)); }

  // ---------- key/value meta ----------
  getMeta(key: string): string | null {
    const r = this.raw.prepare('SELECT value FROM meta WHERE key = ?').get(key) as Row | undefined;
    return r ? String(r.value) : null;
  }
  setMeta(key: string, value: string) {
    this.raw.prepare('INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value').run(key, value);
  }

  // ---------- custom wallets ----------
  listWallets(): SavedWallet[] {
    return (this.raw.prepare('SELECT * FROM wallets ORDER BY created_at DESC').all() as Row[]).map((r) => ({
      id: String(r.id),
      label: String(r.label),
      pubkey: String(r.pubkey),
      kind: r.kind as SavedWallet['kind'],
      createdAt: Number(r.created_at),
    }));
  }
  insertWallet(w: SavedWallet) {
    this.raw.prepare('INSERT INTO wallets (id, label, pubkey, kind, created_at) VALUES (?, ?, ?, ?, ?)')
      .run(w.id, w.label, w.pubkey, w.kind, w.createdAt);
  }
  deleteWallet(id: string): boolean {
    return Number(this.raw.prepare('DELETE FROM wallets WHERE id = ?').run(id).changes) > 0;
  }
}
