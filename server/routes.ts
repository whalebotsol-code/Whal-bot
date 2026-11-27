// REST endpoints. Every mutating route is authenticated, rate limited and audited (in ControlPlane).
import { timingSafeEqual } from 'node:crypto';
import { z } from 'zod';
import { toJsonSchema } from '../lib/config/schema';
import { ConfigPut, ControlInput, KolInput, KolPatch, NonceInput, SellInput, parse, type ControlPlane } from './control';
import type { Db } from './db';
import type { Env } from './env';
import { HttpError, type Ctx, type Route } from './http';
import { buildObserverReport } from './observer';
import { signJwt, verifyOperatorToken } from './security';

const LoginInput = z.object({ token: z.string().min(16, 'Operator token is too short').max(512) }).strict();
const SecretInput = z.object({ value: z.string().min(1).max(16_384) }).strict();
const actor = (c: Ctx) => ({ actor: c.actor, ip: c.ip });

export function routes(env: Env, db: Db, cp: ControlPlane): Route[] {
  return [
    // ---------- auth ----------
    { method: 'POST', path: '/api/auth/login', auth: false, limit: 'auth', handler: (c) => {
      const { token } = parse(LoginInput, c.body);
      const isDevKey = !!env.devApiKey && (() => {
        const a = Buffer.from(token), b = Buffer.from(env.devApiKey);
        return a.length === b.length && timingSafeEqual(a, b);
      })();
      if (!isDevKey && !verifyOperatorToken(token, env.operatorTokenHash)) {
        db.audit({ actor: 'anonymous', ip: c.ip, action: 'auth.login_failed', target: null, detail: null });
        throw new HttpError(401, 'Invalid operator token');
      }
      const sub = isDevKey ? 'dev-api-key' : 'operator';
      const { token: jwt, claims } = signJwt(env.jwtSecret, sub, env.jwtTtlSec);
      db.audit({ actor: sub, ip: c.ip, action: 'auth.login', target: claims.jti, detail: null });
      return { token: jwt, expiresAt: claims.exp * 1000 };
    } },
    { method: 'POST', path: '/api/auth/logout', auth: true, limit: 'write', handler: (c) => {
      db.revoke(c.claims!.jti, c.claims!.exp);
      db.audit({ ...actor(c), action: 'auth.logout', target: c.claims!.jti, detail: null });
      return { ok: true };
    } },
    { method: 'GET', path: '/api/health', auth: false, limit: 'read', handler: () => ({ ok: true, engineSource: cp.engineSource, uptimeSec: Math.round((Date.now() - cp.startedAt) / 1000) }) },

    // ---------- aggregate ----------
    { method: 'GET', path: '/api/state', auth: true, limit: 'read', handler: () => cp.snapshot() },
    { method: 'GET', path: '/api/observer/report', auth: true, limit: 'read', handler: () => buildObserverReport(cp, db) },

    // ---------- config ----------
    { method: 'GET', path: '/api/config', auth: true, limit: 'read', handler: () => cp.configState() },
    { method: 'GET', path: '/api/config/schema', auth: true, limit: 'read', handler: () => toJsonSchema() },
    { method: 'PUT', path: '/api/config', auth: true, limit: 'write', handler: (c) => {
      const body = parse(ConfigPut, c.body);
      return cp.putConfig(body.baseVersion, body.config, actor(c));
    } },
    { method: 'PUT', path: '/api/secrets/:id', auth: true, limit: 'write', handler: (c) => {
      const { value } = parse(SecretInput, c.body);
      cp.setSecret(c.params.id, value, actor(c));
      return { id: c.params.id, ...cp.configState().secrets[c.params.id] };
    } },
    { method: 'DELETE', path: '/api/secrets/:id', auth: true, limit: 'write', handler: (c) => { cp.clearSecret(c.params.id, actor(c)); return { ok: true }; } },

    // ---------- KOLs ----------
    { method: 'GET', path: '/api/kols', auth: true, limit: 'read', handler: () => ({ items: cp.kols() }) },
    { method: 'POST', path: '/api/kols', auth: true, limit: 'write', handler: async (c) => { c.res.statusCode = 201; return cp.createKol(parse(KolInput, c.body), actor(c)); } },
    { method: 'PATCH', path: '/api/kols/:id', auth: true, limit: 'write', handler: (c) => cp.patchKol(c.params.id, parse(KolPatch, c.body), actor(c)) },
    { method: 'DELETE', path: '/api/kols/:id', auth: true, limit: 'write', handler: async (c) => { await cp.deleteKol(c.params.id, actor(c)); return { ok: true }; } },

    // ---------- engine ----------
    { method: 'POST', path: '/api/engine/control', auth: true, limit: 'write', handler: (c) => cp.control(parse(ControlInput, c.body), actor(c)) },

    // ---------- positions ----------
    { method: 'GET', path: '/api/positions', auth: true, limit: 'read', handler: () => cp.positions() },
    { method: 'POST', path: '/api/positions/:mint/sell', auth: true, limit: 'write', handler: (c) => { const s = parse(SellInput, c.body); return cp.sell(c.params.mint, s.pct, actor(c), s.positionId); } },

    // ---------- bursts ----------
    { method: 'GET', path: '/api/bursts', auth: true, limit: 'read', handler: (c) => {
      const limit = Math.min(200, Math.max(1, Number(c.query.get('limit') ?? 50) || 50));
      const raw = c.query.get('cursor');
      let cursor: { ts: number; id: string } | undefined;
      if (raw) { try { cursor = JSON.parse(Buffer.from(raw, 'base64url').toString()); } catch { throw new HttpError(400, 'Bad cursor'); } }
      const kind = c.query.get('kind') ?? 'buys';
      const kinds = kind === 'all' ? ['kol_buy', 'launch', 'sell'] : kind === 'sells' ? ['sell'] : ['kol_buy', 'launch'];
      const outcome = c.query.get('outcome') ?? undefined;
      if (outcome && !['filled', 'missed', 'expired', 'shadow', 'sold'].includes(outcome)) throw new HttpError(400, 'Unknown outcome filter');
      return db.listBursts({ limit, cursor, outcome, kolId: c.query.get('kol') ?? undefined, kinds });
    } },

    // ---------- treasury ----------
    { method: 'GET', path: '/api/treasury', auth: true, limit: 'read', handler: () => cp.treasury() },

    // ---------- nonces ----------
    { method: 'GET', path: '/api/nonces', auth: true, limit: 'read', handler: () => cp.nonces() },
    { method: 'POST', path: '/api/nonces', auth: true, limit: 'write', handler: (c) => cp.nonce(parse(NonceInput, c.body), actor(c)) },

    // ---------- audit ----------
    { method: 'GET', path: '/api/audit', auth: true, limit: 'read', handler: (c) => {
      const limit = Math.min(500, Math.max(1, Number(c.query.get('limit') ?? 100) || 100));
      const before = c.query.get('before');
      const items = db.listAudit(limit, before ? Number(before) : undefined);
      return { items, nextBefore: items.length === limit ? items[items.length - 1].id : null };
    } },
  ];
}
