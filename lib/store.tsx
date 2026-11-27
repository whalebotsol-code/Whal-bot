'use client';

// Terminal state + transport: REST commands + /ws/telemetry against the control plane (NEXT_PUBLIC_WHAL_API_URL).
import { createContext, useCallback, useContext, useEffect, useMemo, useReducer, useRef, useState, type ReactNode } from 'react';
import type { StateSnapshot, TelemetryEvent, WsServerControl } from '@/lib/telemetry';
import type { ClientCommand, TerminalState } from '@/lib/types';

export const API_URL = (process.env.NEXT_PUBLIC_WHAL_API_URL || 'http://127.0.0.1:8787').replace(/\/$/, '');
const TOKEN_KEY = 'whal.jwt';

const initial: TerminalState = {
  connection: 'connecting',
  source: 'live',
  engineSource: undefined,
  lastMessageAt: null,
  engine: { status: 'PAUSED', changedAt: 0 },
  chain: { slot: 0, shredLatencyMs: 0, nonces: { ready: 0, total: 0 }, ring: { used: 0, size: 64 } },
  balances: { sol: 0, wsol: 0, usdc: 0, solUsd: 0 },
  feed: [], positions: [], closed: [], kols: [], landers: [], sweeps: [], inventory: [],
  rent: { accountsClosed: 0, reclaimedSol: 0, openSeededAccounts: 0, rentPerAccountSol: 0 },
  session: { startedAt: 0, bursts: 0, fills: 0, txSent: 0, realizedSol: 0 },
  config: null,
};

type Action =
  | { type: 'conn'; connection: TerminalState['connection'] }
  | { type: 'snapshot'; snap: StateSnapshot }
  | { type: 'event'; e: TelemetryEvent }
  | { type: 'reset' };

const upsert = <T extends { id: string }>(xs: T[], x: T, max = 200) => [x, ...xs.filter((y) => y.id !== x.id)].slice(0, max);

function reducer(s: TerminalState, a: Action): TerminalState {
  if (a.type === 'conn') return { ...s, connection: a.connection };
  if (a.type === 'reset') return initial;
  const t = { ...s, lastMessageAt: Date.now() };
  if (a.type === 'snapshot') return { ...t, ...a.snap, source: 'live', engineSource: a.snap.engineSource, connection: 'open' };
  const e = a.e;
  switch (e.type) {
    case 'slot_tick': return { ...t, chain: e.chain, balances: e.balances };
    case 'burst_dispatched': return { ...t, feed: upsert(s.feed, e.burst) };
    case 'position_updated': return { ...t, positions: e.positions, closed: e.closed ? upsert(s.closed, e.closed, 5000) : s.closed };
    case 'sweep_executed': return { ...t, sweeps: upsert(s.sweeps, e.sweep, 200) };
    case 'treasury_updated': return { ...t, sweeps: e.sweeps, inventory: e.inventory, rent: e.rent };
    case 'engine_status': return { ...t, engine: { status: e.status, changedAt: e.ts, reason: e.reason }, engineSource: e.source };
    case 'kols_updated': return { ...t, kols: e.kols };
    case 'landers_updated': return { ...t, landers: e.landers };
    case 'session_updated': return { ...t, session: e.session };
    case 'config_updated': return { ...t, config: e.config };
    default: return t;
  }
}

// ---------- toasts ----------
export interface Toast { id: number; text: string; tone: 'info' | 'ok' | 'error' }

interface Ctx {
  state: TerminalState;
  send: <R = unknown>(cmd: ClientCommand) => Promise<R>;
  toasts: Toast[];
  toast: (text: string, tone?: Toast['tone']) => void;
  dismiss: (id: number) => void;
  simulateDrop: () => void;
  auth: { required: boolean; signedIn: boolean; login: (operatorToken: string) => Promise<void>; logout: () => Promise<void> };
}
const TerminalCtx = createContext<Ctx | null>(null);

type ApiError = Error & { status?: number; fields?: Record<string, string> };

export function TerminalProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, initial);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [token, setToken] = useState<string | null>(null);
  const [tokenLoaded, setTokenLoaded] = useState(false);
  const stateRef = useRef(state);
  stateRef.current = state;
  const tokenRef = useRef(token);
  tokenRef.current = token;

  const toast = useCallback((text: string, tone: Toast['tone'] = 'info') => {
    const id = Date.now() + Math.random();
    setToasts((x) => [...x.slice(-3), { id, text, tone }]);
    setTimeout(() => setToasts((x) => x.filter((t) => t.id !== id)), 4200);
  }, []);
  const dismiss = useCallback((id: number) => setToasts((x) => x.filter((t) => t.id !== id)), []);

  // Session token lives in sessionStorage: survives reloads, gone when the tab closes.
  useEffect(() => {
    try { setToken(sessionStorage.getItem(TOKEN_KEY)); } catch { /* storage unavailable */ }
    setTokenLoaded(true);
  }, []);
  const saveToken = useCallback((t: string | null) => {
    try { if (t) sessionStorage.setItem(TOKEN_KEY, t); else sessionStorage.removeItem(TOKEN_KEY); } catch { /* ignore */ }
    setToken(t);
  }, []);

  const api = useCallback(async <R,>(method: string, path: string, body?: unknown): Promise<R> => {
    const t = tokenRef.current;
    let r: Response;
    try {
      r = await fetch(API_URL + path, {
        method,
        headers: { ...(t ? { Authorization: `Bearer ${t}` } : {}), ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch { throw Object.assign(new Error(`Can't reach the control plane at ${API_URL}`), { status: 0 }) as ApiError; }
    const data = r.status === 204 ? null : await r.json().catch(() => null);
    if (r.status === 401 && path !== '/api/auth/login') { saveToken(null); dispatch({ type: 'reset' }); }
    if (!r.ok) throw Object.assign(new Error(data?.error ?? `Request failed (${r.status})`), { status: r.status, fields: data?.fields }) as ApiError;
    return data as R;
  }, [saveToken]);

  // ---------- transport ----------
  const sock = useRef<{ close: () => void; drop: () => void } | null>(null);

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let pingTimer: ReturnType<typeof setInterval> | undefined;
    let bannerTimer: ReturnType<typeof setTimeout> | undefined;
    let retry = 0;
    let lastSlotUiAt = 0;

    if (!token) return;
    let ws: WebSocket | null = null;
    const clearTimers = () => {
      clearTimeout(timer);
      clearInterval(pingTimer);
      clearTimeout(bannerTimer);
    };
    const open = async () => {
      try {
        dispatch({ type: 'snapshot', snap: await api<StateSnapshot>('GET', '/api/state') });
        clearTimeout(bannerTimer);
      } catch (e) {
        const err = e as ApiError;
        if (!alive || err.status === 401) return;
        dispatch({ type: 'conn', connection: 'reconnecting' });
        timer = setTimeout(open, Math.min(10_000, 1000 * 2 ** retry++));
        return;
      }
      if (!alive) return;
      ws = new WebSocket(API_URL.replace(/^http/, 'ws') + '/ws/telemetry');
      ws.onopen = () => {
        ws?.send(JSON.stringify({ type: 'auth', token }));
        clearInterval(pingTimer);
        // Client-side JSON keepalive every 25s so Railway's edge proxy never drops idle sockets
        pingTimer = setInterval(() => {
          if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }));
        }, 25_000);
      };
      ws.onmessage = (m) => {
        const e = JSON.parse(String(m.data)) as TelemetryEvent | WsServerControl;
        if (e.type === 'ready') { retry = 0; clearTimeout(bannerTimer); return; }
        if (e.type === 'error' || e.type === 'pong') return;
        // Throttle routine slot_tick UI updates to 1/sec (trades, signals & prices remain 0ms instant)
        if (e.type === 'slot_tick') {
          const now = Date.now();
          if (now - lastSlotUiAt < 1_000) return;
          lastSlotUiAt = now;
        }
        if (e.type === 'circuit_breaker_tripped') toast(`Circuit breaker: ${e.reason}`, 'error');
        dispatch({ type: 'event', e });
      };
      ws.onclose = (ev) => {
        clearInterval(pingTimer);
        if (!alive) return;
        if (ev.code === 4401) { saveToken(null); dispatch({ type: 'reset' }); return; }
        // Wait 3.5s before flashing the red banner so seamless proxy/tab reconnects stay silent
        clearTimeout(bannerTimer);
        bannerTimer = setTimeout(() => {
          if (alive) dispatch({ type: 'conn', connection: 'reconnecting' });
        }, 3_500);
        const delay = retry === 0 ? 300 : Math.min(10_000, 1000 * 2 ** retry);
        retry++;
        timer = setTimeout(open, delay);
      };
    };
    open();
    sock.current = { close: () => ws?.close(), drop: () => ws?.close(4000, 'simulated drop') };
    return () => { alive = false; clearTimers(); ws?.close(1000); };
  }, [token, api, saveToken, toast]);

  // ---------- commands ----------
  const send = useCallback(async (cmd: ClientCommand): Promise<unknown> => {
    const s = stateRef.current;
    switch (cmd.type) {
      case 'engine.set': return api('POST', '/api/engine/control', { action: { RUNNING: 'start', PAUSED: 'pause', SHADOW: 'shadow_mode' }[cmd.status] });
      case 'engine.kill': return api('POST', '/api/engine/control', { action: 'emergency_kill_switch', reason: cmd.reason });
      case 'position.sell': {
        const p = s.positions.find((x) => x.id === cmd.positionId);
        if (!p) throw new Error('Position no longer open');
        return api('POST', `/api/positions/${encodeURIComponent(p.mint)}/sell`, { pct: cmd.pct, positionId: cmd.positionId });
      }
      case 'kol.upsert': {
        const { id, stats: _stats, ...body } = cmd.kol;
        return s.kols.some((k) => k.id === id) ? api('PATCH', `/api/kols/${encodeURIComponent(id)}`, body) : api('POST', '/api/kols', body);
      }
      case 'kol.toggle': return api('PATCH', `/api/kols/${encodeURIComponent(cmd.id)}`, { enabled: cmd.enabled });
      case 'kol.remove': return api('DELETE', `/api/kols/${encodeURIComponent(cmd.id)}`);
      case 'config.update': return api('PUT', '/api/config', { baseVersion: cmd.baseVersion, config: cmd.config });
      case 'secret.set': return api('PUT', `/api/secrets/${encodeURIComponent(cmd.id)}`, { value: cmd.value });
      case 'secret.clear': return api('DELETE', `/api/secrets/${encodeURIComponent(cmd.id)}`);
      case 'nonce.create': return api('POST', '/api/nonces', { action: 'create', count: cmd.count });
      case 'nonce.topup': return api('POST', '/api/nonces', { action: 'topup' });
      case 'nonce.check': return api('POST', '/api/nonces', { action: 'check' });
      case 'nonce.advance': return api('POST', '/api/nonces', { action: 'advance', pubkey: cmd.pubkey });
    }
  }, [api]) as Ctx['send'];

  const login = useCallback(async (operatorToken: string) => {
    const r = await api<{ token: string }>('POST', '/api/auth/login', { token: operatorToken });
    saveToken(r.token);
  }, [api, saveToken]);
  const logout = useCallback(async () => {
    await api('POST', '/api/auth/logout', {}).catch(() => {});
    saveToken(null);
    dispatch({ type: 'reset' });
  }, [api, saveToken]);

  const simulateDrop = useCallback(() => sock.current?.drop(), []);
  const auth = useMemo(() => ({ required: tokenLoaded && !token, signedIn: !!token, login, logout }), [tokenLoaded, token, login, logout]);

  const value = useMemo(() => ({ state, send, toasts, toast, dismiss, simulateDrop, auth }), [state, send, toasts, toast, dismiss, simulateDrop, auth]);
  return <TerminalCtx.Provider value={value}>{children}</TerminalCtx.Provider>;
}

export function useTerminal() {
  const c = useContext(TerminalCtx);
  if (!c) throw new Error('useTerminal must be used inside <TerminalProvider>');
  return c;
}

/** Run a command with toast feedback; returns true on success. */
export function useCommand() {
  const { send, toast } = useTerminal();
  const [busy, setBusy] = useState(false);
  const run = useCallback(async (cmd: ClientCommand, okText?: string) => {
    setBusy(true);
    try { await send(cmd); if (okText) toast(okText, 'ok'); return true; }
    catch (e) { toast((e as Error).message, 'error'); return false; }
    finally { setBusy(false); }
  }, [send, toast]);
  return { run, busy };
}

export const kolLabel = (s: TerminalState, id: string | null) => (id ? s.kols.find((k) => k.id === id)?.label ?? 'Removed KOL' : 'Launch');
export const portfolioUsd = (s: TerminalState) =>
  (s.balances.sol + s.balances.wsol) * s.balances.solUsd + s.balances.usdc +
  s.inventory.reduce((a, i) => a + i.amount * i.priceUsd, 0) +
  s.positions.filter((p) => !p.id.startsWith('paper_')).reduce((a, p) => a + p.tokens * p.price, 0) * s.balances.solUsd;
