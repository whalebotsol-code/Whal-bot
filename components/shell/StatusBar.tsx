'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { Bell, ChevronDown, Copy, Download, Eye, OctagonX, Pause, Play, Plus, Power, Search, Settings, Users, Wallet, Zap } from '@/components/icons';
import { Button, Dot, Modal, Skeleton, cx } from '@/components/ui';
import { usePhantom } from '@/lib/phantom';
import { computePaperEquitySol, usePaperBalance } from '@/lib/paper';
import { isPubkey } from '@/lib/pubkey';
import { portfolioUsd, useCommand, useTerminal } from '@/lib/store';
import { fmtAgo, fmtInt, fmtSlot, fmtSol, fmtUsd, shortKey } from '@/lib/format';
import type { EngineStatus } from '@/lib/types';

const ENGINE: Record<EngineStatus, { label: string; dot: 'ok' | 'warn' | 'info' | 'bad'; icon: typeof Play; tone: string }> = {
  RUNNING: { label: 'Running', dot: 'ok', icon: Play, tone: 'text-emerald-300' },
  PAUSED: { label: 'Paused', dot: 'warn', icon: Pause, tone: 'text-amber-300' },
  SHADOW: { label: 'Paper Mode', dot: 'info', icon: Eye, tone: 'text-sky-300' },
  KILLED: { label: 'Killed', dot: 'bad', icon: OctagonX, tone: 'text-rose-300' },
};

function SolanaLogo() {
  return (
    <svg viewBox="0 0 397.7 311.7" className="size-4 shrink-0" aria-hidden>
      <defs>
        <linearGradient id="sol-grad" x1="360.88" y1="351.46" x2="141.21" y2="-69.29" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#00FFA3" />
          <stop offset="1" stopColor="#DC1FFF" />
        </linearGradient>
      </defs>
      <path fill="url(#sol-grad)" d="M64.6 237.9c2.4-2.4 5.7-3.8 9.2-3.8h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1l62.7-62.7z" />
      <path fill="url(#sol-grad)" d="M64.6 3.8C67.1 1.4 70.4 0 73.8 0h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1L64.6 3.8z" />
      <path fill="url(#sol-grad)" d="M333.1 120.1c-2.4-2.4-5.7-3.8-9.2-3.8H6.5c-5.8 0-8.7 7-4.6 11.1l62.7 62.7c2.4 2.4 5.7 3.8 9.2 3.8h317.4c5.8 0 8.7-7 4.6-11.1l-62.7-62.7z" />
    </svg>
  );
}

/** Left-rail Engine status / Pause button (above Settings). */
export function EngineRailButton() {
  const { state } = useTerminal();
  const { run, busy } = useCommand();
  const [open, setOpen] = useState(false);
  const [rearm, setRearm] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const st = state.engine.status;
  const e = ENGINE[st];
  const Icon = e.icon;
  const engineOnline = state.engineSource === 'bridge';

  useEffect(() => {
    if (!open) return;
    const close = (ev: MouseEvent) => { if (!ref.current?.contains(ev.target as Node)) setOpen(false); };
    const esc = (ev: KeyboardEvent) => ev.key === 'Escape' && setOpen(false);
    document.addEventListener('mousedown', close); document.addEventListener('keydown', esc);
    return () => { document.removeEventListener('mousedown', close); document.removeEventListener('keydown', esc); };
  }, [open]);

  const set = async (status: Exclude<EngineStatus, 'KILLED'>) => {
    setOpen(false);
    await run({ type: 'engine.set', status }, `Engine ${status === 'SHADOW' ? 'in Paper Mode' : status.toLowerCase()}`);
  };

  return (
    <div className="relative" ref={ref}>
      <button
        type="button" aria-haspopup="menu" aria-expanded={open}
        aria-label={`Engine: ${e.label}`}
        disabled={busy || state.connection !== 'open' || !engineOnline}
        onClick={() => (st === 'KILLED' ? setRearm(true) : setOpen((o) => !o))}
        className={cx('glass-pill group relative grid size-11 place-items-center rounded-xl disabled:cursor-not-allowed disabled:opacity-50', e.tone)}
      >
        <Icon className="size-[1.15rem]" aria-hidden />
        <span className="absolute right-2 top-2"><Dot tone={e.dot} /></span>
        <span className="pointer-events-none absolute left-13 z-50 whitespace-nowrap rounded-lg border border-line2 bg-raised px-2 py-1 text-xs text-fg opacity-0 transition-opacity group-hover:opacity-100 group-focus-visible:opacity-100">
          Engine: {e.label}{!engineOnline ? ' (offline)' : ''}
        </span>
      </button>
      {open && (
        <div role="menu" className="absolute bottom-0 left-14 z-50 w-60 rounded-xl border border-line2 bg-panel p-1 shadow-2xl">
          {([
            ['RUNNING', Play, 'Run', 'Detect and send bursts'],
            ['PAUSED', Pause, 'Pause', 'Detect only, send nothing'],
            ['SHADOW', Eye, 'Paper Mode', 'Simulate decisions, send nothing'],
          ] as const).map(([v, ItemIcon, l, d]) => (
            <button key={v} role="menuitemradio" aria-checked={st === v} type="button" onClick={() => set(v)}
              className={cx('flex w-full items-start gap-2.5 rounded-lg px-2.5 py-2 text-left hover:bg-raised', st === v && 'bg-raised')}>
              <ItemIcon className="mt-0.5 size-4 text-fg2" aria-hidden />
              <span><span className="block text-sm font-medium text-fg">{l}</span><span className="block text-xs text-muted">{d}</span></span>
            </button>
          ))}
        </div>
      )}
      <Modal open={rearm} onClose={() => setRearm(false)} title="Re-arm the engine?">
        <p className="text-sm text-fg2">The kill switch is engaged{state.engine.reason ? <> ({state.engine.reason})</> : null}. Re-arming returns the engine to <b className="text-fg">PAUSED</b>, so nothing is sent until you choose Run.</p>
        <div className="mt-5 flex justify-end gap-2">
          <Button onClick={() => setRearm(false)}>Cancel</Button>
          <Button variant="primary" icon={Power} loading={busy} data-autofocus
            onClick={async () => { if (await run({ type: 'engine.set', status: 'PAUSED' }, 'Engine re-armed in PAUSED')) setRearm(false); }}>Re-arm to paused</Button>
        </div>
      </Modal>
    </div>
  );
}

/** Left-rail Emergency Kill Switch button (above Settings, also Shift+K). */
export function KillSwitchRailButton() {
  const { state } = useTerminal();
  const { run, busy } = useCommand();
  const [open, setOpen] = useState(false);
  const [reason, setReason] = useState('Manual kill from terminal');
  const killed = state.engine.status === 'KILLED';
  const engineOnline = state.engineSource === 'bridge';
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement;
      if (e.shiftKey && e.key.toLowerCase() === 'k' && !/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) && !killed && engineOnline) { e.preventDefault(); setOpen(true); }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [killed, engineOnline]);
  return (
    <>
      <button
        type="button"
        disabled={killed || state.connection !== 'open' || !engineOnline}
        onClick={() => setOpen(true)}
        aria-label="Emergency kill switch"
        className="glass-pill-danger group relative grid size-11 place-items-center rounded-xl text-rose-300 disabled:cursor-not-allowed disabled:opacity-45"
      >
        <OctagonX className="size-[1.15rem]" aria-hidden />
        <span className="pointer-events-none absolute left-13 z-50 whitespace-nowrap rounded-lg border border-line2 bg-raised px-2 py-1 text-xs text-fg opacity-0 transition-opacity group-hover:opacity-100 group-focus-visible:opacity-100">
          {killed ? 'Killed' : engineOnline ? 'Kill switch (Shift+K)' : 'Kill switch (engine offline)'}
        </span>
      </button>
      <Modal open={open} onClose={() => setOpen(false)} title="Engage emergency kill switch?" tone="danger">
        <ul className="mb-4 list-disc space-y-1 pl-5 text-sm text-fg2">
          <li>Stops all new detections from sending, on every lander, immediately.</li>
          <li>Open positions stay open. Panic-sell buttons keep working.</li>
          <li>Re-arming later returns the engine to PAUSED, never straight to RUNNING.</li>
        </ul>
        <label htmlFor="kill-reason" className="text-xs font-medium text-fg2">Reason (logged)</label>
        <input id="kill-reason" value={reason} onChange={(e) => setReason(e.target.value)} maxLength={120}
          className="mt-1.5 h-8.5 w-full rounded-[0.6rem] border border-line2 bg-card px-3 text-sm outline-none focus:border-rose-400" />
        <div className="mt-5 flex justify-end gap-2">
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button variant="dangerSolid" icon={OctagonX} loading={busy} data-autofocus
            onClick={async () => { if (await run({ type: 'engine.kill', reason: reason.trim() || 'Manual kill' }, 'Kill switch engaged')) setOpen(false); }}>
            Engage kill switch
          </Button>
        </div>
      </Modal>
    </>
  );
}

function usePopover() {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (ev: MouseEvent) => { if (!ref.current?.contains(ev.target as Node)) setOpen(false); };
    const esc = (ev: KeyboardEvent) => ev.key === 'Escape' && setOpen(false);
    document.addEventListener('mousedown', close); document.addEventListener('keydown', esc);
    return () => { document.removeEventListener('mousedown', close); document.removeEventListener('keydown', esc); };
  }, [open]);
  return { open, setOpen, ref };
}

const panel = 'absolute right-0 top-12 z-40 rounded-2xl border border-line2 bg-panel shadow-2xl';

function PhantomLogo() {
  return (
    <svg viewBox="0 0 128 128" className="size-4 shrink-0" aria-hidden>
      <rect width="128" height="128" rx="30" fill="#AB9FF2" />
      <path d="M98.5 64.5C98.5 85.4 81.9 99 61 99H36.6C31.5 99 28 94.3 29.2 89.3C33.4 72.1 45.7 39 67.5 39C85.6 39 98.5 50.2 98.5 64.5Z" fill="#FFFDF8" />
      <ellipse cx="64.5" cy="61" rx="4.5" ry="6.5" fill="#1C1917" />
      <ellipse cx="79.5" cy="61" rx="4.5" ry="6.5" fill="#1C1917" />
    </svg>
  );
}

function WalletPill() {
  const { state, toast } = useTerminal();
  const phantom = usePhantom();
  const [paperStartSol] = usePaperBalance();
  const { open, setOpen, ref } = usePopover();
  const [depositOpen, setDepositOpen] = useState(false);
  const b = state.balances, loading = state.connection === 'connecting';
  const isShadow = state.engine.status === 'SHADOW';
  const hotWallet = state.config?.config.wallets.hot_wallet_pubkey || phantom.publicKey || '';
  const paperEquitySol = computePaperEquitySol(state, paperStartSol);
  const displaySol = isShadow
    ? paperEquitySol
    : phantom.connected && phantom.solBalance != null && b.sol === 0
      ? phantom.solBalance
      : b.sol;
  const displayUsd = displaySol * b.solUsd;
  const active = isShadow || state.engineSource === 'bridge' || phantom.connected || !!hotWallet;

  const copyAddr = async (addr: string, label: string) => {
    if (!addr) return;
    try { await navigator.clipboard.writeText(addr); toast(`${label} copied`, 'ok'); }
    catch { toast('Copy failed', 'error'); }
  };

  return (
    <div className="relative" ref={ref}>
      <button type="button" aria-haspopup="dialog" aria-expanded={open} onClick={() => setOpen((o) => !o)}
        className="glass-pill inline-flex h-10 items-center gap-2.5 rounded-full px-4 text-[0.95rem] font-semibold">
        {isShadow ? (
          <span className="relative inline-flex items-center text-sky-400">
            <Eye className="size-4" aria-hidden />
            <span className="absolute -bottom-0.5 -right-0.5 size-2 rounded-full border border-panel bg-emerald-400" />
          </span>
        ) : phantom.connected ? (
          <span className="relative inline-flex items-center">
            <PhantomLogo />
            <span className="absolute -bottom-0.5 -right-0.5 size-2 rounded-full border border-panel bg-emerald-400" />
          </span>
        ) : (
          <Wallet className="size-4 text-fg2" aria-hidden />
        )}
        <span aria-hidden className="h-4 w-px bg-white/10" />
        <SolanaLogo />
        {loading ? <Skeleton className="h-4 w-20" /> : <span className="tabular-nums">{fmtSol(displaySol, 4)} <span className="hidden sm:inline">SOL</span></span>}
        <ChevronDown className="hidden size-4 text-muted sm:block" aria-hidden />
      </button>

      {open && (
        <div role="dialog" aria-label="Bot Wallet" className={cx(panel, 'w-[min(21.5rem,calc(100vw-1.5rem))] rounded-[1.35rem] border-white/10 bg-[#0c0d12] p-3.5')}>
          {/* Bot Wallet inner card */}
          <div className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-gradient-to-br from-[#141622] via-[#10111a] to-[#0d0e15] p-4">
            {/* Subtle Solana watermark on right */}
            <svg viewBox="0 0 397.7 311.7" aria-hidden className="pointer-events-none absolute -right-4 bottom-1 w-28 opacity-[0.06]">
              <path fill="#14F195" d="M64.6 237.9c2.4-2.4 5.7-3.8 9.2-3.8h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1l62.7-62.7z" />
              <path fill="#00C2FF" d="M64.6 3.8C67.1 1.4 70.4 0 73.8 0h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1L64.6 3.8z" />
              <path fill="#9945FF" d="M333.1 120.1c-2.4-2.4-5.7-3.8-9.2-3.8H6.5c-5.8 0-8.7 7-4.6 11.1l62.7 62.7c2.4 2.4 5.7 3.8 9.2 3.8h317.4c5.8 0 8.7-7 4.6-11.1l-62.7-62.7z" />
            </svg>

            <div className="relative flex items-start justify-between gap-2">
              <div className="flex items-center gap-3">
                <span className="grid size-10 shrink-0 place-items-center rounded-full border border-white/[0.08] bg-black/55">
                  <SolanaLogo />
                </span>
                <div className="min-w-0">
                  <div className="text-[0.95rem] font-semibold leading-tight text-fg">
                    {isShadow ? 'Paper Wallet' : 'Bot Wallet'}
                  </div>
                  <div className="mt-1 flex items-center gap-1.5 text-xs text-muted">
                    <span className="num truncate" title={isShadow ? 'Simulated Paper Wallet' : hotWallet || undefined}>
                      {isShadow ? 'Paper Simulator' : hotWallet ? shortKey(hotWallet, 4) : 'Not set'}
                    </span>
                    {!isShadow && hotWallet && (
                      <button
                        type="button"
                        onClick={() => void copyAddr(hotWallet, 'Bot wallet address')}
                        aria-label="Copy bot wallet address"
                        className="grid size-5.5 place-items-center rounded-full bg-white/[0.07] text-fg2 hover:bg-white/[0.14] hover:text-fg"
                      >
                        <Copy className="size-3" />
                      </button>
                    )}
                  </div>
                </div>
              </div>
              <span className={cx(
                'inline-flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-[0.72rem] font-semibold',
                active ? 'bg-emerald-500/15 text-emerald-400' : 'bg-white/[0.06] text-muted',
              )}>
                <span className={cx('size-1.5 rounded-full', active ? 'bg-emerald-400' : 'bg-muted')} />
                {active ? 'Active' : 'Idle'}
              </span>
            </div>

            <div className="relative mt-5">
              <div className="flex items-baseline gap-1.5 tabular-nums">
                <span className="text-[1.75rem] font-bold leading-none tracking-tight text-white">{fmtSol(displaySol, 4)}</span>
                <span className="text-sm font-medium text-muted">SOL</span>
              </div>
              <div className="mt-1.5 text-xs text-muted tabular-nums">
                ≈ {fmtUsd(displayUsd)} · {b.usdc.toFixed(2)} USDC
              </div>
            </div>
          </div>

          {/* Phantom connect row */}
          <div className="flex items-center justify-between px-1.5 py-3.5 text-sm">
            <span className="text-fg2">Phantom</span>
            {phantom.connected && phantom.publicKey ? (
              <div className="flex items-center gap-2 text-xs">
                <span className="num text-emerald-300" title={phantom.publicKey}>{shortKey(phantom.publicKey, 4)}</span>
                <button
                  type="button"
                  onClick={() => void phantom.disconnect().then(() => toast('Phantom disconnected', 'info'))}
                  className="text-muted underline underline-offset-4 hover:text-rose-300"
                >
                  Disconnect
                </button>
              </div>
            ) : phantom.installed ? (
              <button
                type="button"
                disabled={phantom.connecting}
                onClick={() => void phantom.connect().then((pk) => { if (pk) toast(`Connected Phantom (${shortKey(pk, 4)})`, 'ok'); })}
                className="font-medium text-[#8b8fff] underline decoration-[#8b8fff]/70 underline-offset-4 hover:text-indigo-300 disabled:opacity-60"
              >
                {phantom.connecting ? 'Connecting…' : 'Connect'}
              </button>
            ) : (
              <a
                href="https://phantom.app/"
                target="_blank"
                rel="noopener noreferrer"
                className="font-medium text-[#8b8fff] underline decoration-[#8b8fff]/70 underline-offset-4 hover:text-indigo-300"
              >
                Install
              </a>
            )}
          </div>
          {phantom.error && <p className="-mt-2 mb-2 px-1.5 text-[0.7rem] text-rose-300">{phantom.error}</p>}

          {/* Action pills */}
          <div className="flex flex-col gap-2.5">
            <button
              type="button"
              onClick={() => { setOpen(false); setDepositOpen(true); }}
              className="inline-flex h-10.5 w-full items-center justify-center gap-2 rounded-full bg-[#6c75f5] px-4 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff]"
            >
              <Download className="size-4" aria-hidden />
              Deposit
            </button>
            <Link
              href="/wallets"
              onClick={() => setOpen(false)}
              className="inline-flex h-10.5 w-full items-center justify-center gap-1 rounded-full bg-white px-4 text-sm font-semibold text-black transition-colors hover:bg-zinc-200"
            >
              Manage wallet <span aria-hidden className="text-base leading-none">›</span>
            </Link>
          </div>
        </div>
      )}

      <Modal open={depositOpen} onClose={() => setDepositOpen(false)} title="Deposit SOL to Bot Wallet">
        <p className="text-sm text-fg2">Send native SOL from Phantom or any Solana wallet to your hot trading wallet address below:</p>
        <div className="mt-3 flex items-center justify-between gap-2 rounded-xl border border-line2 bg-card px-3 py-2.5">
          {hotWallet ? (
            <code className="num break-all text-xs text-fg">{hotWallet}</code>
          ) : (
            <Link href="/settings#wallets" onClick={() => setDepositOpen(false)} className="text-xs font-medium text-accent hover:underline">
              Set your Hot wallet public key in Settings → Wallets &amp; Nonce Pool
            </Link>
          )}
          {hotWallet && (
            <Button size="sm" variant="ghost" icon={Copy} onClick={() => void copyAddr(hotWallet, 'Bot wallet address')}>Copy</Button>
          )}
        </div>
        <div className="mt-4 flex justify-end gap-2">
          <Button onClick={() => setDepositOpen(false)}>Close</Button>
        </div>
      </Modal>
    </div>
  );
}

function Notifications() {
  const { state } = useTerminal();
  const { open, setOpen, ref } = usePopover();
  const [seen, setSeen] = useState(0);
  const items = state.feed.filter((e) => e.kind === 'sell' || e.outcome === 'filled').slice(0, 12);
  const unseen = items.filter((e) => e.ts > seen).length;
  return (
    <div className="relative hidden sm:block" ref={ref}>
      <button type="button" aria-label={`Notifications${unseen ? `, ${unseen} new` : ''}`} aria-expanded={open}
        onClick={() => { setOpen((o) => !o); setSeen(Date.now()); }}
        className="glass-pill relative grid size-10 place-items-center rounded-full text-fg2 hover:text-fg">
        <Bell className="size-[1.1rem]" aria-hidden />
        {unseen > 0 && <span aria-hidden className="absolute right-2.5 top-2.5 size-1.5 rounded-full bg-accent" />}
      </button>
      {open && (
        <div role="dialog" aria-label="Notifications" className={cx(panel, 'max-h-[70vh] w-[22rem] overflow-auto')}>
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Notifications</div>
          {state.engine.status === 'KILLED' && <div className="border-b border-line bg-rose-500/10 px-4 py-2.5 text-sm text-rose-200">Kill switch engaged {fmtAgo(state.engine.changedAt)} ago{state.engine.reason ? `: ${state.engine.reason}` : ''}</div>}
          {items.length === 0 ? <p className="px-4 py-6 text-center text-sm text-muted">Nothing yet.</p> : items.map((e) => (
            <Link key={e.id} href={e.kind === 'sell' ? '/?tab=closed' : '/?tab=positions'} onClick={() => setOpen(false)} className="block border-b border-line px-4 py-2.5 text-sm last:border-0 hover:bg-raised">
              <span className={e.kind === 'sell' ? 'text-fg' : 'text-emerald-300'}>{e.kind === 'sell' ? `Sold $${e.symbol}` : `Filled $${e.symbol}`}</span>
              <span className="text-fg2"> · {e.kind === 'sell' ? `${fmtSol(e.sizeSol, 4)} SOL${e.note ? ` · ${e.note}` : ''}` : `${e.sizeSol} SOL via ${e.winner ?? '—'}`}</span>
              <div className="mt-0.5 text-xs text-muted">{fmtAgo(e.ts)} ago · slot #{fmtSlot(e.slot)}</div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

const QUICK_LINKS: { href: string; label: string; keywords: string }[] = [
  { href: '/', label: 'Overview', keywords: 'home overview positions closed signals pnl' },
  { href: '/watchlist', label: 'KOL watchlist', keywords: 'kol wallets watchlist copy' },
  { href: '/execution', label: 'Execution & landers', keywords: 'execution landers swqos jito nozomi astralane tpu' },
  { href: '/treasury', label: 'Treasury & inventory', keywords: 'treasury sweep usdc wsol rent inventory' },
  { href: '/wallets', label: 'Wallets', keywords: 'wallets phantom paper hot balance deposit' },
  { href: '/diagnostics', label: 'Bug & system diagnostics', keywords: 'diagnostics health audit bugs status' },
  { href: '/settings#network', label: 'Settings · Network & Ingestion', keywords: 'settings network rpc shredstream helius' },
  { href: '/settings#wallets', label: 'Settings · Wallets & Nonce Pool', keywords: 'settings wallets hot key nonce pool' },
  { href: '/settings#strategy', label: 'Settings · Strategy & Buy Guards', keywords: 'settings strategy buy guards slippage curve' },
  { href: '/settings#landers', label: 'Settings · Multi-Lander & Fees', keywords: 'settings landers fees tips jito nozomi astralane' },
  { href: '/settings#exits', label: 'Settings · Exit Ladder & Risk', keywords: 'settings exits stop loss take profit trailing' },
  { href: '/settings#treasury', label: 'Settings · Treasury Auto-Sweep', keywords: 'settings treasury auto sweep clip threshold' },
];

function GlobalSearch() {
  const { state } = useTerminal();
  const { open, setOpen, ref } = usePopover();
  const [q, setQ] = useState('');
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement;
      if (e.key === '/' && !/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) { e.preventDefault(); input.current?.focus(); }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, []);
  const s = q.trim().toLowerCase().replace(/^\$/, '');
  const kols = s ? state.kols.filter((k) => k.label.toLowerCase().includes(s) || k.pubkey.toLowerCase().includes(s)).slice(0, 5) : [];
  const tokens = s ? [...new Map([...state.positions, ...state.closed].filter((p) => p.symbol.toLowerCase().includes(s) || p.mint.toLowerCase() === s).map((p) => [p.mint, p])).values()].slice(0, 5) : [];
  const landers = s ? state.landers.filter((l) => l.name.toLowerCase().includes(s) || l.id.toLowerCase().includes(s)).slice(0, 4) : [];
  const sections = s ? QUICK_LINKS.filter((p) => p.label.toLowerCase().includes(s) || p.keywords.includes(s)).slice(0, 5) : [];
  const newWallet = isPubkey(q.trim()) && !state.kols.some((k) => k.pubkey === q.trim());
  const done = () => { setOpen(false); setQ(''); };
  const item = 'flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left text-sm hover:bg-raised';
  return (
    <div ref={ref} className="relative mx-auto w-full max-w-[26rem] min-w-0">
      <label className="flex h-9 items-center gap-2 rounded-[0.72rem] border border-line bg-card px-3 text-muted focus-within:border-line2">
        <Search className="size-3.5 shrink-0" aria-hidden />
        <input ref={input} value={q} onChange={(e) => { setQ(e.target.value); setOpen(true); }} onFocus={() => setOpen(true)}
          placeholder="Search KOLs, tokens, landers, settings" aria-label="Search KOLs, tokens, landers, settings" autoComplete="off" spellCheck={false}
          className="min-w-0 flex-1 bg-transparent text-[0.85rem] text-fg outline-none placeholder:text-muted" />
        <kbd className="hidden rounded-md border border-line2 px-1.5 text-[0.7rem] text-muted sm:block">/</kbd>
      </label>
      {open && s && (
        <div className={cx(panel, 'left-0 right-0 top-11 max-h-[60vh] overflow-auto p-1.5')}>
          {newWallet && <Link href={`/watchlist#add=${q.trim()}`} onClick={done} className={item}><Plus className="size-4 text-muted" aria-hidden />Track this wallet <span className="num text-xs text-muted">{shortKey(q.trim())}</span></Link>}
          {kols.length > 0 && <div className="px-3 pb-1 pt-2 text-[0.7rem] font-semibold uppercase tracking-wider text-muted">KOLs</div>}
          {kols.map((k) => <Link key={k.id} href={`/watchlist#${k.id}`} onClick={done} className={item}><Users className="size-4 text-muted" aria-hidden />{k.label}<span className="num ml-auto text-xs text-muted">{shortKey(k.pubkey)}</span></Link>)}
          {tokens.length > 0 && <div className="px-3 pb-1 pt-2 text-[0.7rem] font-semibold uppercase tracking-wider text-muted">Tokens</div>}
          {tokens.map((t) => <a key={t.mint} href={`https://solscan.io/token/${encodeURIComponent(t.mint)}`} target="_blank" rel="noopener noreferrer" onClick={done} className={item}><span className="font-semibold">${t.symbol}</span><span className="num ml-auto text-xs text-muted">{shortKey(t.mint)}</span></a>)}
          {landers.length > 0 && <div className="px-3 pb-1 pt-2 text-[0.7rem] font-semibold uppercase tracking-wider text-muted">Landers</div>}
          {landers.map((l) => <Link key={l.id} href="/execution" onClick={done} className={item}><Zap className="size-4 text-muted" aria-hidden />{l.name}<span className="ml-auto text-xs text-muted">{l.enabled ? 'Enabled' : 'Off'}</span></Link>)}
          {sections.length > 0 && <div className="px-3 pb-1 pt-2 text-[0.7rem] font-semibold uppercase tracking-wider text-muted">Sections &amp; Settings</div>}
          {sections.map((p) => <Link key={p.href} href={p.href} onClick={done} className={item}><Settings className="size-4 text-muted" aria-hidden />{p.label}</Link>)}
          {!newWallet && kols.length === 0 && tokens.length === 0 && landers.length === 0 && sections.length === 0 && <p className="px-3 py-3 text-sm text-muted">No matches for “{q.trim()}”.</p>}
        </div>
      )}
    </div>
  );
}

const SOURCE: Record<string, { label: string; tone: string; title: string }> = {
  bridge: { label: 'LIVE ENGINE', tone: 'text-emerald-400', title: 'Connected to the trading engine.' },
  disconnected: { label: 'ENGINE OFFLINE', tone: 'text-rose-400', title: 'Control plane is live; waiting for the Rust engine to connect.' },
};
function SourceBadge() {
  const { state } = useTerminal();
  if (state.engineSource === 'bridge') return null;
  const s = SOURCE[state.engineSource ?? 'disconnected'] ?? SOURCE.disconnected;
  return <span title={s.title} className={cx('hidden text-[0.72rem] font-semibold tracking-wide sm:inline-flex', s.tone)}>{s.label}</span>;
}

function AccountMenu() {
  const { auth } = useTerminal();
  const { open, setOpen, ref } = usePopover();
  return (
    <div className="relative hidden lg:block" ref={ref}>
      <button type="button" aria-label="Account" aria-expanded={open} onClick={() => setOpen((o) => !o)} className="glass-pill grid size-10 place-items-center rounded-full text-sm font-bold">W</button>
      {open && (
        <div role="menu" className={cx(panel, 'w-64 p-1.5')}>
          <div className="px-3 py-2 text-xs text-muted">Signed in as <span className="text-fg">operator</span><br />Control plane session</div>
          <button type="button" role="menuitem" onClick={() => { setOpen(false); auth.logout(); }} className="w-full rounded-lg px-3 py-2 text-left text-sm hover:bg-raised">Sign out</button>
        </div>
      )}
    </div>
  );
}

/** Top bar: brand · search · notifications · wallet · avatar. */
export function TopBar() {
  return (
    <header className="sticky top-0 z-30 border-b border-line bg-black/85 backdrop-blur-xl">
      <div className="flex h-16 items-center gap-3 px-3 md:gap-4 md:px-7">
        <Link href="/" className="flex shrink-0 items-center gap-2 text-[1.05rem]" aria-label="Whal Bot home">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src="/logo.png" alt="" className="size-8 rounded-lg md:hidden" />
          <span className="hidden md:inline"><b className="font-bold">Whal</b> <span className="text-fg2">Bot</span></span>
        </Link>
        <GlobalSearch />
        <div className="flex shrink-0 items-center gap-2.5">
          <Notifications />
          <WalletPill />
          <AccountMenu />
        </div>
      </div>
    </header>
  );
}

/** Bottom bar: chain health on the left, connection on the right. */
export function BottomBar() {
  const { state } = useTerminal();
  const c = state.chain, loading = state.connection === 'connecting';
  const hasSlot = c.slot > 0;
  const lat = c.shredLatencyMs;
  const latTone = !hasSlot ? 'text-muted' : lat > 60 ? 'text-rose-300' : lat > 30 ? 'text-amber-300' : 'text-emerald-300';
  const nonceTone = c.nonces.total === 0 ? 'text-muted' : c.nonces.ready === c.nonces.total ? 'text-emerald-300' : c.nonces.ready <= 2 ? 'text-rose-300' : 'text-amber-300';
  const ringPct = c.ring.size ? (c.ring.used / c.ring.size) * 100 : 0;
  const conn = state.connection;
  const item = 'flex shrink-0 items-center gap-1.5 whitespace-nowrap';
  return (
    <footer className="sticky bottom-15 z-20 flex h-9 items-center gap-4 border-t border-line bg-bg px-3 text-[0.8rem] text-fg2 md:bottom-0 md:px-7" aria-label="Chain and connection status">
      <div className="flex min-w-0 flex-1 items-center gap-5 overflow-x-auto">
        {loading ? <Skeleton className="h-3.5 w-80" /> : <>
          <span className={item}><b className="font-semibold text-fg">SOL</b><span className="tabular-nums">{state.balances.solUsd > 0 ? fmtUsd(state.balances.solUsd) : '—'}</span></span>
          <span className={item}><span className="text-muted">Slot</span><span className="num text-fg">{fmtSlot(c.slot)}</span></span>
          <Link href="/settings#network" className={cx(item, 'hover:text-fg')} title="Shred-to-detection latency — click to configure Network"><span className="text-muted">ShredStream</span><span className={cx('num', latTone)}>{hasSlot ? `${lat} ms` : '—'}</span></Link>
          <Link href="/settings#wallets" className={cx(item, 'hover:text-fg')} title="Durable nonce pool ready / total — click to manage Nonces"><span className="text-muted">Nonces</span><span className={cx('num', nonceTone)}>{c.nonces.ready}/{c.nonces.total}</span></Link>
          <span className={item} title="Dedupe ring buffer entries in use"><span className="text-muted">Ring buffer</span><span className="num text-fg">{c.ring.used}/{c.ring.size}</span>
            <span aria-hidden className="h-1 w-10 overflow-hidden rounded bg-line2"><span className={cx('block h-full', ringPct > 90 ? 'bg-amber-400' : 'bg-accent')} style={{ width: `${ringPct}%` }} /></span></span>
        </>}
      </div>
      <Link href="/diagnostics" className="flex shrink-0 items-center gap-3 hover:opacity-85" title="Open system diagnostics">
        <SourceBadge />
        <span className={item}>
          <Dot tone={conn === 'open' ? 'ok' : conn === 'connecting' ? 'warn' : 'bad'} />
          {conn === 'open' ? 'Connected' : conn === 'connecting' ? 'Connecting…' : 'Reconnecting…'}
        </span>
      </Link>
    </footer>
  );
}
