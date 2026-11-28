'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { ArrowUpRight, Check, Copy, Download, Eye, Lock, Pencil, Plus, Power, RefreshCw, Settings, Trash2, TriangleAlert, Wallet, Zap } from '@/components/icons';
import { Button, Modal, PageHeader, cx } from '@/components/ui';
import { fmtSol, fmtUsd, shortKey } from '@/lib/format';
import { computePaperEquitySol, usePaperBalance } from '@/lib/paper';
import { usePhantom } from '@/lib/phantom';
import { API_URL, useTerminal } from '@/lib/store';

const PAPER_PRESETS = [5, 10, 25, 100];

interface CustomWallet {
  id: string;
  label: string;
  pubkey: string;
  kind: 'generated' | 'imported' | 'watch';
  createdAt: number;
  sol: number;
}

interface GeneratedSecret {
  wallet: CustomWallet;
  secretKeyBase58: string;
  secretKeyArray: number[];
}

async function walletApi<T>(method: 'GET' | 'POST' | 'DELETE', path: string, body?: unknown): Promise<T> {
  const jwt = typeof window !== 'undefined' ? sessionStorage.getItem('whal.jwt') : null;
  const r = await fetch(`${API_URL}${path}`, {
    method,
    headers: {
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...(jwt ? { Authorization: `Bearer ${jwt}` } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error((data as { error?: string; message?: string }).error || (data as { message?: string }).message || `HTTP ${r.status}`);
  }
  return data as T;
}

function SolanaMark({ className = 'size-5' }: { className?: string }) {
  return (
    <svg viewBox="0 0 397.7 311.7" className={cx(className, 'shrink-0')} aria-hidden>
      <defs>
        <linearGradient id="wallets-sol-grad" x1="360.88" y1="351.46" x2="141.21" y2="-69.29" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#00FFA3" />
          <stop offset="1" stopColor="#DC1FFF" />
        </linearGradient>
      </defs>
      <path fill="url(#wallets-sol-grad)" d="M64.6 237.9c2.4-2.4 5.7-3.8 9.2-3.8h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1l62.7-62.7z" />
      <path fill="url(#wallets-sol-grad)" d="M64.6 3.8C67.1 1.4 70.4 0 73.8 0h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1L64.6 3.8z" />
      <path fill="url(#wallets-sol-grad)" d="M333.1 120.1c-2.4-2.4-5.7-3.8-9.2-3.8H6.5c-5.8 0-8.7 7-4.6 11.1l62.7 62.7c2.4 2.4 5.7 3.8 9.2 3.8h317.4c5.8 0 8.7-7 4.6-11.1l-62.7-62.7z" />
    </svg>
  );
}

function UsdcMark({ className = 'size-5' }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" className={cx(className, 'shrink-0')} aria-hidden>
      <circle cx="16" cy="16" r="16" fill="#2775CA" />
      <path
        fill="#FFFFFF"
        d="M20.022 18.124c0-2.124-1.28-2.852-3.84-3.156-1.828-.243-2.193-.728-2.193-1.578 0-.85.61-1.396 1.828-1.396 1.097 0 1.707.364 2.011 1.275a.458.458 0 0 0 .427.303h.975a.416.416 0 0 0 .427-.425v-.06a3.04 3.04 0 0 0-2.743-2.489V9.142c0-.243-.183-.425-.487-.486h-.915c-.243 0-.426.182-.487.486v1.396c-1.829.242-2.986 1.456-2.986 2.974 0 2.002 1.218 2.791 3.778 3.095 1.707.303 2.255.668 2.255 1.639 0 .97-.853 1.638-2.011 1.638-1.585 0-2.133-.667-2.316-1.578-.06-.242-.244-.364-.427-.364h-1.036a.416.416 0 0 0-.426.425v.06c.243 1.518 1.219 2.61 3.23 2.914v1.457c0 .242.183.425.487.485h.915c.243 0 .426-.182.487-.485V21.34c1.829-.303 3.047-1.578 3.047-3.217z"
      />
      <path
        fill="#FFFFFF"
        d="M12.892 24.497c-4.754-1.7-7.192-6.98-5.424-11.653.914-2.55 2.925-4.491 5.424-5.402.244-.121.365-.303.365-.607v-.85c0-.242-.121-.424-.365-.485-.061 0-.183 0-.244.06a10.895 10.895 0 0 0-7.13 13.717c1.096 3.4 3.717 6.01 7.13 7.102.244.121.488 0 .548-.243.061-.06.061-.122.061-.243v-.85c0-.182-.182-.424-.365-.546zm6.46-18.936c-.244-.122-.488 0-.548.242-.061.061-.061.122-.061.243v.85c0 .243.182.485.365.607 4.754 1.7 7.192 6.98 5.424 11.653-.914 2.55-2.925 4.491-5.424 5.402-.244.121-.365.303-.365.607v.85c0 .242.121.424.365.485.061 0 .183 0 .244-.06a10.895 10.895 0 0 0 7.13-13.717c-1.096-3.46-3.778-6.07-7.13-7.162z"
      />
    </svg>
  );
}

function WalletCard({
  title,
  badge,
  badgeTone = 'amber',
  activeHighlight = false,
  asset = 'sol',
  providerLabel,
  subtitleNote = 'on-chain',
  address,
  sol,
  usdc,
  solUsd,
  primaryAction,
  secondaryAction,
  onReceive,
  onCopy,
}: {
  title: string;
  badge: string;
  badgeTone?: 'amber' | 'emerald' | 'sky';
  activeHighlight?: boolean;
  asset?: 'sol' | 'usdc';
  providerLabel: string;
  subtitleNote?: string;
  address: string | null;
  sol: number;
  usdc: number;
  solUsd: number;
  primaryAction: React.ReactNode;
  secondaryAction?: React.ReactNode;
  onReceive?: () => void;
  onCopy: (addr: string) => void;
}) {
  const isUsdc = asset === 'usdc';
  const onChainUsd = isUsdc ? usdc : sol * solUsd + usdc;
  return (
    <div className={cx(
      'relative overflow-hidden rounded-[1.35rem] border bg-gradient-to-br from-[#141622] via-[#10111a] to-[#0c0d14] p-6 shadow-xl transition-colors',
      activeHighlight ? 'border-[#6c75f5]/60 shadow-[0_0_24px_rgba(108,117,245,0.18)]' : 'border-white/[0.08]',
    )}>
      {/* Subtle large watermark */}
      {isUsdc ? (
        <UsdcMark className="pointer-events-none absolute right-28 top-1/2 size-36 -translate-y-1/2 opacity-[0.05]" />
      ) : (
        <svg viewBox="0 0 397.7 311.7" aria-hidden className="pointer-events-none absolute right-28 top-1/2 w-40 -translate-y-1/2 opacity-[0.045]">
          <path fill="#14F195" d="M64.6 237.9c2.4-2.4 5.7-3.8 9.2-3.8h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1l62.7-62.7z" />
          <path fill="#00C2FF" d="M64.6 3.8C67.1 1.4 70.4 0 73.8 0h317.4c5.8 0 8.7 7 4.6 11.1l-62.7 62.7c-2.4 2.4-5.7 3.8-9.2 3.8H6.5c-5.8 0-8.7-7-4.6-11.1L64.6 3.8z" />
          <path fill="#9945FF" d="M333.1 120.1c-2.4-2.4-5.7-3.8-9.2-3.8H6.5c-5.8 0-8.7 7-4.6 11.1l62.7 62.7c2.4 2.4 5.7 3.8 9.2 3.8h317.4c5.8 0 8.7-7 4.6-11.1l-62.7-62.7z" />
        </svg>
      )}

      {/* Top row: Wallet identity + network pill */}
      <div className="relative flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-center gap-3.5">
          <span className="grid size-12 shrink-0 place-items-center rounded-full border border-white/[0.08] bg-black/55">
            {isUsdc ? <UsdcMark className="size-6" /> : <SolanaMark className="size-5.5" />}
          </span>
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <h2 className="text-[1.08rem] font-semibold tracking-tight text-white">{title}</h2>
              <span className={cx(
                'rounded-md border px-2 py-0.5 text-[0.68rem] font-semibold',
                badgeTone === 'amber'
                  ? 'border-amber-400/35 bg-amber-400/12 text-amber-300'
                  : badgeTone === 'sky'
                    ? 'border-sky-400/35 bg-sky-400/12 text-sky-300'
                    : 'border-emerald-400/35 bg-emerald-400/12 text-emerald-300',
              )}>
                {badge}
              </span>
            </div>
            <div className="mt-1 flex items-center gap-2 text-xs text-muted">
              <span className="num text-fg2" title={address ?? undefined}>{address ? shortKey(address, 4) : 'Not connected'}</span>
              {address && (
                <button
                  type="button"
                  onClick={() => onCopy(address)}
                  aria-label={`Copy ${title} address`}
                  className="grid size-6 place-items-center rounded-full bg-white/[0.07] text-fg2 hover:bg-white/[0.14] hover:text-fg"
                >
                  <Copy className="size-3" />
                </button>
              )}
              <span>· {providerLabel}</span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {secondaryAction}
          <span className="inline-flex items-center gap-2 rounded-full border border-white/[0.08] bg-black/45 px-3.5 py-1.5 text-xs font-medium text-fg">
            {isUsdc ? <UsdcMark className="size-3.5" /> : <SolanaMark className="size-3.5" />}
            {isUsdc ? 'USDC · Solana' : 'Solana'}
          </span>
        </div>
      </div>

      {/* Bottom row: Big balance + action pills */}
      <div className="relative mt-7 flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="flex flex-wrap items-baseline gap-2 tabular-nums">
            {isUsdc ? (
              <>
                <span className="text-[2.15rem] font-bold leading-none tracking-tight text-white">
                  {usdc.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </span>
                <span className="text-base font-medium text-fg2">USDC</span>
              </>
            ) : (
              <>
                <span className="text-[2.15rem] font-bold leading-none tracking-tight text-white">{fmtSol(sol, 4)}</span>
                <span className="text-base font-medium text-fg2">SOL · {usdc.toFixed(2)} USDC</span>
              </>
            )}
          </div>
          <div className="mt-1.5 text-xs text-muted tabular-nums">
            ≈ {fmtUsd(onChainUsd)} {subtitleNote}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2.5">
          {address && onReceive && (
            <button
              type="button"
              onClick={onReceive}
              className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 text-sm font-medium text-fg transition-colors hover:bg-white/[0.09]"
            >
              <Download className="size-4 text-fg2" aria-hidden />
              Receive
            </button>
          )}
          {address && (
            <a
              href={`https://solscan.io/account/${encodeURIComponent(address)}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 text-sm font-medium text-fg transition-colors hover:bg-white/[0.09]"
            >
              <ArrowUpRight className="size-4 text-fg2" aria-hidden />
              Explorer
            </a>
          )}
          {primaryAction}
        </div>
      </div>
    </div>
  );
}

export function WalletsView() {
  const { state, send, toast } = useTerminal();
  const phantom = usePhantom();
  const [paperStartSol, setPaperStartSol] = usePaperBalance();
  const [receiveAddr, setReceiveAddr] = useState<{ label: string; addr: string } | null>(null);
  const [paperModal, setPaperModal] = useState(false);
  const [paperInput, setPaperInput] = useState<string>(String(paperStartSol));

  // Multi-wallet management state
  const [customWallets, setCustomWallets] = useState<CustomWallet[]>([]);
  const [loadingWallets, setLoadingWallets] = useState(false);
  const [addModal, setAddModal] = useState(false);
  const [addLabel, setAddLabel] = useState('');
  const [addKeyInput, setAddKeyInput] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const [genModal, setGenModal] = useState(false);
  const [genLabel, setGenLabel] = useState('');
  const [generated, setGenerated] = useState<GeneratedSecret | null>(null);
  const [revealSecret, setRevealSecret] = useState(false);
  const [copiedSecret, setCopiedSecret] = useState(false);

  const [removeTarget, setRemoveTarget] = useState<CustomWallet | null>(null);
  const [hiddenBuiltins, setHiddenBuiltins] = useState<string[]>(() => {
    if (typeof window === 'undefined') return [];
    try { return JSON.parse(localStorage.getItem('whal.hiddenWallets') || '[]') as string[]; } catch { return []; }
  });
  const [hideBuiltinTarget, setHideBuiltinTarget] = useState<{ id: string; label: string } | null>(null);

  const hideBuiltin = (id: string, label: string) => {
    setHiddenBuiltins((prev) => {
      const next = prev.includes(id) ? prev : [...prev, id];
      try { localStorage.setItem('whal.hiddenWallets', JSON.stringify(next)); } catch { /* ignore */ }
      return next;
    });
    setHideBuiltinTarget(null);
    toast(`Deleted "${label}" from Wallets view`, 'info');
  };

  const restoreBuiltins = () => {
    setHiddenBuiltins([]);
    try { localStorage.removeItem('whal.hiddenWallets'); } catch { /* ignore */ }
    toast('Restored default wallets', 'ok');
  };

  const deleteBuiltinBtn = (id: string, label: string) => (
    <button
      type="button"
      onClick={() => setHideBuiltinTarget({ id, label })}
      className="inline-flex h-8 items-center gap-1.5 rounded-full border border-rose-500/25 bg-rose-500/10 px-3 text-xs font-medium text-rose-300 transition-colors hover:bg-rose-500/20"
    >
      <Trash2 className="size-3.5" aria-hidden />
      Delete
    </button>
  );

  const loadWallets = useCallback(async () => {
    setLoadingWallets(true);
    try {
      const res = await walletApi<{ items: CustomWallet[] }>('GET', '/api/wallets');
      setCustomWallets(res.items ?? []);
    } catch {
      /* ignore before auth ready */
    } finally {
      setLoadingWallets(false);
    }
  }, []);

  useEffect(() => {
    void loadWallets();
  }, [loadWallets]);

  const savePaperBalance = (val: number) => {
    if (!Number.isFinite(val) || val < 0 || val > 1_000_000) {
      toast('Enter a valid SOL amount (0 – 1,000,000)', 'error');
      return;
    }
    setPaperStartSol(val);
    setPaperInput(String(val));
    setPaperModal(false);
    toast(`Paper balance set to ${fmtSol(val, 2)} SOL`, 'ok');
  };

  const b = state.balances;
  const isShadow = state.engine.status === 'SHADOW';
  const hotWallet = state.config?.config.wallets.hot_wallet_pubkey || null;
  const sweepEnabled = state.config?.config.treasury.sweep_enabled ?? false;
  const paperBuySol = state.config?.config.strategy.default_buy_sol ?? 1;
  const paperOpen = state.positions.filter((p) => p.id.startsWith('paper_'));
  const paperOpenVal = paperOpen.reduce((a, p) => a + p.tokens * p.price, 0);
  const paperEquitySol = computePaperEquitySol(state, paperStartSol);

  const copyText = async (text: string, label = 'Address copied') => {
    try { await navigator.clipboard.writeText(text); toast(label, 'ok'); }
    catch { toast('Copy failed', 'error'); }
  };

  const handleAddWallet = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!addLabel.trim() || !addKeyInput.trim()) return;
    setSubmitting(true);
    try {
      const res = await walletApi<{ wallet: CustomWallet }>('POST', '/api/wallets', {
        label: addLabel.trim(),
        pubkeyOrSecret: addKeyInput.trim(),
      });
      setCustomWallets((prev) => [res.wallet, ...prev]);
      setAddModal(false);
      setAddLabel('');
      setAddKeyInput('');
      toast(`Added wallet "${res.wallet.label}" (${shortKey(res.wallet.pubkey, 4)})`, 'ok');
    } catch (err) {
      toast((err as Error).message || 'Failed to add wallet', 'error');
    } finally {
      setSubmitting(false);
    }
  };

  const handleGenerateWallet = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!genLabel.trim()) return;
    setSubmitting(true);
    try {
      const res = await walletApi<GeneratedSecret>('POST', '/api/wallets/generate', {
        label: genLabel.trim(),
      });
      setCustomWallets((prev) => [res.wallet, ...prev]);
      setGenerated(res);
      setRevealSecret(false);
      setCopiedSecret(false);
      toast(`Generated new Solana wallet "${res.wallet.label}"`, 'ok');
    } catch (err) {
      toast((err as Error).message || 'Failed to generate wallet', 'error');
    } finally {
      setSubmitting(false);
    }
  };

  const handleRemoveWallet = async () => {
    if (!removeTarget) return;
    setSubmitting(true);
    try {
      await walletApi<{ ok: boolean }>('DELETE', `/api/wallets/${encodeURIComponent(removeTarget.id)}`);
      setCustomWallets((prev) => prev.filter((w) => w.id !== removeTarget.id));
      toast(`Removed wallet "${removeTarget.label}"`, 'info');
      setRemoveTarget(null);
    } catch (err) {
      toast((err as Error).message || 'Failed to remove wallet', 'error');
    } finally {
      setSubmitting(false);
    }
  };

  const handleSetHotWallet = async (w: CustomWallet) => {
    if (!state.config) {
      toast('Config not loaded yet', 'error');
      return;
    }
    try {
      const nextCfg = structuredClone(state.config.config);
      nextCfg.wallets.hot_wallet_pubkey = w.pubkey;
      await send({ type: 'config.update', baseVersion: state.config.version, config: nextCfg });
      toast(`Hot wallet pubkey updated to ${w.label} (${shortKey(w.pubkey, 4)})`, 'ok');
    } catch (err) {
      toast((err as Error).message || 'Failed to update hot wallet', 'error');
    }
  };

  const downloadKeypairJson = (gen: GeneratedSecret) => {
    const blob = new Blob([JSON.stringify(gen.secretKeyArray)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${gen.wallet.label.toLowerCase().replace(/[^a-z0-9]+/g, '-') || 'solana-wallet'}-${gen.wallet.pubkey.slice(0, 6)}.json`;
    a.click();
    URL.revokeObjectURL(url);
    toast('Downloaded Solana keypair JSON', 'ok');
  };

  const shadowCard = (
    <WalletCard
      key="shadow"
      title="Paper Wallet"
      badge={isShadow ? '● Active' : 'Standby'}
      badgeTone={isShadow ? 'emerald' : 'sky'}
      activeHighlight={isShadow}
      providerLabel="Paper Simulator"
      subtitleNote={`paper equity · ${paperOpen.length} open`}
      address="ShadowPaper1111111111111111111111111111111"
      sol={paperEquitySol}
      usdc={0}
      solUsd={b.solUsd}
      onCopy={(a) => void copyText(a)}
      secondaryAction={
        <>
          <Link
            href="/?tab=positions"
            className="inline-flex h-8 items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.04] px-3 text-xs font-medium text-fg2 hover:bg-white/[0.09] hover:text-fg"
          >
            Positions ({paperOpen.length})
          </Link>
          {deleteBuiltinBtn('shadow', 'Paper Wallet')}
        </>
      }
      primaryAction={
        <button
          type="button"
          onClick={() => { setPaperInput(String(paperStartSol)); setPaperModal(true); }}
          className="inline-flex h-10 items-center gap-2 rounded-full bg-[#6c75f5] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff]"
        >
          <Pencil className="size-3.5" aria-hidden />
          Edit Paper Balance
        </button>
      }
    />
  );

  const phantomCard = (
    <WalletCard
      key="phantom"
      title="Solana Wallet"
      badge="Default"
      badgeTone="amber"
      providerLabel="Phantom"
      address={phantom.publicKey}
      sol={phantom.solBalance ?? 0}
      usdc={0}
      solUsd={b.solUsd}
      onCopy={(a) => void copyText(a)}
      onReceive={phantom.publicKey ? () => setReceiveAddr({ label: 'Phantom Wallet', addr: phantom.publicKey! }) : undefined}
      secondaryAction={
        <>
          {phantom.connected && (
            <button
              type="button"
              onClick={() => void phantom.disconnect().then(() => toast('Phantom disconnected', 'info'))}
              className="inline-flex h-8 items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.04] px-3 text-xs font-medium text-fg2 hover:bg-white/[0.09] hover:text-rose-300"
            >
              <Power className="size-3.5" aria-hidden />
              Disconnect
            </button>
          )}
          {deleteBuiltinBtn('phantom', 'Solana Wallet')}
        </>
      }
      primaryAction={
        phantom.connected ? (
          <button
            type="button"
            onClick={() => { setPaperInput(String(paperStartSol)); setPaperModal(true); }}
            className="inline-flex h-10 items-center gap-1.5 rounded-full bg-[#6c75f5] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff]"
          >
            <span aria-hidden>$</span>
            Paper balance
          </button>
        ) : phantom.installed ? (
          <button
            type="button"
            disabled={phantom.connecting}
            onClick={() => void phantom.connect().then((pk) => { if (pk) toast(`Connected Phantom (${shortKey(pk, 4)})`, 'ok'); })}
            className="inline-flex h-10 items-center gap-2 rounded-full bg-[#6c75f5] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff] disabled:opacity-60"
          >
            <Wallet className="size-4" aria-hidden />
            {phantom.connecting ? 'Connecting…' : 'Connect Phantom'}
          </button>
        ) : (
          <a
            href="https://phantom.app/"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex h-10 items-center gap-2 rounded-full bg-[#6c75f5] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff]"
          >
            Install Phantom
          </a>
        )
      }
    />
  );

  const botCard = (
    <WalletCard
      key="bot"
      title="Bot Trading Wallet"
      badge={!isShadow && state.engineSource === 'bridge' ? '● Active' : isShadow ? 'Standby (Paper ON)' : 'Configured'}
      badgeTone={!isShadow && state.engineSource === 'bridge' ? 'emerald' : 'amber'}
      activeHighlight={!isShadow && state.engineSource === 'bridge'}
      providerLabel="Engine Host"
      address={hotWallet}
      sol={b.sol + b.wsol}
      usdc={b.usdc}
      solUsd={b.solUsd}
      onCopy={(a) => void copyText(a)}
      onReceive={hotWallet ? () => setReceiveAddr({ label: 'Bot Trading Wallet', addr: hotWallet }) : undefined}
      secondaryAction={deleteBuiltinBtn('bot', 'Bot Trading Wallet')}
      primaryAction={
        <Link
          href="/settings#wallets"
          className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.06] px-5 text-sm font-semibold text-fg transition-colors hover:bg-white/[0.12]"
        >
          <Settings className="size-4" aria-hidden />
          Configure
        </Link>
      }
    />
  );

  const usdcCard = (
    <WalletCard
      key="usdc"
      title="USDC Wallet"
      badge={sweepEnabled ? 'Auto-Sweep' : 'Treasury'}
      badgeTone="sky"
      asset="usdc"
      providerLabel="SPL USDC · Bot Wallet"
      subtitleNote="swept profits · on-chain"
      address={hotWallet}
      sol={0}
      usdc={b.usdc}
      solUsd={b.solUsd}
      onCopy={(a) => void copyText(a)}
      onReceive={hotWallet ? () => setReceiveAddr({ label: 'USDC Wallet (SPL)', addr: hotWallet }) : undefined}
      secondaryAction={deleteBuiltinBtn('usdc', 'USDC Wallet')}
      primaryAction={
        <Link
          href="/treasury"
          className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.06] px-5 text-sm font-semibold text-fg transition-colors hover:bg-white/[0.12]"
        >
          <Settings className="size-4" aria-hidden />
          Treasury
        </Link>
      }
    />
  );

  return (
    <>
      <PageHeader
        title="Wallets"
        sub={
          isShadow
            ? 'Paper Mode is ON — Paper Wallet is active for all simulated trades.'
            : 'Connected Phantom browser wallet, engine hot trading wallet, and custom Solana wallets on mainnet.'
        }
        actions={
          <>
            {hiddenBuiltins.length > 0 && (
              <button
                type="button"
                onClick={restoreBuiltins}
                className="inline-flex h-10 items-center gap-2 rounded-full border border-amber-400/30 bg-amber-400/10 px-4 text-sm font-medium text-amber-200 transition-colors hover:bg-amber-400/20"
              >
                Restore Wallets ({hiddenBuiltins.length})
              </button>
            )}
            <button
              type="button"
              onClick={() => void loadWallets()}
              disabled={loadingWallets}
              className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 text-sm font-medium text-fg transition-colors hover:bg-white/[0.09] disabled:opacity-50"
            >
              <RefreshCw className={cx('size-4 text-fg2', loadingWallets && 'animate-spin')} aria-hidden />
              Refresh
            </button>
            <button
              type="button"
              onClick={() => { setAddLabel(''); setAddKeyInput(''); setAddModal(true); }}
              className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.06] px-4 text-sm font-semibold text-fg transition-colors hover:bg-white/[0.12]"
            >
              <Plus className="size-4" aria-hidden />
              Add Wallet
            </button>
            <button
              type="button"
              onClick={() => { setGenLabel(`Wallet #${customWallets.length + 1}`); setGenerated(null); setGenModal(true); }}
              className="inline-flex h-10 items-center gap-2 rounded-full bg-[#6c75f5] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#7c84ff]"
            >
              <Zap className="size-4" aria-hidden />
              Generate Wallet
            </button>
          </>
        }
      />

      <div className="grid gap-5">
        {isShadow ? (
          <>
            {!hiddenBuiltins.includes('shadow') && shadowCard}
            {!hiddenBuiltins.includes('phantom') && phantomCard}
            {!hiddenBuiltins.includes('bot') && botCard}
            {!hiddenBuiltins.includes('usdc') && usdcCard}
          </>
        ) : (
          <>
            {!hiddenBuiltins.includes('phantom') && phantomCard}
            {!hiddenBuiltins.includes('bot') && botCard}
            {!hiddenBuiltins.includes('usdc') && usdcCard}
            {!hiddenBuiltins.includes('shadow') && shadowCard}
          </>
        )}

        {customWallets.length > 0 && (
          <div className="mt-2 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-semibold uppercase tracking-wider text-fg2">
                My Wallets ({customWallets.length})
              </h2>
            </div>
            <div className="grid gap-5">
              {customWallets.map((w) => {
                const isBotActive = hotWallet === w.pubkey;
                const kindLabel = w.kind === 'generated' ? 'Generated' : w.kind === 'imported' ? 'Imported' : 'Watch-Only';
                return (
                  <WalletCard
                    key={w.id}
                    title={w.label}
                    badge={isBotActive ? '● Bot Hot Wallet' : kindLabel}
                    badgeTone={isBotActive ? 'emerald' : w.kind === 'generated' ? 'sky' : 'amber'}
                    activeHighlight={isBotActive}
                    providerLabel={w.kind === 'generated' ? 'Ed25519 Keypair' : w.kind === 'imported' ? 'Imported Key' : 'Watched Address'}
                    address={w.pubkey}
                    sol={w.sol}
                    usdc={0}
                    solUsd={b.solUsd}
                    onCopy={(a) => void copyText(a)}
                    onReceive={() => setReceiveAddr({ label: w.label, addr: w.pubkey })}
                    primaryAction={
                      <>
                        {isBotActive ? (
                          <span className="inline-flex h-10 items-center gap-1.5 rounded-full border border-emerald-400/30 bg-emerald-400/10 px-4 text-sm font-semibold text-emerald-300">
                            <Check className="size-4" aria-hidden />
                            Active Hot Wallet
                          </span>
                        ) : (
                          <button
                            type="button"
                            onClick={() => void handleSetHotWallet(w)}
                            className="inline-flex h-10 items-center gap-2 rounded-full border border-white/10 bg-white/[0.06] px-4 text-sm font-semibold text-fg transition-colors hover:bg-white/[0.12]"
                          >
                            <Wallet className="size-4" aria-hidden />
                            Set as Bot Wallet
                          </button>
                        )}
                        <button
                          type="button"
                          onClick={() => setRemoveTarget(w)}
                          className="inline-flex h-10 items-center gap-2 rounded-full border border-rose-500/30 bg-rose-500/12 px-4 text-sm font-semibold text-rose-300 transition-colors hover:bg-rose-500/22"
                        >
                          <Trash2 className="size-4" aria-hidden />
                          Delete
                        </button>
                      </>
                    }
                  />
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* Add Existing Wallet modal */}
      <Modal open={addModal} onClose={() => setAddModal(false)} title="Add Solana Wallet">
        <form className="space-y-4 text-sm" onSubmit={(e) => void handleAddWallet(e)}>
          <p className="text-fg2">
            Paste a Solana public address (watch-only) or a Base58 / JSON private key to import its public address. Private keys are <b className="text-fg">never stored</b> in the database.
          </p>
          <div>
            <label htmlFor="add-wallet-label" className="text-xs font-medium text-fg2">Wallet Label</label>
            <input
              id="add-wallet-label"
              type="text"
              required
              maxLength={48}
              placeholder="e.g. Sniper #2, Profit Vault"
              data-autofocus
              value={addLabel}
              onChange={(e) => setAddLabel(e.target.value)}
              className="mt-1.5 h-10 w-full rounded-xl border border-line2 bg-card px-3.5 text-sm text-fg outline-none focus:border-accent"
            />
          </div>
          <div>
            <label htmlFor="add-wallet-key" className="text-xs font-medium text-fg2">Solana Address or Private Key (Base58 / JSON)</label>
            <textarea
              id="add-wallet-key"
              required
              rows={3}
              placeholder="Paste public key (44 chars) or private key…"
              value={addKeyInput}
              onChange={(e) => setAddKeyInput(e.target.value)}
              className="num mt-1.5 w-full rounded-xl border border-line2 bg-card p-3 text-xs text-fg outline-none focus:border-accent"
            />
          </div>
          <div className="flex justify-end gap-2 pt-1">
            <Button onClick={() => setAddModal(false)}>Cancel</Button>
            <Button type="submit" variant="primary" disabled={submitting}>
              {submitting ? 'Adding…' : 'Add Wallet'}
            </Button>
          </div>
        </form>
      </Modal>

      {/* Generate New Solana Wallet modal */}
      <Modal
        open={genModal}
        onClose={() => { setGenModal(false); setGenerated(null); }}
        title={generated ? `Save Private Key · ${generated.wallet.label}` : 'Generate New Solana Wallet'}
      >
        {!generated ? (
          <form className="space-y-4 text-sm" onSubmit={(e) => void handleGenerateWallet(e)}>
            <p className="text-fg2">
              Creates a brand-new cryptographically secure <b className="text-fg">Ed25519 Solana keypair</b> compatible with Phantom, Solflare, and the Whal Bot engine.
            </p>
            <div>
              <label htmlFor="gen-wallet-label" className="text-xs font-medium text-fg2">Wallet Label</label>
              <input
                id="gen-wallet-label"
                type="text"
                required
                maxLength={48}
                placeholder="e.g. Trading Wallet #2"
                data-autofocus
                value={genLabel}
                onChange={(e) => setGenLabel(e.target.value)}
                className="mt-1.5 h-10 w-full rounded-xl border border-line2 bg-card px-3.5 text-sm text-fg outline-none focus:border-accent"
              />
            </div>
            <div className="flex justify-end gap-2 pt-1">
              <Button onClick={() => setGenModal(false)}>Cancel</Button>
              <Button type="submit" variant="primary" icon={Zap} disabled={submitting}>
                {submitting ? 'Generating…' : 'Generate Keypair'}
              </Button>
            </div>
          </form>
        ) : (
          <div className="space-y-4 text-sm">
            <div className="flex items-start gap-2.5 rounded-xl border border-amber-400/35 bg-amber-400/10 p-3 text-xs text-amber-200">
              <TriangleAlert className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden />
              <span>
                <b>Save this private key now!</b> For security, it is shown <b>once only</b> and never saved to the server database. Anyone with this key controls the wallet.
              </span>
            </div>

            <div>
              <div className="text-xs font-medium text-fg2">Public Address (Solana)</div>
              <div className="mt-1.5 flex items-center justify-between gap-2 rounded-xl border border-line2 bg-card px-3 py-2.5">
                <code className="num break-all text-xs text-fg">{generated.wallet.pubkey}</code>
                <Button size="sm" variant="ghost" icon={Copy} onClick={() => void copyText(generated.wallet.pubkey, 'Public address copied')}>Copy</Button>
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-fg2">Private Key (Base58 · Phantom / Solflare)</span>
                <button
                  type="button"
                  onClick={() => setRevealSecret((v) => !v)}
                  className="inline-flex items-center gap-1 text-xs font-medium text-accent hover:underline"
                >
                  {revealSecret ? <Lock className="size-3" /> : <Eye className="size-3" />}
                  {revealSecret ? 'Hide' : 'Reveal'}
                </button>
              </div>
              <div className="mt-1.5 flex items-center justify-between gap-2 rounded-xl border border-line2 bg-card px-3 py-2.5">
                <code className="num break-all text-xs text-fg">
                  {revealSecret ? generated.secretKeyBase58 : '••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••'}
                </code>
                <Button
                  size="sm"
                  variant="ghost"
                  icon={copiedSecret ? Check : Copy}
                  onClick={() => {
                    void copyText(generated.secretKeyBase58, 'Private key copied');
                    setCopiedSecret(true);
                  }}
                >
                  {copiedSecret ? 'Copied' : 'Copy'}
                </Button>
              </div>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-2 pt-2">
              <Button icon={Download} onClick={() => downloadKeypairJson(generated)}>
                Download .json Keypair
              </Button>
              <Button variant="primary" onClick={() => { setGenModal(false); setGenerated(null); }}>
                I Saved My Private Key
              </Button>
            </div>
          </div>
        )}
      </Modal>

      {/* Delete Custom Wallet confirmation modal */}
      <Modal open={!!removeTarget} onClose={() => setRemoveTarget(null)} title={`Delete Wallet · ${removeTarget?.label ?? ''}`}>
        <div className="space-y-4 text-sm">
          <p className="text-fg2">
            Permanently delete <b className="text-fg">{removeTarget?.label}</b> (<code className="num text-xs">{removeTarget ? shortKey(removeTarget.pubkey, 6) : ''}</code>) from your Wallets list?
          </p>
          <p className="text-xs text-muted">
            Make sure you have backed up your private key before deleting. Your on-chain funds remain on Solana.
          </p>
          <div className="flex justify-end gap-2 pt-1">
            <Button onClick={() => setRemoveTarget(null)}>Cancel</Button>
            <Button variant="danger" icon={Trash2} disabled={submitting} onClick={() => void handleRemoveWallet()}>
              {submitting ? 'Deleting…' : 'Delete Wallet'}
            </Button>
          </div>
        </div>
      </Modal>

      {/* Delete Built-in Wallet card confirmation modal */}
      <Modal open={!!hideBuiltinTarget} onClose={() => setHideBuiltinTarget(null)} title={`Delete Wallet · ${hideBuiltinTarget?.label ?? ''}`}>
        <div className="space-y-4 text-sm">
          <p className="text-fg2">
            Remove <b className="text-fg">{hideBuiltinTarget?.label}</b> from your Wallets page? You can restore default wallets anytime from the header.
          </p>
          <div className="flex justify-end gap-2 pt-1">
            <Button onClick={() => setHideBuiltinTarget(null)}>Cancel</Button>
            <Button variant="danger" icon={Trash2} onClick={() => hideBuiltinTarget && hideBuiltin(hideBuiltinTarget.id, hideBuiltinTarget.label)}>
              Delete Wallet
            </Button>
          </div>
        </div>
      </Modal>

      {/* Receive modal */}
      <Modal open={!!receiveAddr} onClose={() => setReceiveAddr(null)} title={`Receive SOL · ${receiveAddr?.label ?? ''}`}>
        <p className="text-sm text-fg2">Send native Solana (SOL) or SPL tokens to this address:</p>
        <div className="mt-3 flex items-center justify-between gap-2 rounded-xl border border-line2 bg-card px-3 py-2.5">
          <code className="num break-all text-xs text-fg">{receiveAddr?.addr}</code>
          {receiveAddr && <Button size="sm" variant="ghost" icon={Copy} onClick={() => void copyText(receiveAddr.addr)}>Copy</Button>}
        </div>
        <div className="mt-4 flex justify-end"><Button onClick={() => setReceiveAddr(null)}>Close</Button></div>
      </Modal>

      {/* Editable Paper balance modal */}
      <Modal open={paperModal} onClose={() => setPaperModal(false)} title="Edit Paper Balance">
        <form
          className="space-y-4 text-sm"
          onSubmit={(e) => { e.preventDefault(); savePaperBalance(Number(paperInput)); }}
        >
          <p className="text-fg2">
            Set your simulated <b className="text-fg">Paper Mode</b> bankroll in SOL. Trades use real live Pump.fun prices without spending any real funds.
          </p>

          <div>
            <label htmlFor="paper-sol-input" className="text-xs font-medium text-fg2">Starting Paper Balance (SOL)</label>
            <div className="relative mt-1.5">
              <input
                id="paper-sol-input"
                type="number"
                step="any"
                min="0"
                max="1000000"
                data-autofocus
                value={paperInput}
                onChange={(e) => setPaperInput(e.target.value)}
                className="num h-10 w-full rounded-xl border border-line2 bg-card px-3.5 pr-14 text-base font-semibold text-fg outline-none focus:border-accent"
              />
              <span className="pointer-events-none absolute right-3.5 top-1/2 -translate-y-1/2 text-xs font-semibold text-muted">SOL</span>
            </div>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {PAPER_PRESETS.map((n) => (
                <button
                  key={n}
                  type="button"
                  onClick={() => setPaperInput(String(n))}
                  className={cx(
                    'rounded-full border px-3 py-1 text-xs font-medium transition-colors',
                    Number(paperInput) === n
                      ? 'border-[#6c75f5] bg-[#6c75f5]/20 text-white'
                      : 'border-line2 bg-card text-fg2 hover:bg-raised hover:text-fg',
                  )}
                >
                  {n} SOL
                </button>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 rounded-xl border border-line bg-card p-3.5 tabular-nums">
            <div>
              <div className="text-xs text-muted">Paper buy size</div>
              <Link href="/settings#strategy" onClick={() => setPaperModal(false)} className="mt-0.5 inline-block text-base font-bold text-fg hover:text-accent hover:underline">{fmtSol(paperBuySol, 2)} SOL</Link>
            </div>
            <div>
              <div className="text-xs text-muted">Open paper value</div>
              <div className="mt-0.5 text-base font-bold text-fg">{fmtSol(paperOpenVal, 3)} SOL</div>
            </div>
            <div>
              <div className="text-xs text-muted">Open paper positions</div>
              <Link href="/?tab=positions" onClick={() => setPaperModal(false)} className="mt-0.5 inline-block font-semibold text-fg hover:text-accent hover:underline">{paperOpen.length} / 5</Link>
            </div>
            <div>
              <div className="text-xs text-muted">Session realized PnL</div>
              <div className={cx('mt-0.5 font-semibold', state.session.realizedSol >= 0 ? 'text-emerald-400' : 'text-rose-400')}>
                {fmtSol(state.session.realizedSol, 4, true)} SOL
              </div>
            </div>
          </div>

          <div className="flex items-center justify-between gap-2 pt-1">
            <span className="inline-flex items-center gap-1.5 text-xs text-muted">
              <Eye className="size-3.5 text-sky-400" aria-hidden />
              ≈ {fmtUsd((Number(paperInput) || 0) * b.solUsd)}
            </span>
            <div className="flex gap-2">
              <Button onClick={() => setPaperModal(false)}>Cancel</Button>
              <Button type="submit" variant="primary">Save balance</Button>
            </div>
          </div>
        </form>
      </Modal>
    </>
  );
}
