import { DhanAdapter } from './DhanAdapter.js';
import { AngelAdapter } from './AngelAdapter.js';
import { config, hasDhanCredentials, hasAngelCredentials } from '../config/index.js';
import {
  canAutoRefreshDhanToken,
  startDhanTokenAutoRefresh,
  onDhanTokenRefreshed
} from '../auth/dhanTokenManager.js';

/**
 * AdapterManager
 *
 * Owns provider selection and fallback. Downstream code (rolling
 * store, engines) never imports a concrete adapter — only this
 * manager, and only through onQuote/onDepth/onError + getStatus().
 *
 * Fallback rule (LIVE ONLY — no mock/synthetic data anywhere):
 *   1. DHAN primary
 *   2. ANGEL fallback
 * If both live providers fail (at startup or mid-session), the
 * manager does NOT fabricate data. It preserves the last valid
 * market state, marks the frontend STALE/DISCONNECTED, and keeps
 * retrying the primary on an interval. Real tick-by-tick data or
 * an explicit stale flag — never a synthetic tick.
 */
export class AdapterManager {
  constructor() {
    this.active = null; // current adapter instance
    this.activeName = null; // 'DHAN' | 'ANGEL' | 'MOCK'
    this._quoteCallbacks = [];
    this._depthCallbacks = [];
    this._statusCallbacks = [];
    this._instruments = [];
    this._depthInstruments = [];
  }

  onQuote(cb) {
    this._quoteCallbacks.push(cb);
  }
  onDepth(cb) {
    this._depthCallbacks.push(cb);
  }
  onStatusChange(cb) {
    this._statusCallbacks.push(cb);
  }

  getStatus() {
    return {
      activeProvider: this.activeName,
      connectionStatus: this.active?.getConnectionStatus() || 'DISCONNECTED',
      dataMode: config.dataMode
    };
  }

  async start() {
    if (config.dataMode === 'REPLAY') {
      // Replay mode is wired up in src/replay — AdapterManager just
      // stays idle and lets the replay service push into the store.
      this.activeName = 'REPLAY';
      return;
    }

    // LIVE mode: try primary, then secondary. No mock fallback.
    if (config.primaryBroker === 'DHAN' && hasDhanCredentials()) {
      try {
        const dhanConfig = { ...config.dhan };
        if (canAutoRefreshDhanToken()) {
          // Auto-generate a fresh 24h token via TOTP instead of
          // trusting a possibly-stale pasted DHAN_ACCESS_TOKEN.
          dhanConfig.accessToken = await startDhanTokenAutoRefresh();
          if (!this._dhanRefreshWired) {
            this._dhanRefreshWired = true;
            // Every daily refresh, reconnect with the new token so
            // the WebSocket never sits on an expired one.
            onDhanTokenRefreshed(() => {
              if (this.activeName === 'DHAN') {
                this._notifyStatus({ event: 'DHAN_TOKEN_ROTATING', action: 'RECONNECT' });
                this.start();
              }
            });
          }
        }
        await this._activate(new DhanAdapter(dhanConfig), 'DHAN');
        return;
      } catch (err) {
        this._notifyStatus({ event: 'PRIMARY_FAILED', provider: 'DHAN', error: err.message });
      }
    }

    if (config.secondaryBroker === 'ANGEL' && hasAngelCredentials()) {
      try {
        await this._activate(new AngelAdapter(config.angel), 'ANGEL');
        return;
      } catch (err) {
        this._notifyStatus({ event: 'SECONDARY_FAILED', provider: 'ANGEL', error: err.message });
      }
    }

    // Both live providers unavailable at startup. Do NOT fabricate data.
    // Surface DISCONNECTED to the frontend and keep retrying the primary
    // broker on a fixed interval until a real connection succeeds.
    this.active = null;
    this.activeName = null;
    this._notifyStatus({ event: 'ALL_PROVIDERS_FAILED', action: 'RETRY_LIVE_ONLY' });
    this._scheduleRetry();
  }

  _scheduleRetry() {
    if (this._retryTimer) return;
    this._retryTimer = setTimeout(async () => {
      this._retryTimer = null;
      if (this.active) return; // a connection succeeded via another path meanwhile
      this._notifyStatus({ event: 'RETRYING_LIVE_PROVIDERS' });
      await this.start();
    }, config.liveRetryIntervalMs);
  }

  async subscribe(instruments) {
    this._instruments = instruments;
    if (!this.active) return;
    await this.active.subscribeMarketData(instruments);
  }

  async subscribeDepth(instruments, levels) {
    this._depthInstruments = instruments;
    if (!this.active) return;
    await this.active.subscribeDepth(instruments, levels);
  }

  async getOptionChain(underlying, expiry) {
    if (!this.active) return null;
    return this.active.getOptionChain(underlying, expiry);
  }

  async _activate(adapterInstance, name) {
    // If replacing a live connection (e.g. daily Dhan token rotation),
    // cleanly close the old socket first instead of leaking it.
    if (this.active && this.active !== adapterInstance) {
      try {
        await this.active.disconnect();
      } catch {
        // best-effort — the old socket may already be dead
      }
    }

    this.active = adapterInstance;
    this.activeName = name;

    adapterInstance.onQuote((q) => {
      for (const cb of this._quoteCallbacks) cb(q, name);
    });
    adapterInstance.onDepth((d) => {
      for (const cb of this._depthCallbacks) cb(d, name);
    });
    adapterInstance.onError((err) => this._handleAdapterError(err));

    await adapterInstance.connect();
    // Re-subscribe immediately so a token-rotation reconnect doesn't
    // silently sit idle until the next manual subscribe() call.
    if (this._instruments.length) await adapterInstance.subscribeMarketData(this._instruments);
    if (this._depthInstruments.length) await adapterInstance.subscribeDepth(this._depthInstruments, 20);
    this._notifyStatus({ event: 'CONNECTED', provider: name });
  }

  async _handleAdapterError(err) {
    this._notifyStatus({ event: 'ADAPTER_ERROR', ...err });

    // If the primary (Dhan) drops, attempt automatic fallback to Angel.
    if (this.activeName === 'DHAN' && hasAngelCredentials()) {
      try {
        await this._activate(new AngelAdapter(config.angel), 'ANGEL');
        if (this._instruments.length) await this.active.subscribeMarketData(this._instruments);
        if (this._depthInstruments.length) await this.active.subscribeDepth(this._depthInstruments);
        return;
      } catch (fallbackErr) {
        this._notifyStatus({ event: 'FALLBACK_FAILED', provider: 'ANGEL', error: fallbackErr.message });
      }
    }

    // Both live providers unavailable — preserve last state, mark STALE.
    this._notifyStatus({ event: 'ALL_LIVE_PROVIDERS_DOWN', action: 'PRESERVE_LAST_STATE_MARK_STALE' });
  }

  _notifyStatus(payload) {
    // Always print to server logs (Render/PM2/etc). Without this, a
    // failed DHAN/ANGEL connect is only ever sent over the socket to
    // the frontend and is invisible when debugging from server logs.
    const level = /FAILED|ERROR|DOWN/.test(payload.event) ? 'error' : 'log';
    console[level](`[AdapterManager] ${payload.event}`, payload);
    for (const cb of this._statusCallbacks) cb(payload);
  }
}
