import 'dotenv/config';

function bool(v, fallback) {
  if (v === undefined) return fallback;
  return String(v).toLowerCase() === 'true';
}

export const config = {
  port: parseInt(process.env.PORT || '10000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  frontendOrigin: process.env.FRONTEND_ORIGIN || '*',

  primaryBroker: (process.env.PRIMARY_BROKER || 'DHAN').toUpperCase(),
  secondaryBroker: (process.env.SECONDARY_BROKER || 'ANGEL').toUpperCase(),

  // LIVE | REPLAY — mock/synthetic data mode has been removed entirely.
  // Replay is real recorded tick data, not fabricated data.
  dataMode: (process.env.DATA_MODE || 'LIVE').toUpperCase(),

  // How often (ms) to retry connecting to a live broker after both
  // DHAN and ANGEL fail. No mock fallback — just silence + retry.
  liveRetryIntervalMs: parseInt(process.env.LIVE_RETRY_INTERVAL_MS || '15000', 10),

  dhan: {
    clientId: process.env.DHAN_CLIENT_ID || '',
    accessToken: process.env.DHAN_ACCESS_TOKEN || '',
    // Optional: if both are set, the backend auto-generates a fresh
    // access token daily via Dhan's TOTP login endpoint instead of
    // relying on a manually pasted, 24h-expiring DHAN_ACCESS_TOKEN.
    pin: process.env.DHAN_PIN || '',
    totpSecret: process.env.DHAN_TOTP_SECRET || ''
  },

  angel: {
    apiKey: process.env.ANGEL_API_KEY || '',
    clientCode: process.env.ANGEL_CLIENT_CODE || '',
    pin: process.env.ANGEL_PIN || '',
    totpSecret: process.env.ANGEL_TOTP_SECRET || ''
  },

  // Web Push (alarm notifications) — generate a keypair once with
  // `npx web-push generate-vapid-keys` and set both below. Optional:
  // signal alarms are simply disabled if these aren't set.
  vapid: {
    publicKey: process.env.VAPID_PUBLIC_KEY || '',
    privateKey: process.env.VAPID_PRIVATE_KEY || '',
    contactEmail: process.env.VAPID_CONTACT_EMAIL || ''
  },

  ringBuffer: {
    '1s': parseInt(process.env.RING_BUFFER_1S || '120', 10),
    '5s': parseInt(process.env.RING_BUFFER_5S || '180', 10),
    '15s': parseInt(process.env.RING_BUFFER_15S || '240', 10),
    '30s': parseInt(process.env.RING_BUFFER_30S || '240', 10),
    '60s': parseInt(process.env.RING_BUFFER_60S || '300', 10),
    '5m': parseInt(process.env.RING_BUFFER_5M || '300', 10)
  },

  // Signal-only safety flag. Must never become true from env alone.
  AUTO_TRADING_ENABLED: false
};

export function hasDhanCredentials() {
  // Either a manually pasted access token, OR the pieces needed to
  // auto-generate one daily (clientId + PIN + TOTP secret).
  return Boolean(
    config.dhan.clientId &&
    (config.dhan.accessToken || (config.dhan.pin && config.dhan.totpSecret))
  );
}

export function hasAngelCredentials() {
  return Boolean(
    config.angel.apiKey &&
    config.angel.clientCode &&
    config.angel.pin &&
    config.angel.totpSecret
  );
}
