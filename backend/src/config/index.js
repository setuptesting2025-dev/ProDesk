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
    accessToken: process.env.DHAN_ACCESS_TOKEN || ''
  },

  angel: {
    apiKey: process.env.ANGEL_API_KEY || '',
    clientCode: process.env.ANGEL_CLIENT_CODE || '',
    pin: process.env.ANGEL_PIN || '',
    totpSecret: process.env.ANGEL_TOTP_SECRET || ''
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
  return Boolean(config.dhan.clientId && config.dhan.accessToken);
}

export function hasAngelCredentials() {
  return Boolean(
    config.angel.apiKey &&
    config.angel.clientCode &&
    config.angel.pin &&
    config.angel.totpSecret
  );
}
