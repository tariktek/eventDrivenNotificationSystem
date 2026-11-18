// Shared helpers for the k6 scenarios. Run them with `make test-load` (or
// see loadtest/README.md); they talk to the docker compose stack.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Gauge } from 'k6/metrics';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const MOCK_URL = __ENV.MOCK_URL || 'http://localhost:9000';
const API_KEY = __ENV.API_KEY || 'dev-api-key-change-me';
const RATE_LIMIT = Number(__ENV.RATE_LIMIT || 100); // WORKER_RATE_LIMIT

export const headers = { 'Content-Type': 'application/json', 'X-API-Key': API_KEY };
export const CHANNELS = ['sms', 'email', 'push'];

// Measured after the load: how long the backlog took to drain, and what
// the provider saw.
const drainTime = new Trend('queue_drain_seconds');
const maxPerSecond = new Gauge('provider_max_per_second');
const duplicates = new Gauge('provider_duplicates');
const accepted = new Gauge('provider_accepted');
const failed = new Gauge('notifications_failed');

export function item(i, tag) {
  const channel = CHANNELS[i % 3];
  const n = {
    channel,
    recipient: channel === 'sms' ? `+90555${String(i % 10000000).padStart(7, '0')}`
      : channel === 'email' ? `user${i}@example.com` : `device-token-${i}`,
    content: `${tag} #${i}: 50% off today only`,
    priority: i % 10 === 0 ? 'high' : 'normal',
  };
  if (channel !== 'sms') n.subject = 'Flash sale';
  return n;
}

export function setProvider(cfg) {
  const res = http.put(`${MOCK_URL}/admin/config`, JSON.stringify(cfg), { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'provider configured': (r) => r.status === 200 });
}

export function resetProviderStats() {
  http.post(`${MOCK_URL}/admin/reset`);
}

function metrics() {
  return http.get(`${BASE_URL}/api/v1/metrics`, { headers, tags: { name: 'metrics' } }).json();
}

// waitForDrain polls the JSON metrics until nothing is pending, scheduled
// or in flight, and returns the seconds since startMs.
export function waitForDrain(startMs, timeoutSec) {
  const deadline = Date.now() + timeoutSec * 1000;
  while (Date.now() < deadline) {
    const q = metrics().queue;
    const left = Object.values(q).reduce((s, c) => s + c.pending + c.scheduled + c.processing, 0);
    if (left === 0) {
      sleep(6); // one more sampling period: the sampler runs every 5 s
      if (Object.values(metrics().queue).reduce((s, c) => s + c.pending + c.scheduled + c.processing, 0) === 0) break;
    }
    sleep(2);
  }
  const secs = (Date.now() - startMs) / 1000;
  drainTime.add(secs);
  return secs;
}

// failedSince counts notifications created since startMs that ended in
// failed (dead letter), so earlier runs do not count.
function failedSince(startMs) {
  const from = new Date(startMs - 1000).toISOString();
  const res = http.get(`${BASE_URL}/api/v1/notifications?status=failed&limit=100&created_from=${encodeURIComponent(from)}`,
    { headers, tags: { name: 'list_failed' } });
  return res.json().items.length; // 100 means "100 or more"
}

// report records what the provider saw and prints a short summary.
export function report(name, expected, startMs) {
  const s = http.get(`${MOCK_URL}/admin/stats`).json();
  const m = metrics();
  const nFailed = failedSince(startMs);
  const worst = Math.max(...Object.values(s.max_per_second || { none: 0 }));
  maxPerSecond.add(worst);
  duplicates.add(s.duplicates);
  accepted.add(s.accepted);
  failed.add(nFailed);
  console.log(`\n== ${name} ==
  expected notifications : ${expected === null ? '-' : expected}
  provider accepted      : ${s.accepted} (received ${s.received}, so ${s.received - s.accepted} retried or rejected calls)
  duplicates             : ${s.duplicates}
  busiest second/channel : ${JSON.stringify(s.max_per_second)} (limit ${RATE_LIMIT}/s)
  failed (dead letter)   : ${nFailed}
  latency (last 15 min)  : p50 ${m.latency.all.p50_seconds.toFixed(2)}s  p95 ${m.latency.all.p95_seconds.toFixed(2)}s  p99 ${m.latency.all.p99_seconds.toFixed(2)}s
  everything delivered in: ${((Date.now() - startMs) / 1000).toFixed(1)}s\n`);
  return s;
}

// Sends leave exactly 1/RATE_LIMIT apart (the limiter's own tests prove
// it), but arrival times at the receiver carry network jitter: under heavy
// load a few requests from one second's edge land in the next, and the
// mock counts per calendar second. Measured: 101-103 at a limit of 100.
// A provider that enforces its limit strictly on arrival should get a
// little headroom (WORKER_RATE_LIMIT=95).
export const providerThresholds = {
  provider_max_per_second: [`value<=${Math.ceil(RATE_LIMIT * 1.05)}`],
  provider_duplicates: ['value==0'],
};
