// Flash sale: 50,000 notifications arrive within 60 seconds as 50 batches
// of 1,000 (sms, email and push mixed). The API must accept them quickly;
// the workers then drain the backlog at the provider's limit (100/s per
// channel, so about 167 s for 16,667 per channel) without exceeding it,
// losing or duplicating anything.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, headers, item, setProvider, resetProviderStats, waitForDrain, report, providerThresholds } from './lib.js';

const BATCHES = Number(__ENV.BATCHES || 50);
const SIZE = 1000;

export const options = {
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    flash_sale: {
      executor: 'constant-arrival-rate',
      rate: BATCHES, timeUnit: '60s', duration: '60s',
      preAllocatedVUs: 5, maxVUs: 20,
    },
  },
  teardownTimeout: '15m',
  thresholds: {
    'http_req_duration{name:create_batch}': ['p(99)<2000'],
    'checks{check:batch accepted}': ['rate==1'],
    queue_drain_seconds: [`max<${Math.ceil(BATCHES * SIZE / 3 / 100) + 90}`], // drain at 100/s per channel, plus slack
    notifications_failed: ['value==0'],
    ...providerThresholds,
  },
};

export function setup() {
  setProvider({ failure_rate: 0, rate_limit_rate: 0, permanent_rate: 0, latency: '20ms', latency_jitter: '30ms' });
  resetProviderStats();
  return { start: Date.now() };
}

export default function () {
  const base = (__VU * 1000 + __ITER) * SIZE;
  const items = Array.from({ length: SIZE }, (_, i) => item(base + i, 'flash'));
  const res = http.post(`${BASE_URL}/api/v1/notifications/batch`, JSON.stringify({ items }), {
    headers, tags: { name: 'create_batch' }, timeout: '30s',
  });
  check(res, { 'batch accepted': (r) => r.status === 201 });
}

export function teardown(data) {
  waitForDrain(data.start, 900);
  report('flash sale', BATCHES * SIZE, data.start);
}
