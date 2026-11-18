// Steady traffic: 150 single notifications per second for 2 minutes,
// mixed channels and priorities. The point is API latency under load
// (one request per notification, idempotency keys included) while the
// workers deliver concurrently.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, headers, item, setProvider, resetProviderStats, waitForDrain, report, providerThresholds } from './lib.js';

const RATE = Number(__ENV.RATE || 150);
const DURATION = __ENV.DURATION || '2m';

export const options = {
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 100,
    },
  },
  teardownTimeout: '10m',
  thresholds: {
    // Measured on a laptop through Docker Desktop: p50 ~33 ms, p95 ~105 ms.
    // The bounds catch real regressions (the NOTIFY trigger made p50 855 ms).
    'http_req_duration{name:create}': ['p(95)<200', 'p(99)<400'],
    'checks{check:created}': ['rate==1'],
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
  const i = __VU * 1000000 + __ITER;
  const res = http.post(`${BASE_URL}/api/v1/notifications`, JSON.stringify(item(i, 'steady')), {
    headers: { ...headers, 'Idempotency-Key': `steady-${__VU}-${__ITER}-${Date.now()}` },
    tags: { name: 'create' },
  });
  check(res, { created: (r) => r.status === 201 });
}

export function teardown(data) {
  waitForDrain(data.start, 600);
  report('steady traffic', null, data.start);
}
