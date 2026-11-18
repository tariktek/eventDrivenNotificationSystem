// Unreliable provider: 20% of calls fail with 503 (and 5% more with 429)
// while 100 notifications per second arrive for one minute. Retries with
// backoff must deliver everything: with 5 attempts the chance that one
// notification fails all of them is 0.25^5 ≈ 0.1%. The 25% error rate
// stays below the circuit breaker's 50% trip ratio, so this is
// "degraded", not "down": deliveries continue.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, headers, item, setProvider, resetProviderStats, waitForDrain, report, providerThresholds } from './lib.js';

const RATE = Number(__ENV.RATE || 100);

export const options = {
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    faults: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: '60s',
      preAllocatedVUs: 20, maxVUs: 100,
    },
  },
  teardownTimeout: '15m',
  thresholds: {
    'checks{check:created}': ['rate==1'],
    // Expected failures: about 0.1% of 6,000 (attempts exhausted).
    notifications_failed: [`value<=${Math.ceil(RATE * 60 * 0.004)}`],
    ...providerThresholds,
  },
};

export function setup() {
  setProvider({ failure_rate: 0.2, rate_limit_rate: 0.05, permanent_rate: 0, latency: '20ms', latency_jitter: '30ms' });
  resetProviderStats();
  return { start: Date.now() };
}

export default function () {
  const res = http.post(`${BASE_URL}/api/v1/notifications`, JSON.stringify(item(__VU * 1000000 + __ITER, 'faults')), {
    headers, tags: { name: 'create' },
  });
  check(res, { created: (r) => r.status === 201 });
}

export function teardown(data) {
  waitForDrain(data.start, 900);
  report('provider failing 25% of calls', RATE * 60, data.start);
  setProvider({ failure_rate: 0, rate_limit_rate: 0, permanent_rate: 0, latency: '20ms', latency_jitter: '30ms' });
}
