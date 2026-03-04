import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: Number(__ENV.K6_VUS || 20),
  duration: __ENV.K6_DURATION || '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250'],
    checks: ['rate>0.99'],
  },
};

const baseUrl = __ENV.BASE_URL || 'http://127.0.0.1:18080';

export default function () {
  const livez = http.get(`${baseUrl}/livez`);
  check(livez, {
    'livez status is 200': (r) => r.status === 200,
  });

  const readyz = http.get(`${baseUrl}/readyz`);
  check(readyz, {
    'readyz status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}
