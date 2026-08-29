import request from 'supertest';
import app from '../src/index';
import { DeadLetterEntry, DeadLetterReason } from '../src/deadLetterQueue';
import * as dlq from '../src/deadLetterQueue';
import * as delivery from '../src/delivery';
import { resetMetricCache, getStatsSummary } from '../src/metrics';
import axios from 'axios';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

function makeEntry(overrides: Partial<DeadLetterEntry> = {}): DeadLetterEntry {
  return {
    id: Math.random().toString(36).substring(2, 15),
    url: 'https://api.example.com/hook',
    payload: { event: 'low_balance', timestamp: Date.now(), data: { meter_id: 1 } },
    secret: 'secret',
    attempts: 5,
    maxAttempts: 5,
    reason: 'MAX_ATTEMPTS_EXHAUSTED' as DeadLetterReason,
    errorMessage: 'Max attempts (5) exhausted. Last error: Network Error',
    deadLetteredAt: Date.now(),
    ...overrides,
  };
}

describe('Dead Letter Queue Module', () => {
  beforeEach(async () => {
    delivery.clearQueueAndLogs();
    resetMetricCache();
    await new Promise((resolve) => setTimeout(resolve, 30)); // let in-flight timers settle
  });

  describe('1. Reporting dead letters', () => {
    test('should add an entry and reflect it in the queue', () => {
      const entry = makeEntry({ id: 'job-1' });
      dlq.reportDeadLetter(entry);

      expect(dlq.getDeadLetterCount()).toBe(1);
      expect(dlq.getDeadLetter('job-1')).toEqual(entry);
      expect(dlq.getDeadLetters()[0].id).toBe('job-1');
      expect(getStatsSummary().dlqCount).toBe(1);
    });

    test('should return undefined for unknown ids', () => {
      expect(dlq.getDeadLetter('nope')).toBeUndefined();
      expect(dlq.getDeadLetterCount()).toBe(0);
    });

    test('should return newest-first ordering', () => {
      dlq.reportDeadLetter(makeEntry({ id: 'a', deadLetteredAt: 100 }));
      dlq.reportDeadLetter(makeEntry({ id: 'b', deadLetteredAt: 300 }));
      dlq.reportDeadLetter(makeEntry({ id: 'c', deadLetteredAt: 200 }));

      expect(dlq.getDeadLetters().map((e) => e.id)).toEqual(['b', 'c', 'a']);
    });
  });

  describe('2. FIFO eviction at capacity', () => {
    test('should evict the oldest entry when the queue is full', () => {
      dlq.setMaxDeadLetters(2);
      dlq.reportDeadLetter(makeEntry({ id: 'first', deadLetteredAt: 100 }));
      dlq.reportDeadLetter(makeEntry({ id: 'second', deadLetteredAt: 200 }));
      dlq.reportDeadLetter(makeEntry({ id: 'third', deadLetteredAt: 300 }));

      expect(dlq.getDeadLetterCount()).toBe(2);
      expect(dlq.getDeadLetter('first')).toBeUndefined(); // evicted
      expect(dlq.getDeadLetter('second')).toBeDefined();
      expect(dlq.getDeadLetter('third')).toBeDefined();
    });

    test('should not evict when reporting an existing id', () => {
      dlq.setMaxDeadLetters(1);
      dlq.reportDeadLetter(makeEntry({ id: 'a', deadLetteredAt: 100 }));
      dlq.reportDeadLetter(makeEntry({ id: 'a', attempts: 6 }));

      expect(dlq.getDeadLetterCount()).toBe(1);
      expect(dlq.getDeadLetter('a')!.attempts).toBe(6);
    });
  });

  describe('3. Popping dead letters for requeue', () => {
    test('should pop the entry and remove it from the queue', () => {
      dlq.reportDeadLetter(makeEntry({ id: 'job-1' }));

      const popped = dlq.popDeadLetter('job-1');

      expect(popped).toBeDefined();
      expect(popped!.id).toBe('job-1');
      expect(dlq.getDeadLetter('job-1')).toBeUndefined();
      expect(dlq.getDeadLetterCount()).toBe(0);
    });

    test('should return undefined for an unknown id', () => {
      expect(dlq.popDeadLetter('unknown')).toBeUndefined();
      expect(dlq.getDeadLetterCount()).toBe(0);
    });
  });

  describe('4. Removing and purging dead letters', () => {
    test('should remove a single dead letter', () => {
      dlq.reportDeadLetter(makeEntry({ id: 'job-1' }));
      expect(dlq.removeDeadLetter('job-1')).toBe(true);
      expect(dlq.getDeadLetterCount()).toBe(0);
      expect(dlq.removeDeadLetter('job-1')).toBe(false);
    });

    test('should purge all dead letters and report the count', () => {
      dlq.reportDeadLetter(makeEntry({ id: 'a' }));
      dlq.reportDeadLetter(makeEntry({ id: 'b' }));
      const removed = dlq.purgeDeadLetters();

      expect(removed).toBe(2);
      expect(dlq.getDeadLetterCount()).toBe(0);
      expect(getStatsSummary().dlqCount).toBe(0);
    });
  });
});

describe('DLQ Integration with Delivery', () => {
  beforeEach(async () => {
    jest.clearAllMocks();
    delivery.clearQueueAndLogs();
    dlq.resetDeadLetterQueue();
    resetMetricCache();
    await new Promise((resolve) => setTimeout(resolve, 30));
  });

  test('permanently failed deliveries (max attempts exhausted) enter the DLQ', async () => {
    jest.spyOn(delivery, 'calculateRetryDelay').mockReturnValue(1);
    mockedAxios.post.mockRejectedValue({ message: 'Network Error', response: { status: 500 } });

    const jobId = delivery.enqueueWebhook(
      { event: 'test', timestamp: Date.now(), data: {} },
      'https://api.example.com/hook',
      'secret',
      undefined,
      2
    );

    await new Promise((resolve) => setTimeout(resolve, 500));

    const entry = dlq.getDeadLetter(jobId);
    expect(entry).toBeDefined();
    expect(entry!.reason).toBe('MAX_ATTEMPTS_EXHAUSTED');
    expect(entry!.attempts).toBe(2);
    expect(entry!.maxAttempts).toBe(2);
    expect(entry!.url).toBe('https://api.example.com/hook');
    expect(entry!.errorMessage).toContain('Max attempts (2) exhausted');
  });

  test('SSRF-blocked deliveries enter the DLQ without an HTTP request', async () => {
    const jobId = delivery.enqueueWebhook(
      { event: 'test', timestamp: Date.now(), data: {} },
      'http://127.0.0.1:9000/hooks',
      'secret'
    );

    await new Promise((resolve) => setTimeout(resolve, 100));

    const entry = dlq.getDeadLetter(jobId);
    expect(entry).toBeDefined();
    expect(entry!.reason).toBe('SSRF_BLOCKED');
    expect(entry!.errorMessage).toContain('SSRF Prevention');
    expect(mockedAxios.post).not.toHaveBeenCalled();
  });

  test('requeueing a dead letter re-enqueues and can be delivered successfully', async () => {
    jest.spyOn(delivery, 'calculateRetryDelay').mockReturnValue(1);

    // First: let the job fail permanently so it lands in the DLQ.
    mockedAxios.post
      .mockRejectedValueOnce({ message: 'Network Error', response: { status: 500 } })
      .mockRejectedValueOnce({ message: 'Network Error', response: { status: 500 } });

    const jobId = delivery.enqueueWebhook(
      { event: 'test', timestamp: Date.now(), data: {} },
      'https://api.example.com/hook',
      'secret',
      undefined,
      2 // fails after 2 attempts
    );

    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(dlq.getDeadLetter(jobId)).toBeDefined();

    // Second: requeue the dead letter and let it succeed this time.
    mockedAxios.post.mockResolvedValue({ status: 200, data: {} });
    const newJobId = delivery.requeueDeadLetter(jobId);
    expect(newJobId).not.toBeNull();

    await new Promise((resolve) => setTimeout(resolve, 500));

    const logs = delivery.getDeliveryLogs();
    const log = logs.find((l) => l.id === newJobId!);
    expect(log).toBeDefined();
    expect(log!.status).toBe('SUCCESS');
    expect(dlq.getDeadLetter(jobId)).toBeUndefined(); // moved off the DLQ
    expect(dlq.getDeadLetterCount()).toBe(0);
  });

  test('DLQ endpoints list, requeue, and delete dead letters over HTTP', async () => {
    // Fail a webhook so it lands in the DLQ.
    jest.spyOn(delivery, 'calculateRetryDelay').mockReturnValue(1);
    mockedAxios.post.mockRejectedValue({ message: 'Network Error', response: { status: 500 } });
    const jobId = delivery.enqueueWebhook(
      { event: 'test', timestamp: Date.now(), data: {} },
      'https://api.example.com/hook',
      'secret',
      undefined,
      2
    );
    await new Promise((resolve) => setTimeout(resolve, 500));

    // List
    let res = await request(app).get('/deadletter');
    expect(res.status).toBe(200);
    expect(res.body.count).toBe(1);
    expect(res.body.deadLetters[0].id).toBe(jobId);

    // Fetch single
    res = await request(app).get(`/deadletter/${jobId}`);
    expect(res.status).toBe(200);
    expect(res.body.id).toBe(jobId);

    // Requeue (delivery now succeeds) -> removed from DLQ
    mockedAxios.post.mockResolvedValue({ status: 200, data: {} });
    res = await request(app).post(`/deadletter/${jobId}/requeue`);
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('REQUEUED');
    expect(dlq.getDeadLetterCount()).toBe(0);

    // 404 for unknown id
    res = await request(app).get('/deadletter/does-not-exist');
    expect(res.status).toBe(404);

    // Purge requires confirmation
    dlq.reportDeadLetter(makeEntry({ id: 'x' }));
    res = await request(app).delete('/deadletter');
    expect(res.status).toBe(400);

    res = await request(app).delete('/deadletter?confirm=true');
    expect(res.status).toBe(200);
    expect(res.body.message).toContain('1');
    expect(dlq.getDeadLetterCount()).toBe(0);
  });
});