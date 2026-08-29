import { JobScheduler, LeaseStore } from '../src/jobScheduler';

/** Poll until `cond` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 3000, intervalMs = 10): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('LeaseStore (shared lease registry)', () => {
  test('claim grants an unexpired lease to the requesting worker', () => {
    const store = new LeaseStore();
    const attempt = store.claim('job-1', 'worker-a', 500);

    expect(attempt.granted).toBe(true);
    expect(attempt.reclaimed).toBe(false);
    expect(attempt.lease!.workerId).toBe('worker-a');
    expect(store.isActive('job-1')).toBe(true);
    expect(store.activeCount()).toBe(1);
  });

  test('claim is denied while another worker holds an active lease', () => {
    const store = new LeaseStore();
    store.claim('job-1', 'worker-a', 500);

    const attempt = store.claim('job-1', 'worker-b', 500);

    expect(attempt.granted).toBe(false);
    expect(attempt.reason).toBe('TAKEN');
    expect(store.isActive('job-1')).toBe(true);
  });

  test('an expired lease can be reclaimed by another worker', async () => {
    const store = new LeaseStore();
    store.claim('job-1', 'worker-a', 20);
    expect(store.isActive('job-1')).toBe(true);

    await sleep(40); // let the lease expire

    const attempt = store.claim('job-1', 'worker-b', 500);

    expect(attempt.granted).toBe(true);
    expect(attempt.reclaimed).toBe(true);
    expect(attempt.lease!.workerId).toBe('worker-b');
  });

  test('renew only extends a lease owned by the requesting worker', () => {
    const store = new LeaseStore();
    store.claim('job-1', 'worker-a', 50);

    expect(store.renew('job-1', 'worker-b', 5000)).toBe(false); // not the holder
    expect(store.renew('job-1', 'worker-a', 5000)).toBe(true); // holder heartbeats
    expect(store.isActive('job-1')).toBe(true);
  });

  test('release only succeeds for the current lease holder', () => {
    const store = new LeaseStore();
    store.claim('job-1', 'worker-a', 500);

    expect(store.release('job-1', 'worker-b')).toBe(false);
    expect(store.release('job-1', 'worker-a')).toBe(true);
    expect(store.isActive('job-1')).toBe(false);
    expect(store.activeCount()).toBe(0);
  });
});

describe('JobScheduler (lease-based worker claiming)', () => {
  test('a job is executed exactly once even with multiple competing workers', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(3);
    let executions = 0;
    let maxConcurrent = 0;
    let active = 0;

    scheduler.submit({
      id: 'job-once',
      runAt: Date.now(),
      execute: async () => {
        active++;
        maxConcurrent = Math.max(maxConcurrent, active);
        await sleep(30);
        active--;
        executions++;
      },
    });

    await waitFor(() => executions === 1);
    await sleep(100); // give competing workers a chance to (incorrectly) re-run

    expect(executions).toBe(1);
    expect(maxConcurrent).toBe(1); // never two workers on the same job
    expect(scheduler.getPendingCount()).toBe(0);
    expect(scheduler.getActiveLeaseCount()).toBe(0);
    await scheduler.stop();
  });

  test('independent jobs are each processed exactly once across workers', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 5 });
    scheduler.start(4);
    const executed = new Set<string>();
    const workerUsed = new Set<string>();

    for (let i = 0; i < 8; i++) {
      const id = `job-${i}`;
      scheduler.submit({
        id,
        runAt: Date.now(),
        execute: async (ctx) => {
          executed.add(id);
          workerUsed.add(ctx.workerId);
          await sleep(10);
        },
      });
    }

    await waitFor(() => executed.size === 8);
    await sleep(80);

    expect(executed.size).toBe(8);
    expect(workerUsed.size).toBeGreaterThan(1); // work was distributed
    expect(scheduler.getPendingCount()).toBe(0);
    await scheduler.stop();
  });

  test('a job is not executed before its scheduled runAt', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(2);
    let executions = 0;

    scheduler.submit({
      id: 'delayed',
      runAt: Date.now() + 200,
      execute: async () => {
        executions++;
      },
    });

    await sleep(80);
    expect(executions).toBe(0); // not due yet

    await waitFor(() => executions === 1);
    await scheduler.stop();
  });

  test('reschedule() re-queues a job at a future time (retry pattern)', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(2);
    let executions = 0;
    let scheduledAgain = false;

    scheduler.submit({
      id: 'retrying',
      runAt: Date.now(),
      execute: async (ctx) => {
        executions++;
        if (!scheduledAgain) {
          scheduledAgain = true;
          ctx.reschedule(Date.now() + 30); // like a backoff retry
        }
      },
    });

    await waitFor(() => executions === 2);
    await sleep(80);
    expect(executions).toBe(2); // ran twice, then done
    expect(scheduler.getPendingCount()).toBe(0);
    await scheduler.stop();
  });

  test('heartbeat renewal prevents a healthy long-running job from being stolen', async () => {
    const scheduler = new JobScheduler({
      leaseDurationMs: 30, // lease would expire quickly...
      leaseRenewIntervalMs: 5, // ...but the worker heartbeats faster than expiry
      pollIntervalMs: 5,
    });
    scheduler.start(2);
    let executions = 0;

    scheduler.submit({
      id: 'long-running',
      runAt: Date.now(),
      execute: async () => {
        executions++;
        await sleep(120); // far longer than the 30ms lease
      },
    });

    await waitFor(() => executions === 1);
    await waitFor(() => scheduler.getActiveLeaseCount() === 0);
    await sleep(100); // any stolen re-execution would have happened by now

    expect(executions).toBe(1); // no double processing while healthy
    await scheduler.stop();
  });

  test('an expired lease is reclaimed by another worker (crash recovery, at-least-once)', async () => {
    const scheduler = new JobScheduler({
      leaseDurationMs: 20, // short lease
      leaseRenewIntervalMs: 60000, // effectively never heartbeats (simulates a dead worker)
      pollIntervalMs: 5,
    });
    scheduler.start(3);
    let executions = 0;

    scheduler.submit({
      id: 'crashed-worker',
      runAt: Date.now(),
      execute: async () => {
        executions++;
        await sleep(40); // lease expires mid-execution because there is no heartbeat
      },
    });

    await waitFor(() => executions >= 2); // a second worker reclaimed the job
    await scheduler.stop();

    expect(executions).toBeGreaterThanOrEqual(2);
  });

  test('clear() drops queued jobs without executing them', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(2);
    let executions = 0;

    scheduler.submit({
      id: 'to-clear',
      runAt: Date.now(),
      execute: async () => {
        executions++;
      },
    });

    scheduler.clear();
    expect(scheduler.getPendingCount()).toBe(0);
    await sleep(120);
    expect(executions).toBe(0);
    await scheduler.stop();
  });

  test('onError is invoked when a job handler throws', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(2);
    const onError = jest.fn();

    scheduler.submit({
      id: 'throwing',
      runAt: Date.now(),
      execute: async () => {
        throw new Error('boom');
      },
      onError,
    });

    await waitFor(() => onError.mock.calls.length === 1);
    expect(onError.mock.calls[0][0]).toEqual(new Error('boom'));
    expect(scheduler.getPendingCount()).toBe(0);
    await scheduler.stop();
  });

  test('stop() halts workers so queued jobs are no longer processed', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    const workers = scheduler.start(2);
    expect(workers).toHaveLength(2);
    expect(scheduler.getWorkerIds()).toHaveLength(2);

    await scheduler.stop();
    expect(scheduler.getWorkerIds()).toHaveLength(0);

    let executions = 0;
    scheduler.submit({
      id: 'after-stop',
      runAt: Date.now(),
      execute: async () => {
        executions++;
      },
    });

    await sleep(150);
    expect(executions).toBe(0);
  });

  test('submit() with a future runAt is claimable only after runAt', async () => {
    const scheduler = new JobScheduler({ pollIntervalMs: 10 });
    scheduler.start(1);
    let executions = 0;

    scheduler.submit({
      id: 'future',
      runAt: Date.now() + 150,
      execute: async () => {
        executions++;
      },
    });

    await sleep(60);
    expect(executions).toBe(0);
    await waitFor(() => executions === 1);
    await scheduler.stop();
  });
});