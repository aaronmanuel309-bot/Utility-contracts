/**
 * Distributed job scheduler with lease-based worker claiming.
 *
 * Multiple stateless workers (each running a `runWorker` loop) compete to claim
 * due jobs from a shared scheduler. A worker claims a job by acquiring a short
 * lived *lease* from a shared lease registry (modelled here by `LeaseStore`; in
 * production the same protocol maps to Redis/etcd). While a lease is valid only
 * the holding worker may process the job, so concurrent workers can never
 * double-deliver the same unit of work.
 *
 * - **Fencing**: `LeaseStore.claim` is synchronous (therefore atomic within the
 *   Node event loop, and serialisable against a shared store across processes),
 *   so at most one worker can hold a given job's lease at a time.
 * - **Heartbeat / lease renewal**: while a job is executing, the scheduler
 *   renews the lease on an interval so a long-running, healthy job is not
 *   stolen by another worker.
 * - **Crash recovery**: if a worker fails to renew (crashed or unresponsive),
 *   its lease expires and another worker may reclaim the job, giving
 *   exactly-once-under-load / at-least-once-on-failure delivery semantics.
 * - **Retry with reschedule**: a handler may call `ctx.reschedule(runAt)` to
 *   requeue the job at a future time (e.g. exponential backoff). Until then the
 *   job is not claimable by any worker.
 */
import {
  trackActiveLeases,
  trackJobSubmitted,
  trackJobProcessed,
  trackJobFailed,
  trackSchedulerWorkers,
  trackLeaseReclaimed,
} from './metrics';

export interface ExecuteContext {
  /** Id of the claimed job. */
  jobId: string;
  /** Worker that currently holds this job's lease. */
  workerId: string;
  /** Extend the current lease (heartbeat) for this job. */
  renewLease: () => void;
  /** Requeue the job to run again at the given timestamp (ms). */
  reschedule: (runAt: number) => void;
}

export interface ScheduledTask {
  id: string;
  /** Earliest timestamp (ms) at which the job becomes claimable. */
  runAt: number;
  /** Performs the job. Rescheduling is done through `ctx`. */
  execute: (ctx: ExecuteContext) => Promise<void>;
  /** Called (and swallowed) if `execute` throws. */
  onError?: (error: unknown) => void;
}

export interface Lease {
  jobId: string;
  workerId: string;
  expiresAt: number;
  fencingToken: number;
}

export interface ClaimAttempt {
  granted: boolean;
  reason: 'TAKEN' | 'GRANTED';
  reclaimed: boolean;
  lease?: Lease;
}

/**
 * Shared lease registry. Within a single process `claim` is synchronous and
 * thus atomic; when backed by a shared key/value store (Redis, etc.) the same
 * read-compare-write (with a fencing token) provides cross-process mutual
 * exclusion.
 */
export class LeaseStore {
  private readonly leases = new Map<string, Lease>();
  private tokenCounter = 0;

  private now(): number {
    return Date.now();
  }

  /**
   * Attempt to claim the lease for a job. A lease that is still unexpired is
   * held and the claim fails; an expired lease may be reclaimed. Returns
   * `false` if the lease is currently held by (possibly another) worker.
   */
  claim(jobId: string, workerId: string, leaseDurationMs: number): ClaimAttempt {
    const existing = this.leases.get(jobId);
    if (existing && existing.expiresAt > this.now()) {
      return { granted: false, reason: 'TAKEN', reclaimed: false };
    }

    const reclaimed = existing != null;
    const lease: Lease = {
      jobId,
      workerId,
      expiresAt: this.now() + leaseDurationMs,
      fencingToken: ++this.tokenCounter,
    };
    this.leases.set(jobId, lease);
    return { granted: true, reason: 'GRANTED', reclaimed, lease };
  }

  /** Extend the lease if still held by the requesting worker. */
  renew(jobId: string, workerId: string, leaseDurationMs: number): boolean {
    const lease = this.leases.get(jobId);
    if (lease && lease.workerId === workerId) {
      lease.expiresAt = this.now() + leaseDurationMs;
      return true;
    }
    return false;
  }

  /** Release a lease; only succeeds for the current holder. */
  release(jobId: string, workerId: string): boolean {
    const lease = this.leases.get(jobId);
    if (lease && lease.workerId === workerId) {
      this.leases.delete(jobId);
      return true;
    }
    return false;
  }

  /** Whether a job currently has an unexpired lease. */
  isActive(jobId: string): boolean {
    const lease = this.leases.get(jobId);
    return lease != null && lease.expiresAt > this.now();
  }

  /** Number of currently active (unexpired) leases. */
  activeCount(): number {
    const now = this.now();
    let count = 0;
    for (const lease of this.leases.values()) {
      if (lease.expiresAt > now) {
        count++;
      }
    }
    return count;
  }
}

export interface JobSchedulerOptions {
  /**
   * How long a worker's claim on a job lasts before it expires and may be
   * reclaimed by another worker.
   * @default 30000
   */
  leaseDurationMs?: number;
  /**
   * How often the running worker renews (heartbeats) its lease while a job is
   * executing.
   * @default 5000
   */
  leaseRenewIntervalMs?: number;
  /**
   * How often idle workers poll for newly due jobs.
   * @default 50
   */
  pollIntervalMs?: number;
}

export class JobScheduler {
  private readonly leaseStore = new LeaseStore();
  private readonly pending = new Map<string, ScheduledTask>();
  private readonly leaseDurationMs: number;
  private readonly leaseRenewIntervalMs: number;
  private readonly pollIntervalMs: number;
  private readonly workers = new Set<string>();
  private workerCounter = 0;
  private running = false;

  constructor(options: JobSchedulerOptions = {}) {
    this.leaseDurationMs = options.leaseDurationMs ?? 30000;
    this.leaseRenewIntervalMs = options.leaseRenewIntervalMs ?? 5000;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
  }

  /**
   * Register a unit of work. Jobs are only executed once their `runAt` has
   * elapsed and a worker acquires the lease.
   */
  submit(task: ScheduledTask): void {
    this.pending.set(task.id, task);
    trackJobSubmitted();
    trackActiveLeases(this.leaseStore.activeCount());
  }

  /**
   * Start worker loops. Idempotent: repeated calls return the existing workers
   * rather than spawning duplicates. Returns the worker ids.
   */
  start(workerCount: number): string[] {
    if (this.running) {
      return [...this.workers];
    }
    this.running = true;
    for (let i = 0; i < workerCount; i++) {
      const workerId = `worker-${this.workerCounter++}`;
      this.workers.add(workerId);
      setImmediate(() => {
        void this.runWorker(workerId);
      });
    }
    trackSchedulerWorkers(this.workers.size);
    return [...this.workers];
  }

  /** Stop all worker loops and wait for them to exit. */
  async stop(): Promise<string[]> {
    this.running = false;
    const ids = [...this.workers];
    this.workers.clear();
    trackSchedulerWorkers(0);
    trackActiveLeases(this.leaseStore.activeCount());
    // Give running iterations a moment to observe the stop flag.
    await new Promise((resolve) => setTimeout(resolve, this.pollIntervalMs * 2));
    return ids;
  }

  /** Drop every queued job that is waiting for execution. */
  clear(): void {
    this.pending.clear();
    trackActiveLeases(this.leaseStore.activeCount());
  }

  /** Number of jobs currently waiting (or due) to be claimed. */
  getPendingCount(): number {
    return this.pending.size;
  }

  /** Number of jobs currently being executed under an active lease. */
  getActiveLeaseCount(): number {
    return this.leaseStore.activeCount();
  }

  getWorkerIds(): string[] {
    return [...this.workers];
  }

  getLeaseStore(): LeaseStore {
    return this.leaseStore;
  }

  /** Status summary for dashboards / `/health`. */
  getStatus(): { workers: string[]; pendingCount: number; activeLeases: number } {
    return {
      workers: this.getWorkerIds(),
      pendingCount: this.getPendingCount(),
      activeLeases: this.getActiveLeaseCount(),
    };
  }

  /** Claim the next due job not already under an active lease. */
  private pickAndClaim(workerId: string): ScheduledTask | null {
    const now = Date.now();
    for (const task of this.pending.values()) {
      if (task.runAt > now) {
        continue;
      }
      const attempt = this.leaseStore.claim(task.id, workerId, this.leaseDurationMs);
      if (attempt.granted) {
        if (attempt.reclaimed) {
          trackLeaseReclaimed();
        }
        trackActiveLeases(this.leaseStore.activeCount());
        return task;
      }
    }
    return null;
  }

  /** Polling loop for a single worker. */
  private async runWorker(workerId: string): Promise<void> {
    while (this.running) {
      const claimed = this.pickAndClaim(workerId);
      if (!claimed) {
        // No due job available; back off before polling again.
        await delay(this.pollIntervalMs);
        continue;
      }
      await this.execute(workerId, claimed);
    }
  }

  private async execute(workerId: string, task: ScheduledTask): Promise<void> {
    // The job stays in the pending pool while in flight. Its active lease
    // prevents any other worker from claiming it; if the lease expires without
    // renewal (worker crash), another worker may reclaim it (at-least-once
    // recovery). On completion the job is removed from the pool entirely.
    let rescheduleAt: number | null = null;

    // Heartbeat the lease while the job is in flight so a healthy execution is
    // never stolen by a competing worker.
    const heartbeat = setInterval(() => {
      this.leaseStore.renew(task.id, workerId, this.leaseDurationMs);
    }, this.leaseRenewIntervalMs);

    const ctx: ExecuteContext = {
      jobId: task.id,
      workerId,
      renewLease: () => this.leaseStore.renew(task.id, workerId, this.leaseDurationMs),
      reschedule: (at: number) => {
        rescheduleAt = at;
      },
    };

    try {
      await task.execute(ctx);
      trackJobProcessed();
    } catch (error) {
      trackJobProcessed();
      trackJobFailed();
      if (task.onError) {
        try {
          task.onError(error);
        } catch {
          // Never let error handling mask a delivery failure.
        }
      }
    } finally {
      clearInterval(heartbeat);
      this.leaseStore.release(task.id, workerId);
      trackActiveLeases(this.leaseStore.activeCount());
    }

    if (rescheduleAt !== null) {
      task.runAt = rescheduleAt;
      // Job remains in the pending pool, claimable again at the new runAt.
    } else {
      this.pending.delete(task.id);
      trackActiveLeases(this.leaseStore.activeCount());
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function createJobScheduler(options?: JobSchedulerOptions): JobScheduler {
  return new JobScheduler(options);
}