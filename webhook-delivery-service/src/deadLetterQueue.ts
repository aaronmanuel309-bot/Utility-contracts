/**
 * Dead Letter Queue (DLQ) for the Webhook Delivery Service.
 *
 * Webhook deliveries that permanently fail (max attempts exhausted) or that are
 * rejected by the SSRF security shield are moved into the dead letter queue
 * instead of being silently dropped. This preserves the failed messages for
 * inspection, offline retry ("requeue"), and operational forensics, so that a
 * downstream subscriber outage or a mis-configured endpoint is never the cause
 * of silent data loss.
 *
 * The queue is backed by an in-memory bounded store (mirroring the rest of the
 * service's in-memory delivery pipeline) and is drained through the `/deadletter`
 * HTTP endpoints. A pluggable requeue handler is registered by the delivery
 * module so that dead letters can be pushed back onto the active delivery queue
 * with a fresh retry budget.
 */
import type { WebhookPayload } from './delivery';
import {
  trackDeadLetterCount,
  trackDeadLetterEnqueued,
  trackDeadLetterDiscarded,
} from './metrics';

export type DeadLetterReason = 'MAX_ATTEMPTS_EXHAUSTED' | 'SSRF_BLOCKED';

export interface DeadLetterEntry {
  /** Identifier of the original webhook job. */
  id: string;
  /** Destination endpoint that the webhook was targeting. */
  url: string;
  /** Event payload that could not be delivered. */
  payload: WebhookPayload;
  /** Shared secret used for HMAC signing (required for requeue). */
  secret: string;
  /** Optional Ed25519 private key used for signing (required for requeue). */
  privateKey?: string;
  /** Number of delivery attempts made before dead-lettering. */
  attempts: number;
  /** Maximum number of attempts permitted for redelivery. */
  maxAttempts: number;
  /** Why the message was dead-lettered. */
  reason: DeadLetterReason;
  /** Human readable error description captured at failure time. */
  errorMessage: string;
  /** HTTP status code observed on the last failed attempt, if any. */
  statusCode?: number;
  /** Timestamp (ms) at which the message entered the dead letter queue. */
  deadLetteredAt: number;
}

const DEFAULT_MAX_DEAD_LETTERS = 1000;

const store = new Map<string, DeadLetterEntry>();
let maxDeadLetters = DEFAULT_MAX_DEAD_LETTERS;

/*
 * The requeue orchestration (dead letter -> active delivery queue) lives in the
 * delivery module, which owns both the queue and this store. This module only
 * exposes the primitive data operations; requeueing pops an entry and the
 * delivery module reconstructs a fresh job from it.
 */

/**
 * Insert a permanently failed delivery into the dead letter queue. If the
 * queue is at capacity the oldest entry is evicted (FIFO) so the queue stays
 * bounded; evicted entries are tracked as discarded for alerting purposes.
 */
export function reportDeadLetter(entry: DeadLetterEntry): void {
  if (!store.has(entry.id) && store.size >= maxDeadLetters) {
    const oldest = oldestEntryId();
    if (oldest) {
      store.delete(oldest);
      trackDeadLetterDiscarded();
    }
  }
  store.set(entry.id, entry);
  trackDeadLetterEnqueued(entry.reason);
  trackDeadLetterCount(store.size);
}

/** Return a snapshot of all dead letters, newest first. */
export function getDeadLetters(): DeadLetterEntry[] {
  return [...store.values()].sort((a, b) => b.deadLetteredAt - a.deadLetteredAt);
}

/** Return a single dead letter by id. */
export function getDeadLetter(id: string): DeadLetterEntry | undefined {
  return store.get(id);
}

/** Return the current number of dead letters. */
export function getDeadLetterCount(): number {
  return store.size;
}

/**
 * Atomically remove a dead letter from the queue and return it so the caller
 * can re-enqueue it as a fresh delivery job. Returns `undefined` if the entry
 * does not exist.
 */
export function popDeadLetter(id: string): DeadLetterEntry | undefined {
  const entry = store.get(id);
  if (!entry) {
    return undefined;
  }
  store.delete(id);
  trackDeadLetterCount(store.size);
  return entry;
}

/**
 * Remove a single dead letter from the queue without requeueing it.
 * Returns `true` if an entry was present and removed.
 */
export function removeDeadLetter(id: string): boolean {
  const existed = store.delete(id);
  if (existed) {
    trackDeadLetterCount(store.size);
    trackDeadLetterDiscarded();
  }
  return existed;
}

/** Remove all dead letters from the queue. Returns the number removed. */
export function purgeDeadLetters(): number {
  const count = store.size;
  if (count > 0) {
    store.clear();
    trackDeadLetterCount(0);
    for (let i = 0; i < count; i++) {
      trackDeadLetterDiscarded();
    }
  }
  return count;
}

/** Clear the DLQ and reset internal state (primarily for tests). */
export function resetDeadLetterQueue(): void {
  store.clear();
  maxDeadLetters = DEFAULT_MAX_DEAD_LETTERS;
  trackDeadLetterCount(0);
}

/** Configure the maximum size of the dead letter queue (primarily for tests). */
export function setMaxDeadLetters(size: number): void {
  maxDeadLetters = size;
}

/** Return the id of the oldest entry (the FIFO eviction candidate). */
function oldestEntryId(): string | undefined {
  let oldestId: string | undefined;
  let oldestTime = Infinity;
  for (const entry of store.values()) {
    if (entry.deadLetteredAt < oldestTime) {
      oldestTime = entry.deadLetteredAt;
      oldestId = entry.id;
    }
  }
  return oldestId;
}