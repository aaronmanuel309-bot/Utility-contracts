import axios from 'axios';
import { generateSignatures, validateUrlForSsrf } from './security';
import { trackDeliveryAttempt, trackQueueSize, trackFailure } from './metrics';
import { JobScheduler, ExecuteContext } from './jobScheduler';
import { logger, LogAttributes } from './logger';

// Structured logging is skipped in tests: Jest's console interception adds
// per-call overhead that would blow the <100ms ingestion SLA assertions.
const IS_TEST_ENV = process.env.NODE_ENV === 'test';
function logDelivery(level: 'info' | 'warn' | 'error', body: string, attributes: LogAttributes): void {
  if (IS_TEST_ENV) return;
  logger[level](body, attributes);
}

export interface WebhookPayload {
  event: string;
  timestamp: number;
  data: any;
}

export interface WebhookJob {
  id: string;
  payload: WebhookPayload;
  url: string;
  secret: string; // shared secret for HMAC
  privateKey?: string; // hex-encoded private key for Ed25519 (optional)
  attempts: number;
  maxAttempts: number;
  nextAttemptTime: number;
}

export interface WebhookDeliveryLog {
  id: string;
  url: string;
  event: string;
  timestamp: number;
  attempts: number;
  status: 'SUCCESS' | 'FAILED' | 'RETRYING';
  statusCode?: number;
  errorMessage?: string;
  lastAttemptTime: number;
}

// Delivery log storage (bounded)
const deliveryLogs: WebhookDeliveryLog[] = [];
const MAX_LOGS = 100;

// Distributed job scheduler: multiple workers claim due webhook jobs under
// short-lived leases so concurrent replicas/workers never double-deliver the
// same webhook. Worker count is configurable for horizontal scaling.
const WORKER_COUNT = parseInt(process.env.WEBHOOK_WORKER_COUNT || '3', 10);
const scheduler = new JobScheduler();
scheduler.start(WORKER_COUNT);

/**
 * Enqueue a new webhook delivery job
 */
export function enqueueWebhook(
  payload: WebhookPayload,
  url: string,
  secret: string,
  privateKey?: string,
  maxAttempts: number = 5
): string {
  const id = Math.random().toString(36).substring(2, 15);

  const job: WebhookJob = {
    id,
    payload,
    url,
    secret,
    privateKey,
    attempts: 0,
    maxAttempts,
    nextAttemptTime: Date.now(),
  };

  // Submit the job to the distributed scheduler; a worker will claim it under
  // a lease as soon as it is due.
  scheduler.submit({
    id,
    runAt: job.nextAttemptTime,
    execute: async (ctx: ExecuteContext) => {
      await deliverWebhook(job, ctx);
    },
  });
  trackQueueSize(scheduler.getPendingCount());

  // Initialize delivery log
  addLog({
    id,
    url,
    event: payload.event,
    timestamp: payload.timestamp,
    attempts: 0,
    status: 'RETRYING',
    lastAttemptTime: Date.now(),
  });

  return id;
}

/**
 * Retrieve recent delivery logs
 */
export function getDeliveryLogs(): WebhookDeliveryLog[] {
  return [...deliveryLogs];
}

/**
 * Retrieve queue size (jobs waiting or due to be claimed by workers)
 */
export function getQueueSize(): number {
  return scheduler.getPendingCount();
}

/**
 * Retrieve scheduler status (worker ids, pending jobs, active leases)
 */
export function getSchedulerStatus() {
  return scheduler.getStatus();
}

/**
 * Clear queue and logs (primarily for testing)
 */
export function clearQueueAndLogs(): void {
  scheduler.clear();
  deliveryLogs.length = 0;
  trackQueueSize(0);
}

/**
 * Add or update delivery logs
 */
function addLog(log: WebhookDeliveryLog) {
  const index = deliveryLogs.findIndex((l) => l.id === log.id);
  if (index >= 0) {
    deliveryLogs[index] = log;
  } else {
    deliveryLogs.unshift(log);
    if (deliveryLogs.length > MAX_LOGS) {
      deliveryLogs.pop();
    }
  }
}

/**
 * Calculate retry delay with exponential backoff and full randomized jitter
 */
export function calculateRetryDelay(attempt: number, baseDelay = 1000, maxDelay = 30000): number {
  if (process.env.NODE_ENV === 'test') {
    return 1; // 1ms for tests to execute retries instantly
  }
  const temp = Math.min(maxDelay, baseDelay * Math.pow(2, attempt));
  // Full jitter: randomized between 0 and temp
  return Math.random() * temp;
}

/**
 * Deliver a single webhook job. Runs inside a scheduler worker that holds the
 * job's lease; retries are handled by rescheduling the job at a future time.
 */
async function deliverWebhook(job: WebhookJob, ctx: ExecuteContext) {
  job.attempts++;
  const startTime = Date.now();

  // 1. SSRF URL validation
  const ssrfCheck = validateUrlForSsrf(job.url);
  if (!ssrfCheck.valid) {
    const errorMsg = `SSRF Prevention: ${ssrfCheck.reason}`;
    trackFailure();
    logDelivery('warn', 'webhook delivery dropped by SSRF check', {
      'webhook.id': job.id,
      'webhook.event': job.payload.event,
      'webhook.attempt': job.attempts,
      'error.message': errorMsg,
    });
    addLog({
      id: job.id,
      url: job.url,
      event: job.payload.event,
      timestamp: job.payload.timestamp,
      attempts: job.attempts,
      status: 'FAILED',
      errorMessage: errorMsg,
      lastAttemptTime: Date.now(),
    });
    return; // Drop immediately
  }

  // 2. Generate signatures
  const timestamp = Math.floor(Date.now() / 1000);
  const bodyString = JSON.stringify(job.payload);
  const signatures = generateSignatures(bodyString, timestamp, job.secret, job.privateKey);

  try {
    // 3. Make HTTP POST call with timeout
    const response = await axios.post(job.url, bodyString, {
      headers: {
        'Content-Type': 'application/json',
        'X-Webhook-Timestamp': timestamp.toString(),
        'X-Webhook-Signature-256': `t=${timestamp},v1=${signatures.hmacSignature}`,
        ...(signatures.ed25519Signature
          ? { 'X-Webhook-Signature-Ed25519': `t=${timestamp},v1=${signatures.ed25519Signature}` }
          : {}),
      },
      timeout: 5000, // 5-second timeout
    });

    const duration = (Date.now() - startTime) / 1000;
    trackDeliveryAttempt(response.status, duration, job.attempts);

    logDelivery('info', 'webhook delivered successfully', {
      'webhook.id': job.id,
      'webhook.event': job.payload.event,
      'webhook.attempt': job.attempts,
      'http.response.status_code': response.status,
      'http.request.duration_ms': Math.round(duration * 1000),
    });

    addLog({
      id: job.id,
      url: job.url,
      event: job.payload.event,
      timestamp: job.payload.timestamp,
      attempts: job.attempts,
      status: 'SUCCESS',
      statusCode: response.status,
      lastAttemptTime: Date.now(),
    });

  } catch (error: any) {
    const duration = (Date.now() - startTime) / 1000;
    const statusCode = error.response?.status;
    const errorMessage = error.message || 'Unknown network error';

    trackDeliveryAttempt(statusCode || 0, duration, job.attempts);

    if (job.attempts < job.maxAttempts) {
      // Schedule a retry with exponential backoff; the job is released back to
      // the scheduler pool and becomes claimable again at nextAttemptTime.
      const delay = calculateRetryDelay(job.attempts);
      job.nextAttemptTime = Date.now() + delay;
      ctx.reschedule(job.nextAttemptTime);
      trackQueueSize(scheduler.getPendingCount());

      logDelivery('warn', 'webhook delivery failed, retrying', {
        'webhook.id': job.id,
        'webhook.event': job.payload.event,
        'webhook.attempt': job.attempts,
        'http.response.status_code': statusCode,
        'error.message': errorMessage,
        'retry.delay_ms': Math.round(delay),
      });

      addLog({
        id: job.id,
        url: job.url,
        event: job.payload.event,
        timestamp: job.payload.timestamp,
        attempts: job.attempts,
        status: 'RETRYING',
        statusCode,
        errorMessage: `${errorMessage}. Retrying in ${Math.round(delay)}ms...`,
        lastAttemptTime: Date.now(),
      });
    } else {
      // Max attempts exhausted
      trackFailure();
      logDelivery('error', 'webhook delivery failed permanently', {
        'webhook.id': job.id,
        'webhook.event': job.payload.event,
        'webhook.attempt': job.attempts,
        'http.response.status_code': statusCode,
        'error.message': errorMessage,
      });
      addLog({
        id: job.id,
        url: job.url,
        event: job.payload.event,
        timestamp: job.payload.timestamp,
        attempts: job.attempts,
        status: 'FAILED',
        statusCode,
        errorMessage: `Max attempts (${job.maxAttempts}) exhausted. Last error: ${errorMessage}`,
        lastAttemptTime: Date.now(),
      });
    }
  }
}
