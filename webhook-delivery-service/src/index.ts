import express, { Request, Response } from 'express';
import { enqueueWebhook, getDeliveryLogs, getQueueSize, requeueDeadLetter } from './delivery';
import {
  getDeadLetters,
  getDeadLetter,
  getDeadLetterCount,
  removeDeadLetter,
  purgeDeadLetters,
} from './deadLetterQueue';
import { getPrometheusMetrics, getStatsSummary } from './metrics';
import { logger } from './logger';

const app = express();
const port = process.env.PORT || 3001;

// Middlewares
app.use(express.json());

// Enable CORS for dashboard queries
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

/**
 * POST /webhooks
 * Decoupled High-Performance Webhook Ingestion API (<100ms SLA, typically <10ms)
 */
app.post('/webhooks', (req: Request, res: Response) => {
  const started = process.hrtime.bigint();
  const { payload, url, secret, privateKey, maxAttempts } = req.body;

  // Input validation
  if (!payload || !url || !secret) {
    return res.status(400).json({
      error: 'Missing required parameters. "payload", "url", and "secret" are mandatory.',
    });
  }

  if (typeof payload !== 'object' || typeof url !== 'string' || typeof secret !== 'string') {
    return res.status(400).json({
      error: 'Invalid parameter types.',
    });
  }

  // Enqueue job asynchronously (executes in <1ms)
  const jobId = enqueueWebhook(
    payload,
    url,
    secret,
    privateKey,
    maxAttempts ? parseInt(maxAttempts, 10) : undefined
  );

  // Return immediately with 202 Accepted
  const durationMs = Number(process.hrtime.bigint() - started) / 1e6;
  trackIngestionDuration(durationMs);
  return res.status(202).json({
    status: 'ACCEPTED',
    message: 'Webhook enqueued for asynchronous delivery.',
    jobId,
  });
});

/**
 * GET /metrics
 * Expose standard Prometheus exposition formats
 */
app.get('/metrics', async (req: Request, res: Response) => {
  try {
    const metrics = await getPrometheusMetrics();
    res.set('Content-Type', 'text/plain; version=0.0.4');
    return res.send(metrics);
  } catch (err: any) {
    return res.status(500).send(err.message);
  }
});

/**
 * GET /stats
 * Expose real-time metrics for direct dashboard rendering
 */
app.get('/stats', (req: Request, res: Response) => {
  const stats = getStatsSummary();
  return res.json({
    ...stats,
    queueSize: getQueueSize(),
  });
});

/**
 * GET /logs
 * Expose historical logs of delivery attempts
 */
app.get('/logs', (req: Request, res: Response) => {
  const logs = getDeliveryLogs();
  return res.json(logs);
});

/**
 * GET /deadletter
 * List all messages held in the dead letter queue
 */
app.get('/deadletter', (req: Request, res: Response) => {
  return res.json({
    count: getDeadLetterCount(),
    deadLetters: getDeadLetters(),
  });
});

/**
 * GET /deadletter/:id
 * Fetch a single dead letter by id
 */
app.get('/deadletter/:id', (req: Request, res: Response) => {
  const entry = getDeadLetter(req.params.id);
  if (!entry) {
    return res.status(404).json({ error: 'Dead letter not found.' });
  }
  return res.json(entry);
});

/**
 * POST /deadletter/:id/requeue
 * Push a dead letter back onto the active delivery queue
 */
app.post('/deadletter/:id/requeue', (req: Request, res: Response) => {
  const newJobId = requeueDeadLetter(req.params.id);
  if (newJobId === null) {
    return res.status(404).json({ error: 'Dead letter not found or cannot be requeued.' });
  }
  return res.json({
    status: 'REQUEUED',
    message: 'Dead letter pushed back onto the active delivery queue.',
    jobId: newJobId,
  });
});

/**
 * DELETE /deadletter/:id
 * Permanently remove a single dead letter
 */
app.delete('/deadletter/:id', (req: Request, res: Response) => {
  const removed = removeDeadLetter(req.params.id);
  if (!removed) {
    return res.status(404).json({ error: 'Dead letter not found.' });
  }
  return res.json({
    status: 'DELETED',
    message: 'Dead letter removed from the queue.',
  });
});

/**
 * DELETE /deadletter
 * Purge all dead letters (requires ?confirm=true)
 */
app.delete('/deadletter', (req: Request, res: Response) => {
  if (req.query.confirm !== 'true') {
    return res.status(400).json({
      error: 'Purging the dead letter queue requires a ?confirm=true query parameter.',
    });
  }
  const removed = purgeDeadLetters();
  return res.json({
    status: 'PURGED',
    message: `Removed ${removed} dead letter(s) from the queue.`,
  });
});

/**
 * GET /health
 * Basic system health indicator
 */
app.get('/health', (req: Request, res: Response) => {
  return res.json({
    status: 'UP',
    timestamp: Date.now(),
    queueSize: getQueueSize(),
    deadLetterQueueSize: getDeadLetterCount(),
  });
});

// Start the server
if (process.env.NODE_ENV !== 'test') {
  app.listen(port, () => {
    logger.info('webhook delivery service started', { 'server.port': Number(port) });
  });
}

export default app;
