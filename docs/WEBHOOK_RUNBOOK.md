# Webhook Delivery Service Runbook & Disaster Recovery

**Service Name**: Webhook Delivery Service
**Criticality**: Tier-1 (High Availability, 99.99% Target)
**P99 Latency SLA**: < 100ms Ingestion

---

## 1. Diagnostics & Monitoring Checklists

If an incident or alert is triggered, use these fast-path diagnostic endpoints to isolate the root cause:

### Step 1: Health Status Query
```bash
curl -s http://webhook-service.internal/health
```
**Expected Output**:
```json
{ "status": "UP", "queueSize": 0 }
```

### Step 2: Queue size and Summary Statistics
```bash
curl -s http://webhook-service.internal/stats
```
*Check `queueSize` and `successRate`. If `queueSize` is climbing (> 1000) and `successRate` is dropping (< 95%), a downstream receiver or network partition is likely causing delivery failures.*

### Step 3: View Recent Failure Logs
```bash
curl -s http://webhook-service.internal/logs | grep -E '"status":"FAILED"|"status":"RETRYING"'
```

---

## 2. Emergency Escalation Workflows

### Scenario 1: Queue Backlog Accumulation (`queueSize` climbing rapidly)
- **Symptom**: `webhook_queue_size_current` is sustained above 500.
- **Root Cause**: Downstream client webhook endpoints are offline, rate-limiting requests (HTTP 429), or experiencing extreme latencies, clogging the background processor thread.
- **Remedy Actions**:
  1. Increase horizontal scale (increase replica count of the webhook containers) to expand total delivery concurrency.
  2. Increase the maximum attempts limit temporarily or lower the HTTP timeout value from 5s to 2s to prune slow connections faster.
  ```bash
  # Example to scale replicas in Kubernetes
  kubectl scale deployment webhook-delivery-service --replicas=10
  ```

### Scenario 2: High Memory / OOM Crashing
- **Symptom**: Webhook process crashes with "Out of Memory" or CPU utilization is constantly at 100%.
- **Root Cause**: Memory leak in the in-memory queue or too many pending retries under extreme ingestion spikes.
- **Remedy Actions**:
  1. Terminate container and force restart to release leaked memory buffer.
  2. Implement rate limiting on ingestion endpoints to protect the memory boundaries.
  3. Deploy a permanent out-of-process persistent queue (like Redis or RabbitMQ) if traffic spikes are consistently exceeding the memory boundaries.

---

## 3. Webhook Secret Key Rotation Procedure

To maintain high security, shared webhook secrets must be rotated every **180 days**, or immediately upon key compromise:

### Step 1: Generate New Shared Secret
Generate a secure, cryptographically random key:
```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
# Output example: b98b816a13d9cfc892809e20a2e39958e2bfb73be5c6138be6e3557e49c7bc29
```

### Step 2: Implement "Double-Signing" Transition Period
To prevent breaking integrations during rotation:
1. Configure the Webhook Service to temporarily sign payloads with **both** the old secret and the new secret.
2. Provide the new secret key to the subscriber.
3. Once the subscriber updates their webhook receiver to verify using the new secret, remove the old secret from the active signing list.

---

## 4. SSRF Prevention Audits

To ensure the safety of the off-chain system, the SSRF (Server-Side Request Forgery) engine must be audited after any networking or DNS upgrades:
1. Verify that the URL parser correctly flags subnets by running integration tests.
2. Inspect server firewalls, ensuring egress traffic is strictly barred from routing to cloud provider private IP ranges and internal Kubernetes API service accounts.

---

## 5. Scheduler & Worker Operations

Queue processing runs on the distributed job scheduler, where workers claim jobs under short-lived leases. Diagnose scheduler health through the `webhook_scheduler_*` metrics on `/metrics` and the scheduler block on `/health`.

### Health Indicators
- `webhook_scheduler_workers_current` **0** → worker loops are not running; the scheduler cannot drain the queue. Restart the service.
- `webhook_scheduler_active_leases_current` sustained at the worker count → all workers are blocked on slow deliveries; inspect downstream endpoint latency and consider scaling out.
- `webhook_scheduler_lease_reclaimed_total` climbing → workers are expiring mid-delivery (lease not renewed). Investigate event-loop blocking / GC pauses, or increase the lease duration.

### Diagnosing duplicate or missed deliveries
1. Confirm workers are healthy: `curl -s http://webhook-service.internal/health` and check `scheduler.workers` is non-empty and `scheduler.pendingCount` is not climbing.
2. Confirm no lease thrash: `curl -s http://webhook-service.internal/metrics | grep webhook_scheduler_lease_reclaimed_total`.
3. If `pendingCount` climbs while `active_leases` stays low, a worker crash loop is likely; scale the deployment and inspect container restart counts.

### Tuning
- Delivery concurrency per instance: `WEBHOOK_WORKER_COUNT` (default `3`).
- Lease duration and heartbeat are configurable in `jobScheduler.ts` (`leaseDurationMs`, `leaseRenewIntervalMs`, `pollIntervalMs`).
