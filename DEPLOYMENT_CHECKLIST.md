# Opsyn Backend Deployment Checklist

## AWS Secrets Manager Configuration

### Required Railway Variables

Set these in your Railway project variables:

```
AWS_ACCESS_KEY_ID=<your-aws-access-key>
AWS_SECRET_ACCESS_KEY=<your-aws-secret-key>
AWS_REGION=us-east-1
AWS_SECRETS_MANAGER_PREFIX=opsyn/leaflink/webhooks
```

### Required IAM Permissions

The AWS user/role must have these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:PutSecretValue",
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret",
        "secretsmanager:DeleteSecret",
        "secretsmanager:ListSecrets"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:opsyn/leaflink/webhooks/*"
    }
  ]
}
```

### Verification

After deployment, verify AWS Secrets Manager is working:

```bash
# Check AWS configuration status
curl "https://opsyn-backend-production.up.railway.app/admin/aws-secrets-status"

# Configure a webhook (should return 200 if AWS is configured)
curl -X POST "https://opsyn-backend-production.up.railway.app/api/leaflink/orders/webhook-config" \
  -H "Content-Type: application/json" \
  -H "X-OPSYN-ORG: noble" \
  -d '{
    "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
    "webhook_key": "TEST_SECRET",
    "webhook_enabled": true,
    "webhook_signature_required": true,
    "leaflink_company_id": "9008"
  }'
```

Expected responses:
- If AWS is configured: HTTP 200
- If AWS is not configured: HTTP 503 with `AWS_SECRETS_NOT_CONFIGURED` error code
- If AWS permissions are insufficient: HTTP 503 with `AWS_SECRETS_PERMISSION_DENIED` error code

## Bootstrap Schema Recovery

Bootstrap schema recovery runs automatically during app startup. Verify it completed:

```bash
curl "https://opsyn-backend-production.up.railway.app/admin/bootstrap-status"
curl "https://opsyn-backend-production.up.railway.app/admin/raw-schema-check"
```

Both should return HTTP 200 with `ok: true`.

---

## Pre-Deployment
- [ ] All tests pass (unit + integration)
- [ ] Code review approved
- [ ] Backward compatibility verified (existing brands work without webhook_key_secret_ref)
- [ ] AWS credentials configured in production environment

## Environment Variables Required
- `AWS_ACCESS_KEY_ID` — AWS access key (required; uses IAM role if not set)
- `AWS_SECRET_ACCESS_KEY` — AWS secret key (required; uses IAM role if not set)
- `AWS_REGION` — AWS region for Secrets Manager (required; no default)
- `AWS_SECRETS_MANAGER_PREFIX` (default: opsyn/leaflink/webhooks) — secret name prefix
- `LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES` (default: 62) — lookback window for incremental sync
- `SYNC_REQUEST_WORKER_POLL_INTERVAL` (default: 1) — seconds between queue polls

## AWS Permissions Required (legacy secret path)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:DeleteSecret",
        "secretsmanager:TagResource",
        "secretsmanager:ListSecrets"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:leaflink/webhook/*"
    }
  ]
}
```

## Migrations to Run
1. `2026_05_27_01_add_webhook_credential_fields.sql` — adds webhook fields to brand_api_credentials
2. `2026_05_27_02_add_webhook_event_status_fields.sql` — creates leaflink_webhook_events table

Both migrations are idempotent (`IF NOT EXISTS`) and safe to run multiple times.

## Deployment Order
1. Deploy code (includes migrations)
2. Run migrations (automatic on startup via `services/migration_runner.py`)
3. Restart sync_request_background_worker (if running separately)
4. Verify webhook health: `GET /api/leaflink/orders/webhook-status`
5. Test webhook config: `POST /api/leaflink/orders/webhook-config` with test brand
6. Monitor logs for `[WEBHOOK_SECRET_STORE_FAILED]` or `[WEBHOOK_SECRET_LOAD_FAILED]`

## Route Namespace Reference
| Path | Method | Description |
|------|--------|-------------|
| `/webhooks/leaflink` | POST | Inbound webhook receiver (LeafLink sends here) |
| `/webhooks/leaflink/setup-info` | GET | Webhook setup info for a brand |
| `/webhooks/leaflink/diagnostic` | GET | Webhook + sync diagnostics |
| `/api/leaflink/orders/webhook-config` | POST | Configure per-brand webhook key |
| `/api/leaflink/orders/webhook-config` | GET | Get webhook config status |
| `/api/leaflink/orders/webhook-status` | GET | Aggregate webhook health metrics |
| `/api/leaflink/orders/webhook-replay` | POST | Replay unresolved/failed events |

**No changes to existing webhook receiver path** — LeafLink subscriptions continue to work.

## Sync Worker Restart Required?
- **Yes** if sync_request_background_worker runs in separate process
- **No** if running in same FastAPI process (lifespan task)
- Current: Running in FastAPI lifespan task (no separate restart needed)

## Backward Compatibility
Existing brands without `webhook_key_secret_ref` continue to work via the fallback chain:
1. `webhook_key_secret_ref` (AWS Secrets Manager) — used if set
2. `webhook_key` (plaintext, deprecated) — fallback if AWS unavailable or not configured
3. `None` — skip signature verification (log warning)

Log marker `[WEBHOOK_SECRET_FALLBACK]` is emitted whenever the fallback chain is used.

## Rollback Plan
- If AWS Secrets Manager is unavailable:
  - Webhook processing falls back to plaintext `webhook_key` column
  - Existing brands continue to work
  - New webhook config will fail with 500 error (structured JSON response)
  - Rollback: disable `webhook_enabled` flag on all brands, revert code
- If queue priority is wrong:
  - Webhook jobs may be delayed behind full resync
  - Rollback: revert code, restart worker
- If payload sanitization breaks parsing:
  - Webhook events may be truncated
  - Rollback: revert code, replay events via `POST /api/leaflink/orders/webhook-replay`

## Monitoring & Alerts
Monitor these log markers in production:

| Marker | Severity | Meaning | Action |
|--------|----------|---------|--------|
| `[WEBHOOK_SECRET_LOAD_FAILED]` | ERROR | AWS Secrets Manager load failed | Check AWS credentials/permissions |
| `[WEBHOOK_SECRET_STORE_FAILED]` | ERROR | AWS Secrets Manager store failed | Check AWS credentials/permissions |
| `[WEBHOOK_SECRET_FALLBACK]` | WARNING | Using plaintext key fallback | Migrate brand to AWS storage |
| `[WEBHOOK_LATENCY_MS]` | INFO | Every webhook request latency | Baseline monitoring |
| `[WEBHOOK_LATENCY_WARNING]` | WARNING | Webhook >500ms response time | Investigate DB query performance |
| `[WEBHOOK_LATENCY_ERROR]` | ERROR | Webhook >2000ms response time | Urgent: investigate blocking operations |
| `[WEBHOOK_PAYLOAD_SANITIZED]` | WARNING | Payload truncated/sanitized | Review LeafLink payload sizes |
| `[WEBHOOK_PAYLOAD_TRUNCATED]` | WARNING | Payload exceeded 1MB cap | Review LeafLink payload sizes |
| `[WEBHOOK_JOB_DEDUPED]` | INFO | Duplicate job skipped | Normal -- deduplication working |
| `[WEBHOOK_REPLAY_INITIATED]` | INFO | Webhook replay started/completed | Operational |
| `[QUEUE_PRIORITY_ORDER]` | INFO | Worker polling for next job | Operational |
| `[INCREMENTAL_SYNC_ENQUEUED]` | INFO | Incremental sync lookback window | Operational |
| `[INCREMENTAL_LOOKBACK_TOO_SHORT]` | WARNING | Lookback < 30 minutes | Increase LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES |

## Success Criteria
- ✅ Existing brands without webhook_key_secret_ref continue to work
- ✅ AWS Secrets Manager failures don't crash the app (graceful 500 with structured error)
- ✅ Webhook endpoint responds in <1s (logged via `[WEBHOOK_LATENCY_MS]`)
- ✅ Unresolved events return 202 and are stored for later replay
- ✅ Ambiguous events return 409 and are stored
- ✅ Webhook payloads are sanitized (max 1MB, 100KB per string, 10K array items)
- ✅ Unresolved/failed events can be replayed via `POST /api/leaflink/orders/webhook-replay`
- ✅ All routes use consistent namespace (/api/leaflink/orders or /webhooks/leaflink)
- ✅ Zero-downtime deployment (no webhook URL changes required)
- ✅ Structured AWS error responses with error_code field
