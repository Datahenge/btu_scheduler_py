# Future: Outbound Webhook Notifications

## Why this was deferred

Slack notifications were removed in the Phase 1 refactor. The `slack_sdk` dependency was only ever used by the `btu test slack` diagnostic command — it was never called during normal daemon operation. Carrying a vendor-specific SDK for a dormant capability wasn't worth it, especially given that job *outcomes* (success, failure, exception) are not visible to the scheduler at all — it enqueues tasks and moves on. Outcome-level alerting belongs in Frappe BTU, not here.

## The right future abstraction

If administrators need push notifications for scheduler-level events (connectivity failures, daemon restarts, unrecoverable errors), the right abstraction is a **generic outbound webhook**: a single configurable HTTP POST to a URL the operator supplies.

A single webhook field covers every destination without SDK dependencies:

| Destination | How |
|---|---|
| Slack | Slack Incoming Webhook URL |
| Microsoft Teams | Teams Incoming Webhook URL |
| Discord | Discord Webhook URL |
| PagerDuty | PagerDuty Events API v2 endpoint |
| ntfy.sh | `https://ntfy.sh/<topic>` |
| Custom dashboard | Any HTTP endpoint |

## Proposed config field

```
BTU_SCHEDULER_ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Optional. When set, the daemon POSTs a JSON payload on scheduler-level error events.

## Suggested payload shape

```json
{
  "source": "btu-scheduler",
  "host": "<hostname>",
  "level": "error",
  "event": "redis_connection_lost",
  "message": "Cannot reach Redis at 10.0.0.5:6379 after 3 retries.",
  "timestamp": "2026-07-05T18:42:00Z"
}
```

Keeping the payload generic means any receiver can parse it without btu-specific knowledge.

## What events are worth alerting on

The scheduler only knows about its own health, not job outcomes. Meaningful scheduler-level events:

- Redis unreachable at startup or during polling
- SQL database unreachable at startup or during queue refill
- Frappe web server unreachable when attempting to enqueue a task
- Daemon crash / unhandled exception in the asyncio TaskGroup

Job-level failures (task raised an exception, timed out, was retried) are Frappe BTU's responsibility.

## Implementation sketch

A single `_post_alert(config, event, message)` helper in `lib/utils.py` using `requests.post()` — no new dependencies. Guard it with `if config.alert_webhook_url` so it's a no-op when unconfigured.
