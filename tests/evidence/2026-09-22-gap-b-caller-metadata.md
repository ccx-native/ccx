# Gap B live evidence — Protocol V1 caller metadata

Date: 2026-09-22  
Scope: disposable CCX root only (`/tmp/ccx-gap-b-live-20260922` during execution; removed after this note was written)  
Engines: Codex `gpt-5.6-luna` / low, Claude `haiku` / low  
Parent: managed Codex `gpt-5.6-luna` / low  
Result: **PASS**

No normal-root CCX worker was used. No raw transcript, native thread id, event id, worker short, auth material, environment secret, or provider payload is retained here.

## Capability

A real one-shot `system.capabilities` request advertised:

```text
turn.send.caller_metadata
```

## Wire request

Both children were sent through the one-shot Protocol V1 process surface. Identifiers below are synthetic correlation values used only for this disposable campaign.

```json
{
  "protocol_version": 1,
  "request_id": "<transport-attempt>",
  "op": "turn.send",
  "params": {
    "target": "<disposable-child>",
    "text": "Reply exactly GAP-B-<ENGINE>-DONE.",
    "delivery_id": "gap-b-live-<engine>-delivery",
    "notify_parent": true,
    "caller_metadata": {
      "agentInstanceId": "live-agent-<engine>",
      "workId": "live-work-<engine>",
      "executionId": "live-exec-<engine>"
    }
  }
}
```

The immediate `turn.send` result for each engine returned the same object as normalized `caller_metadata`. Codex placement was already `accepted`; Claude was still `pending` at the immediate response, which is valid because placement/settlement can lag the one-shot request.

## Settled wake and notification.inspect

For **both Codex and Claude**, the notification reached terminal state `delivered`. The stored final payload returned by real `notification.inspect` contained:

```json
{
  "callerMetadata": {
    "agentInstanceId": "live-agent-<engine>",
    "workId": "live-work-<engine>",
    "executionId": "live-exec-<engine>"
  }
}
```

The normalized inspect projection independently returned:

```json
{
  "caller_metadata": {
    "agentInstanceId": "live-agent-<engine>",
    "workId": "live-work-<engine>",
    "executionId": "live-exec-<engine>"
  },
  "operation": {
    "caller_metadata": {
      "agentInstanceId": "live-agent-<engine>",
      "workId": "live-work-<engine>",
      "executionId": "live-exec-<engine>"
    }
  }
}
```

The managed Codex parent's normalized transcript was then read through Protocol V1 `transcript.read`. For each child, the actual delivered parent wake transcript contained the exact synthetic caller metadata object. The transcript itself is intentionally not retained.

This proves the wake is self-correlating even if a product has not yet persisted the `notification.event_id` from the racing `turn.send` response.

## Durable reconciliation after runtime shutdown

After the child workers were parked and the disposable Codex daemon was stopped, fresh one-shot Protocol V1 `turn.reconcile` processes read the durable send receipts.

Both engines returned `status=accepted` and the exact bound object in both:

- top-level `caller_metadata`;
- detailed `delivery.caller_metadata`.

This is a post-runtime durable-state check, not an in-memory assertion.

## Assertions from the campaign

```text
Codex / Luna:
  child settled: idle
  notification: delivered
  wake caller metadata: exact match
  notification.inspect caller metadata: exact match
  parent transcript wake: exact match
  post-park turn.reconcile: accepted + exact match

Claude / Haiku:
  child settled: idle
  notification: delivered
  wake caller metadata: exact match
  notification.inspect caller metadata: exact match
  parent transcript wake: exact match
  post-park turn.reconcile: accepted + exact match
```
