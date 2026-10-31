# Answering input

Use a live `worker.wait` result to find a pending approval, question or form.
Reply to that specific input request with a caller-chosen response id.

When `status` is `waiting`, read top-level `pending_input`: `kind` is
`permission`, `question` or `elicitation`; `request_id` identifies the input.
Show the request to the person or policy that will answer it. Recorded
`worker.inspect` state does not establish which input is currently open.

## Reply

These are example parameters for the three reply operations. Replace the
input id with the one returned by wait and use a full worker target.

```json
{"protocol_version":2,"request_id":"approve-1","op":"input.decide","params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","request_id":"input-42","response_id":"response-1","decision":"approve"}}
```

`decision` is `approve` or `reject`. Claude also accepts an optional `reason`
for the model. Permissions work on both engines.

```json
{"protocol_version":2,"request_id":"answer-1","op":"input.answer","params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","request_id":"input-43","response_id":"response-2","answers":{"Which file should I summarize?":"README.md"}}}
```

For Claude questions, `answers` maps each question's exact full text to a
nonempty answer string. Cover every question and do not add unknown questions.

```json
{"protocol_version":2,"request_id":"form-1","op":"input.respond","params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","request_id":"input-44","response_id":"response-3","action":"accept","content":{"filename":"README.md"}}}
```

For Claude MCP forms, `action` is `accept`, `decline` or `cancel`. Accepted
`content` must match the requested form schema. Decline or cancel when no
form content should be submitted.

## Recover and continue

The outer `request_id` correlates the RPC response. `params.request_id`
identifies the worker's input, and `response_id` identifies your answer.
Save the input id, response id and payload before sending.

Read `result.input.status` and `recommended_action`. `accepted` confirms the
response; `prepared`, `submitted` or `unknown` does not confirm it was applied.
If the response is lost, repeat the same operation with the same input id,
response id and content. It will not answer twice. A conflicting id or a
request for a different input is refused; do not replace the id to force it.

After an accepted response, call `worker.wait` again. The turn may continue,
finish or ask for more input.

## Known limitations

- Questions and MCP forms are Claude-only. Codex declines its own questions;
  these operations cannot answer them.
- A Claude permission with `request_id: null` is a prompt from Claude Code's own
  safety checks, which only a person can answer: use `ccx attach` on the worker.
  The worker leaves `waiting` once it's answered there or times out.
- Normal `turn.send` is not an input response. Use the operation matching
  the pending input's kind.
- Input can change while a user decides. On a refusal, obtain a fresh live
  wait result before preparing a new response.
