# Permissions

Workers skip permission prompts by default. Restricted mode enables approval
requests. Claude workers can also wait for questions and MCP forms.

## Full and restricted

| | Claude | Codex |
|---|---|---|
| `--permissions full` (default) | `--dangerously-skip-permissions` | sandbox `danger-full-access`, approval policy `never` |
| `--permissions restricted` | Claude Code's normal permission prompts, sent to ccx | sandbox `workspace-write`, approval policy `on-request` |

With full permissions a worker can read, write and run anything your user can,
not only inside its folder; `--cwd` sets where it starts, it doesn't confine it.
ccx adds no privileges of its own.

Finer control, at creation:

- **Claude:** `--permission-mode` (Claude Code's modes, such as `acceptEdits` or
  `plan`), `--allowed-tools` and `--disallowed-tools`. Allow rules in your
  Claude settings still let matching tools run without asking. A restricted
  worker starts in Claude's `default` mode, so a `defaultMode` in your settings
  doesn't answer its prompts for it.
- **Codex:** `--sandbox` and `--approval-policy` set the two Codex settings
  directly.

Change a Claude worker's permissions later with `ccx set "$W" --permissions
restricted`. It applies the next time the worker starts; add `--restart` to stop
an idle worker so the next `send` brings it back with the change. Codex
permissions are fixed at creation.

## Approving a request

```bash
R=$(ccx new --engine claude --permissions restricted --cwd ~/proj "Run the tests")
ccx wait "$R"            # exits 4: waiting for input
ccx peek "$R"            # the tool and its input
ccx approve "$R"         # allow this one request
ccx reject "$R" --reason "Use the existing helper instead"   # or refuse it
```

Each decision covers one request. Tools allowed by the engine's settings can
still run without asking. `--reason` (Claude only) tells the model why you
rejected it. On Claude, `--wait` waits for the resumed turn and prints its reply.
On Codex, follow up with `ccx wait`.

## Questions

A Claude worker can ask a multiple-choice question (Claude Code's question
tool), even with full permissions: it's a question, not a permission. Answer
with an option label or free text:

```bash
ccx answer "$W" "Use PostgreSQL" --wait
ccx answer "$W" --option 1 "Alpha" --option 2 "Beta"   # several questions at once
```

`ccx peek "$W" --status --json` shows the pending question and its options under
`waiting_for`.

## MCP forms

An MCP server can ask for input in the middle of a tool call: a small form, or a
URL to open. ccx catches it on every Claude worker and waits, like a question:

```bash
ccx peek "$W"                                          # message and fields
ccx respond "$W" --accept --field name=value --wait    # submit
ccx respond "$W" --decline                             # say no; the server may remember
ccx respond "$W" --cancel                              # dismiss without answering
```

ccx checks the fields against the server's schema before sending anything. For a
URL form, `--accept` confirms you completed it.

## From Python and RPC

`WorkerClient.approve()` and `reject()` work on both engines; Claude questions
and MCP forms are answered with `WorkerClient.answer()` and
`respond_elicitation()`. RPC has `input.answer`, `input.decide` and
`input.respond`, each sent once per `response_id`. See the
[Python API](python/README.md) and [RPC](rpc/README.md).

## Known limitations

- **Unanswered requests time out.** A Claude permission request, question or
  form left alone for 6 hours (`CCX_PERMISSION_HOOK_TIMEOUT_S`) is denied or
  cancelled, and the turn continues.
- **Questions and MCP forms are Claude-only.** Codex's own questions to the user
  are declined.
- **Some Claude prompts can only be answered in the terminal.** Claude Code's own
  safety checks (for example a risky `rm`) ask in its terminal without a request
  id. `ccx peek` shows them; answer with `ccx attach`. Claude denies them by
  itself after a couple of minutes, and the turn continues without the command.
- **Codex permissions can't be changed after creation**, and a fork keeps its
  source's settings. Create a new worker instead.
- **One decision per request.** If several are pending, pass `--request-id`
  (`ccx peek --status` lists the ids).
