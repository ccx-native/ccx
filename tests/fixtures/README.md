# Native transcript fixtures

These files are **observations of provider-native shapes**, not declarations that
providers will serialize the same way forever.

In particular, `claude_compaction_observed_2026_09.jsonl` captures the Claude Code
shape observed in September 2026: a `compact_boundary`, a structurally linked
`isCompactSummary` child, and local-command bookkeeping that can duplicate the
user's submitted slash command. CCX relies only on exact native identities
(`uuid`/`parentUuid`, `promptId` plus exact normalized command), never timestamps,
adjacency, or prose prefixes. If Claude changes its native journal format, update
or add an observed fixture and review the adapter; unrecognized/unlinked summary
text must degrade visibly rather than disappear.

Codex `contextCompaction` remains intentionally opaque. Fixtures/tests must not
invent readable summary semantics for encrypted or undocumented provider data.
