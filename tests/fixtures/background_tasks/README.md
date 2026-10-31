Synthetic rows in Claude Code's transcript format (tool ids, commands and descriptions are made
up). Lifecycle fields and notification text follow the shapes Claude Code writes.

- `interrupted.jsonl`: a background Bash whose session ended before its result was recorded.
- `monitors.jsonl`: two Monitors that expired with no events delivered.
- `timed_out.jsonl`: the older Monitor timeout notice; its test uses a synthetic launch.
