# Reading worker history

`ccx history` prints past conversations of the workers you name, as text, and
searches them. It never sends anything and works on stopped and busy workers.

## What a line means

    #184.0 10-25 00:40 U: do we need to support apple silicon?
        .7 A*: Yes for the CLI, no for the dashboard…
        [12 tools · 4m10 · pytest×3, git×2]

- `#184.0` is turn 184, message 0: the same numbers as `ccx logs`. Inside a
  turn, `.7` is message 7 of that turn.
- `U` user, `U+` a message sent while the turn ran, `N[kind]` a notification
  or event (`bg`, `monitor`, `agent`, `cron`, `system`, `ccx`, …), `/` a slash
  command, `!` an interrupt, `A` assistant text, `A*` the final answer, `A…`
  the latest text of a turn still running, `T` a tool call (`✗` failed).
- `══ #212.3 compaction` marks a compaction; `--turns 212.3` prints its summary
  when one is stored (Codex encrypts its summaries).

## Recipes

| need | command |
|---|---|
| what a worker did lately | `ccx history W` |
| a long history at a glance | `ccx history W --view map` |
| where something was decided | `ccx history W --grep 'cache|eager' --role user,final` |
| the exchange around a hit | `ccx history W --turns 1032-1034` |
| one message in full | `ccx history W --turns 1033.12` |
| every instruction the user gave | `ccx history W --role user --turns 1-` |
| what woke a worker | `ccx history W --role notify` |
| how a worker tested | `ccx history W --cmd pytest --role tool,output` |
| what failed | `ccx history W --errors` |
| your children's last turns | `ccx history A B C --turns -1` |
| several workers in one period | `ccx history A B --since '11-07 21:00' --until '11-07 22:00'` |
| after your own compaction | `ccx history $CCX_SELF_WORKER_ID --turns <latest compaction address> --max-chars 2000` |

## Things worth knowing

- A search shows the newest 20 hits per worker, oldest first; the count line
  says how to see older ones. `-C 1` reads the matching turns instead of hit
  lines.
- `--cmd` matches the command each shell call ran, not words inside it:
  `--cmd python` finds `cd x && timeout 30 .venv/bin/python -m pytest`, not a
  message that mentions python. `--grep` searches text.
- Tool output is searched only with `--role output`. A cut summary
  (`--max-chars`) keeps its end, where current work and next steps are.
- Turn times come from the session; times are local, and the header names the
  zone.
- Searching your own running turn finds your own `ccx history` call.
