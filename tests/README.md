# ccx public test suite

**Quota-free** pytest suite covering ccx's logic and disposable fake-provider
hosts/daemons. Tests do not launch live Claude/Codex providers or consume quota.

## Running

```sh
python -m pip install pytest pytest-xdist   # dev/test dependencies only
python -m pytest -q tests/ -n 16 --dist worksteal  # full suite; tune -n to your cores
python -m pytest -q tests/test_parser.py    # focused runs stay serial
python -m py_compile ccx/*.py ccx.py examples/*.py  # byte-compile check (also run in CI)
```

`tests/conftest.py` redirects `CCX_ROOT` / `CLAUDE_CONFIG_DIR` to a throwaway
temp dir per pytest worker before any `ccx` module imports. Tune `-n` to about
two-thirds of your logical CPU cores, or fewer under load. No parallel
`addopts` are applied to focused runs.

## Coverage

| File | Area |
| --- | --- |
| `test_commands_helpers.py` | command helper logic: list filters, cwd tree-match, recency/relative-time, prompt-source validation, report extraction, peek JSON fitting |
| `test_dashboard_example.py` | dashboard example helpers, sorting/filtering, worker-thread seams, stream lifecycle, and headless snapshot mode |
| `test_examples_importable.py` | headless import smoke for every Python example |
| `test_external.py` | external record/mode detection, wait classification, state derivation, import/native-bg send eligibility, capability hints, pending-question payloads |
| `test_host_helpers.py` | host-side helper behavior that can be exercised without launching Claude |
| `test_liveness.py` | pid liveness + session-identity parsing (argv/`/proc` gates) |
| `test_main_entrypoint.py` | direct `ccx/__main__.py` execution used by host/hook re-exec paths |
| `test_next_turn_wait.py` | managed `--next-turn` observation against a fake host socket: input-gate endings, epoch-ordered sidecar catch-up, and the `after_turn` settled-turn checkpoint (latest-state fast path, baseline races, timeouts, terminal/host-dead exits) |
| `test_parser.py` | CLI parser wiring: subcommand dispatch, cap-flag validation, choice enforcement |
| `test_peek_output.py` | `ccx peek --json`, `--brief`, and `--status` output contracts |
| `test_records.py` | record construction + sandboxed write/read/merge round-trip |
| `test_rpc.py` | RPC/revive/wait behavior with fake records and sockets |
| `test_rpc_helpers.py` | side-effect-free RPC helper functions |
| `test_transcript.py` | transcript cleanup: command-tag filtering, tool-call formatting, tool-result truncation, turn pairing, auto-continue swallowing, assistant-text extraction, pending-question detection |
| `test_usage.py` | usage-cap math: metric utilization, cap-tripped logic (fail-open on absent metrics), `--cap-*` percentage validation, cap merge (strictest-wins) |

## Notes for maintainers

Live tests that need a real `claude` login belong outside this suite, for
example in a gitignored local `private/` scratch directory.
