# Contributing to ccx

ccx is maintained by one person, so the scope for outside pull requests is narrow.

**Welcome as pull requests:**

- Bug fixes, small and focused.
- Compatibility fixes for new Claude Code or Codex releases.
- Performance improvements that don't change behavior.
- Doc fixes: wrong commands, broken links, unclear steps.

**Not accepted as pull requests:**

- New features, commands or options.
- Refactors, renames and style-only changes.
- Changes to intended behavior or defaults.
- New runtime dependencies.

For anything in the second list, open an issue describing the problem instead.
If it fits the project, the maintainer may build it; pull requests for it are
closed without review.

Keep changes focused, dependency-free at runtime, and covered by the quota-free
test suite.

## Dev setup

```bash
git clone https://github.com/ccx-native/ccx
cd ccx
pip install -e .          # editable install: edits to ccx/ take effect immediately
python -m pip install pytest pytest-xdist
ccx --help
```

ccx needs Python 3.11+ and the `claude` or `codex` CLI, with no third-party Python
dependencies at runtime. `pytest` and `pytest-xdist` are for development.

## Core constraints

- **Stdlib only at runtime.** `pyproject.toml` keeps `dependencies = []`.
  Explain why standard-library alternatives aren't enough and get approval
  before adding a runtime dependency.
- **Python 3.11+.** CI runs the suite on 3.11 to 3.14.
- **Public surfaces only.** ccx drives Claude Code through its terminal, hooks
  and transcripts, and Codex through `codex app-server`. Prefer what those tools
  document; when ccx has to rely on undocumented behavior, keep it in one place
  and say so in a comment. [Architecture](docs/architecture.md) maps the code.

## Tests

```bash
python -m pytest -q tests/ -n 16 --dist worksteal        # full suite; tune -n to your cores
python -m pytest -q tests/test_parser.py                 # focused, serial by default
python -m compileall -q ccx examples ccx.py              # byte-compile check
ruff check .                                            # lint (rules in pyproject.toml; CI pins the version)
```

The public suite under `tests/` is **quota-free**: pure tests and disposable
fake-provider host/daemon integration tests do not require a Claude login or
live provider calls. See [`tests/README.md`](tests/README.md) for the coverage
map. Tune `-n` to about two-thirds of your logical CPU cores (fewer under
competing load); focused runs stay serial.

Add quota-free tests under `tests/`. Keep tests that require provider logins or
model turns out of that suite.

### When Codex changes its protocol

ccx pins the `codex app-server` schema it was written against. After upgrading
Codex, compare the new one:

```bash
codex app-server generate-json-schema --out /tmp/codex-schema
python scripts/check_codex_protocol.py --generated /tmp/codex-schema
```

## Docs

Docs are plain Markdown under `docs/` plus the top-level `README.md`, read on
GitHub. Each page covers one topic and ends with its known limitations. Check
links before sending doc changes (CI runs this too):

```bash
python3 scripts/check_doc_links.py
```

## Release hygiene

- Keep the version in sync across `pyproject.toml` and `package.json`
  (`test_package_metadata.py` enforces this).
- Note user-facing changes in [`CHANGELOG.md`](CHANGELOG.md) under
  `[Unreleased]`.
- Before tagging a release, run `pytest -q`, `ruff check .`,
  `python3 scripts/check_doc_links.py`, `npm pack --dry-run`, and the byte-compile check —
  the same checks CI runs (`.github/workflows/ci.yml`).
- Security issues: see [`SECURITY.md`](SECURITY.md).

## Pull requests

One fix per pull request. Describe the user-visible effect and how you tested it,
including the Claude Code or Codex version for compatibility fixes. CI must pass
before merge.

## Known limitations

The public test suite uses fake providers; it doesn't verify compatibility with
live Claude Code or Codex releases.
