# Examples

- **[Dashboard](dashboard.md)**: a terminal dashboard for watching and joining
  workers. It's a single file that uses only Python's standard library and the
  installed `ccx` command.
- **[controller_implement_review.py](controller_implement_review.py)**: a
  [controller](../docs/controllers.md) where Claude Code implements a change, a
  fresh Codex worker reviews it, and Claude fixes what it found. The two share
  the review through a [variable](../docs/variables/README.md).

Run the dashboard without cloning the repository:

```bash
uv run https://raw.githubusercontent.com/ccx-native/ccx/main/examples/dashboard.py
```

Or download `dashboard.py` and run `python3 dashboard.py` (Python 3.11+).
