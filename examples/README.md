# Examples

The source checkout includes a terminal dashboard for ccx workers. It uses Python's
standard library and the installed `ccx` command.

Clone the repository and install it in editable mode for development:

```bash
git clone https://github.com/oneferdev/ccx
cd ccx
python3 -m pip install -e .
python3 examples/dashboard.py
```

The dashboard needs Python 3.11+ and can also print snapshots without an
interactive terminal.

See **[Dashboard](dashboard.md)** for controls, headless commands, JSON output, and limitations.
