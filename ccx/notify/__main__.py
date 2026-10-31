"""Run the notification daemon: ``python -m ccx.notify``."""
from ccx.notify.daemon import run

if __name__ == "__main__":
    raise SystemExit(run())
