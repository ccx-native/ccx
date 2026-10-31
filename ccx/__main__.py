from __future__ import annotations

if __package__:
    from ccx.cli.main import main
else:
    # Hosts and hooks may re-exec this file directly after the parent command
    # was invoked as `python -m ccx`. In direct-script mode Python does not know
    # the package parent, so add it before importing ccx.cli absolutely.
    #
    # Do not import pathlib here before fixing sys.path: when this file is
    # executed directly, sys.path[0] is the package directory, so stdlib imports
    # of `types` can accidentally resolve to ccx/types.py.
    import sys

    normalized = __file__.replace("\\", "/")
    package_dir = normalized.rsplit("/", 1)[0]
    root = normalized.rsplit("/", 2)[0]
    # Direct script execution sets sys.path[0] to the package directory. That
    # makes top-level stdlib imports like `types` resolve to ccx/types.py. Drop
    # that entry and import ccx as a package from its parent instead.
    sys.path = [p for p in sys.path if p.replace("\\", "/") != package_dir]
    if root not in sys.path:
        sys.path.insert(0, root)
    from ccx.cli.main import main


if __name__ == "__main__":
    main()
