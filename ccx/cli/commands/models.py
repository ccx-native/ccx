"""Read-only model catalog CLI."""

from __future__ import annotations

import json
import sys

from ccx.constants import EXIT_UNAVAILABLE
from ccx.models import read_models
from ccx.types import ModelCatalog


def _table(catalog: ModelCatalog) -> None:
    label = catalog.engine.capitalize()
    if catalog.engine_version:
        label += f" {catalog.engine_version}"
    print(f"{label} models ({catalog.source}):")
    if catalog.note:
        print(f"  {catalog.note}")
    models = sorted(catalog.models, key=lambda entry: (not entry.default, not entry.alias))
    if not models:
        print("  No models available")
        return
    rows = [
        (entry.id, entry.display_name,
         entry.resolved if entry.resolved and entry.resolved != entry.id else "",
         ", ".join(entry.efforts))
        for entry in models
    ]
    headers = ("--model", "Display name", "Resolved", "Efforts")
    widths = [max(len(row[i]) for row in [headers, *rows]) for i in range(3)]
    for row in [headers, *rows]:
        print("  " + "  ".join(
            [*(row[i].ljust(widths[i]) for i in range(3)), row[3]]
        ).rstrip())


def cmd_models(args) -> None:
    """Print one or both available model catalogs."""
    catalogs = read_models(engine=args.engine, refresh=args.refresh)
    if args.json:
        print(json.dumps([catalog.as_dict() for catalog in catalogs], indent=2))
    else:
        for i, catalog in enumerate(catalogs):
            if i:
                print()
            _table(catalog)
    if args.refresh:
        failed = [catalog for catalog in catalogs if catalog.source in ("stale", "builtin")]
        if failed:
            for catalog in failed:
                sys.stderr.write(f"ccx models: {catalog.engine} refresh failed: {catalog.note}\n")
            raise SystemExit(EXIT_UNAVAILABLE)
