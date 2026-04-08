"""One-off Databricks runner for catalog and schema setup."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_setup_module():
    module_path = Path(__file__).with_name("00_setup_catalogs.py")
    spec = importlib.util.spec_from_file_location("setup_catalogs", module_path)
    module = importlib.util.module_from_spec(spec)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    spec.loader.exec_module(module)
    return module


def main() -> None:
    setup_module = _load_setup_module()
    setup_module.main(spark)


if __name__ == "__main__":
    main()
