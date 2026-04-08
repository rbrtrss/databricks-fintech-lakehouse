"""Utilities for Databricks job runner notebooks."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def load_notebook_module(current_file: str, target_file_name: str, module_name: str) -> Any:
    module_path = Path(current_file).with_name(target_file_name)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_widget_value(name: str, default_value: str) -> str:
    try:
        dbutils.widgets.text(name, default_value)
        return dbutils.widgets.get(name)
    except NameError as error:
        raise RuntimeError(
            "This runner is intended for Databricks notebook execution where dbutils is available."
        ) from error
