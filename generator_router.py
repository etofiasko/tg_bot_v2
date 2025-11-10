import sys
import importlib
from contextvars import ContextVar
from contextlib import contextmanager
from config import REPORT_GEN_PATHS
from typing import Literal


_active_alias: Literal["main", "alt"] = "main"

def _purge_document_gen_modules():
    for name in list(sys.modules.keys()):
        if name == "document_gen" or name.startswith("document_gen."):
            sys.modules.pop(name, None)

def activate_generator(alias: Literal["main", "alt"]) -> None:
    global _active_alias
    if alias not in REPORT_GEN_PATHS:
        raise ValueError("alias must be 'main' or 'alt'")

    target_path = REPORT_GEN_PATHS[alias]

    if not sys.path or sys.path[0] != target_path:
        try:
            sys.path.remove(target_path)
        except ValueError:
            pass
        sys.path.insert(0, target_path)

    _purge_document_gen_modules()
    importlib.invalidate_caches()

    importlib.import_module("document_gen.generator")

    _active_alias = alias

def get_generate_trade_document():
    mod = importlib.import_module("document_gen.generator")
    return mod.generate_trade_document

def debug_where_loaded() -> str:
    mod = importlib.import_module("document_gen.generator")
    return getattr(mod, "__file__", "<unknown>")