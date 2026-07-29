from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_fetcher_module() -> ModuleType:
    """Load the focused crawler without importing the full TrendRadar app."""

    module_name = "_trendradar_xiaohongshu_fetcher"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    module_path = (
        Path(__file__).resolve().parents[1]
        / "trendradar"
        / "crawler"
        / "xiaohongshu.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load Xiaohongshu fetcher from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_FETCHER_MODULE = _load_fetcher_module()
XiaohongshuFetcher = _FETCHER_MODULE.XiaohongshuFetcher
XiaohongshuRiskControlError = _FETCHER_MODULE.XiaohongshuRiskControlError
XiaohongshuSessionError = _FETCHER_MODULE.XiaohongshuSessionError
load_spider_xhs_login_api = _FETCHER_MODULE.load_spider_xhs_login_api
spider_xhs_runtime = _FETCHER_MODULE.spider_xhs_runtime
