# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Optional
from .base import MarketplaceDriver, DetectedDoc

_DRIVERS: List[MarketplaceDriver] = []


def register(driver: MarketplaceDriver) -> None:
    _DRIVERS.append(driver)


def drivers() -> List[MarketplaceDriver]:
    return list(_DRIVERS)


def detect_best(pdf_path: str) -> Optional[DetectedDoc]:
    best = None
    for d in _DRIVERS:
        try:
            r = d.detect(pdf_path)
            if not r:
                continue
            if (best is None) or (r.confidence > best.confidence):
                best = r
        except Exception:
            continue
    return best


def get_driver_by_kind(kind: str) -> Optional[MarketplaceDriver]:
    for d in _DRIVERS:
        if getattr(d, "kind", None) == kind or getattr(d, "name", None) == kind:
            return d
    return None
