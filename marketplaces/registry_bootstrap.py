# -*- coding: utf-8 -*-
from .registry import register
from .shopee_danfe import ShopeeDanfeDriver
from .tiktok_shop import TikTokShopDriver
from .temu import TemuDriver
from .generic_fallback import GenericFallbackDriver

_BOOTSTRAPPED = False


def bootstrap_drivers():
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    register(ShopeeDanfeDriver())
    register(TikTokShopDriver())
    register(TemuDriver())
    register(GenericFallbackDriver())
    _BOOTSTRAPPED = True
