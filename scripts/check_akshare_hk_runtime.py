"""Validate AKShare HK runtime dependencies inside Docker.

This intentionally avoids network calls. It only checks that the V8 native
library used by AKShare's HK daily endpoint can be loaded in the image.
"""

from py_mini_racer import py_mini_racer
import akshare as ak


ctx = py_mini_racer.MiniRacer()
result = ctx.eval("1 + 1")
if result != 2:
    raise RuntimeError(f"py_mini_racer returned unexpected result: {result!r}")

if not hasattr(ak, "stock_hk_daily"):
    raise RuntimeError("AKShare does not expose stock_hk_daily")

print("AKShare HK runtime check passed")
