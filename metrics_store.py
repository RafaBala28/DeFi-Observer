import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# Simple JSON-backed history store with pruning.
# Files live under ./data to keep things tidy next to the app.

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")
UNISWAP_HISTORY_FILE = os.path.join(DATA_DIR, "uniswap_history.json")
AAVE_HISTORY_FILE = os.path.join(DATA_DIR, "aave_history.json")

# Retention and safety limits
DEFAULT_RETENTION_SECONDS = 7 * 24 * 3600  # keep one week by default
MAX_POINTS_PER_FILE = 5000  # hard cap to avoid runaway size


def _ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)


def _read_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # Corrupt file; rename and start fresh to avoid breaking the app
        try:
            os.replace(path, path + ".bad")
        except Exception:
            pass
        return None


def _write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _now_ts() -> float:
    return time.time()


def _parse_window(window: Optional[str]) -> int:
    # Accept forms like "1h", "24h", "7d", "30m". Default 24h.
    if not window:
        return 24 * 3600
    try:
        unit = window[-1].lower()
        value = float(window[:-1])
        if unit == "m":
            return int(value * 60)
        if unit == "h":
            return int(value * 3600)
        if unit == "d":
            return int(value * 24 * 3600)
        # Fallback assume seconds
        return int(float(window))
    except Exception:
        return 24 * 3600


# -------- Uniswap --------

def append_uniswap_point(point: Dict[str, Any]) -> None:
    """
    point must include at least:
      - t (unix seconds float)
      - tvl_usd, eth_price, eth_reserve, usdc_reserve
    """
    _ensure_dirs()
    data = _read_json(UNISWAP_HISTORY_FILE)
    if not isinstance(data, list):
        data = []
    data.append(point)

    # prune by time and hard cap
    cutoff = _now_ts() - DEFAULT_RETENTION_SECONDS
    data = [p for p in data if isinstance(p, dict) and p.get("t", 0) >= cutoff]
    if len(data) > MAX_POINTS_PER_FILE:
        data = data[-MAX_POINTS_PER_FILE:]

    _write_json(UNISWAP_HISTORY_FILE, data)


def get_uniswap_series(window: Optional[str]) -> List[Dict[str, Any]]:
    window_s = _parse_window(window)
    data = _read_json(UNISWAP_HISTORY_FILE)
    if not isinstance(data, list):
        return []
    cutoff = _now_ts() - window_s
    return [p for p in data if isinstance(p, dict) and p.get("t", 0) >= cutoff]


# -------- Aave --------

def append_aave_snapshot(snapshot: Dict[str, Any]) -> None:
    """
    snapshot shape:
      {
        "t": unix seconds,
        "assets": [
           {"symbol": "WETH", "deposit_apy": float, "borrow_apy": float,
            "utilization": float, "liquidity_usd": float, "borrowed_usd": float,
            "price_usd": float}
        ]
      }
    """
    _ensure_dirs()
    data = _read_json(AAVE_HISTORY_FILE)
    if not isinstance(data, list):
        data = []
    data.append(snapshot)

    cutoff = _now_ts() - DEFAULT_RETENTION_SECONDS
    data = [s for s in data if isinstance(s, dict) and s.get("t", 0) >= cutoff]
    if len(data) > MAX_POINTS_PER_FILE:
        data = data[-MAX_POINTS_PER_FILE:]

    _write_json(AAVE_HISTORY_FILE, data)


def get_aave_series(asset_symbol: str, window: Optional[str]) -> List[Dict[str, Any]]:
    window_s = _parse_window(window)
    data = _read_json(AAVE_HISTORY_FILE)
    if not isinstance(data, list):
        return []
    cutoff = _now_ts() - window_s
    out: List[Dict[str, Any]] = []
    for snap in data:
        try:
            if not isinstance(snap, dict):
                continue
            t = snap.get("t")
            if t is None or t < cutoff:
                continue
            for a in snap.get("assets", []):
                if not isinstance(a, dict):
                    continue
                if a.get("symbol") == asset_symbol:
                    out.append({
                        "t": t,
                        "deposit_apy": a.get("deposit_apy"),
                        "borrow_apy": a.get("borrow_apy"),
                        "utilization": a.get("utilization"),
                        "liquidity_usd": a.get("liquidity_usd"),
                        "borrowed_usd": a.get("borrowed_usd"),
                        "price_usd": a.get("price_usd"),
                    })
        except Exception:
            continue
    return out
