from typing import Any, Dict, List
from web3 import Web3
import statistics
import time

from web3_utils import get_web3

_cache: Dict[str, Dict[str, Any]] = {}


def get_eth_network_stats() -> Dict[str, Any]:
    now = time.time()
    c = _cache.get("eth_stats")
    if c and (now - c.get("t", 0) < 15):
        return c["v"]

    w3 = get_web3(timeout=10, sticky=True)
    if not w3 or not w3.is_connected():
        return {"error": "web3_connection_failed"}

    latest = w3.eth.block_number
    # get last 10 blocks timestamps
    blocks: List[Dict[str, Any]] = []
    for n in range(max(0, latest - 10), latest + 1):
        try:
            blk = w3.eth.get_block(n)
            blocks.append({"number": blk.number, "ts": blk.timestamp})
        except Exception:
            continue

    avg_block_time = None
    if len(blocks) >= 2:
        blocks_sorted = sorted(blocks, key=lambda x: x["number"])  # ensure ascending
        deltas = [blocks_sorted[i+1]["ts"] - blocks_sorted[i]["ts"] for i in range(len(blocks_sorted)-1)]
        # guard against zero/negative deltas (reorgs, clock skew)
        deltas = [d for d in deltas if d > 0]
        avg_block_time = statistics.mean(deltas) if deltas else None

    gas_price_wei = None
    try:
        gas_price_wei = w3.eth.gas_price
    except Exception:
        gas_price_wei = None

    base_fees = []
    try:
        # last 10 blocks fee history
        hist = w3.eth.fee_history(10, "latest", [10, 50, 90])
        # baseFeePerGas as array (hex) or int depending on provider wrapper; normalize to int
        bfp = hist.get("baseFeePerGas", [])
        for v in bfp:
            try:
                base_fees.append(int(v))
            except Exception:
                # some providers return HexBytes
                base_fees.append(int(v, 16) if isinstance(v, str) else None)
        base_fees = [x for x in base_fees if isinstance(x, int)]
    except Exception:
        base_fees = []

    result = {
        "latest_block": int(latest),
        "avg_block_time_sec": round(avg_block_time, 2) if avg_block_time else None,
        "gas_price_gwei": round(gas_price_wei / 1e9, 2) if gas_price_wei else None,
        "avg_base_fee_gwei": round(statistics.mean(base_fees) / 1e9, 2) if base_fees else None,
        "sample_size": len(blocks),
    }

    _cache["eth_stats"] = {"t": now, "v": result}
    return result
