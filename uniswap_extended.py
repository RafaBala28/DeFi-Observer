from typing import Any, Dict, List
import time

from web3 import Web3
from web3._utils.events import get_event_data

from abis import ERC20_ABI, UNISWAP_V3_POOL_ABI
from config import UNISWAP_V3_ETH_USDC_POOL
# from price_service import get_price_service  # Not needed
from web3_utils import get_web3, get_logs_chunked

SWAP_TOPIC = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()

_cache: Dict[str, Dict[str, Any]] = {}


def _to_decimal(value: int, decimals: int) -> float:
    try:
        return float(value) / (10 ** decimals)
    except Exception:
        return 0.0


def _price_from_sqrt(sqrt_price_x96: int, token0_dec: int, token1_dec: int) -> float:
    # price = (sqrtPriceX96^2 / 2^192) * 10^(dec0-dec1) gives token1/token0
    try:
        ratio = (sqrt_price_x96 ** 2) / (2 ** 192)
        scale = 10 ** (token0_dec - token1_dec)
        return float(ratio * scale)
    except Exception:
        return 0.0


def get_uniswap_extended() -> Dict[str, Any]:
    # Simple cache (30s)
    now = time.time()
    c = _cache.get("uni_ext")
    if c and (now - c.get("t", 0) < 30):
        return c["v"]

    w3 = get_web3(timeout=10, sticky=True)
    if not w3 or not w3.is_connected():
        return {"error": "web3_connection_failed"}

    pool = w3.eth.contract(address=UNISWAP_V3_ETH_USDC_POOL, abi=UNISWAP_V3_POOL_ABI)
    try:
        token0_addr = pool.functions.token0().call()
        token1_addr = pool.functions.token1().call()
        fee_tier = int(pool.functions.fee().call())
        tick_spacing = int(pool.functions.tickSpacing().call())
        slot0 = pool.functions.slot0().call()
        sqrt_price_x96 = int(slot0[0])
        curr_tick = int(slot0[1])
    except Exception as e:
        return {"error": f"pool_call_failed: {e}"}

    t0 = w3.eth.contract(address=token0_addr, abi=ERC20_ABI)
    t1 = w3.eth.contract(address=token1_addr, abi=ERC20_ABI)
    try:
        sym0 = t0.functions.symbol().call()
    except Exception:
        sym0 = "T0"
    try:
        sym1 = t1.functions.symbol().call()
    except Exception:
        sym1 = "T1"
    try:
        dec0 = int(t0.functions.decimals().call())
        dec1 = int(t1.functions.decimals().call())
    except Exception:
        dec0, dec1 = 6, 18

    # Balances in pool (TVL components)
    try:
        bal0 = int(t0.functions.balanceOf(UNISWAP_V3_ETH_USDC_POOL).call())
        bal1 = int(t1.functions.balanceOf(UNISWAP_V3_ETH_USDC_POOL).call())
    except Exception:
        bal0 = bal1 = 0

    amt0 = _to_decimal(bal0, dec0)
    amt1 = _to_decimal(bal1, dec1)

    # On-chain price (token1 per token0). For USDC/WETH, this is WETH per USDC; we want ETH/USD => invert if token0 is USDC
    p1_per_0 = _price_from_sqrt(sqrt_price_x96, dec0, dec1)
    price_service = get_price_service()
    onchain_price_eth_usd = None
    if sym0.upper() in ("USDC", "USDT") and sym1.upper() in ("WETH", "ETH"):
        onchain_price_eth_usd = 1.0 / max(p1_per_0, 1e-12)
        tvl_usd = amt0 + amt1 * onchain_price_eth_usd
    elif sym1.upper() in ("USDC", "USDT") and sym0.upper() in ("WETH", "ETH"):
        onchain_price_eth_usd = p1_per_0  # token1 is stable; token0 is WETH
        tvl_usd = amt1 + amt0 * onchain_price_eth_usd
    else:
        onchain_price_eth_usd = (1.0 / max(p1_per_0, 1e-12))
        tvl_usd = amt0 + amt1 * onchain_price_eth_usd

    price_eth_usd = price_service.get_token_price("WETH") or onchain_price_eth_usd
    tvl_usd = amt0 + amt1 * price_eth_usd if price_eth_usd else tvl_usd

    # 24h volume via Swap logs (approx by summing stable amounts)
    latest = w3.eth.block_number
    approx_blocks_24h = 7200  # ~12s/block
    from_block = max(0, latest - approx_blocks_24h)
    logs = get_logs_chunked(w3, UNISWAP_V3_ETH_USDC_POOL, [SWAP_TOPIC], from_block, latest)

    # Decode swaps
    event_abi = None
    for item in UNISWAP_V3_POOL_ABI:
        if item.get("type") == "event" and item.get("name") == "Swap":
            event_abi = item
            break
    volume_usd = 0.0
    if event_abi:
        for lg in logs:
            try:
                ev = get_event_data(w3.codec, event_abi, lg)
                a0 = int(ev["args"]["amount0"])  # can be negative
                a1 = int(ev["args"]["amount1"])  # can be negative
                # If token0 is stable (USDC), use abs(amount0); else convert amount1 using price
                if sym0.upper() in ("USDC", "USDT"):
                    volume_usd += abs(_to_decimal(a0, dec0))
                elif sym1.upper() in ("USDC", "USDT"):
                    volume_usd += abs(_to_decimal(a1, dec1))
                else:
                    # fallback: use amount0 with current price conversion
                    volume_usd += abs(_to_decimal(a0, dec0))
            except Exception:
                continue

    # Neighbor ticks
    ticks: List[Dict[str, Any]] = []
    for m in (-2, -1, 0, 1, 2):
        t = int((curr_tick // tick_spacing) * tick_spacing + m * tick_spacing)
        try:
            tick_data = pool.functions.ticks(t).call()
            ticks.append({
                "tick": t,
                "liquidityNet": int(tick_data[1]),
                "liquidityGross": int(tick_data[0]),
            })
        except Exception:
            ticks.append({"tick": t, "liquidityNet": 0, "liquidityGross": 0})

    result = {
        "protocol": "Uniswap V3 Extended",
        "pair": f"{sym1 if sym1.upper() in ('WETH','ETH') else sym0}/{sym0 if sym1.upper() in ('WETH','ETH') else sym1}",
        "fee_tier": fee_tier,
        "tvl_usd": tvl_usd,
        "volume_24h_usd": volume_usd,
        "price_eth_usd": price_eth_usd,
        "ticks": ticks,
    }

    _cache["uni_ext"] = {"t": now, "v": result}
    return result
