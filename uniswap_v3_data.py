# ============================================================
# Uniswap V3 - Selected Pools snapshot (TVL via pool balances)
# ------------------------------------------------------------
# Requirements:
#   pip install web3 requests
# ============================================================

from web3 import Web3
import time

# Import shared utilities
from config import Tokens, STABLECOINS, get_chain_config
from abis import UNISWAP_V3_FACTORY_ABI, UNISWAP_V3_POOL_ABI, ERC20_ABI
from web3_utils import get_web3
from aave_data import get_price_service

# Token mapping for this module
TOKENS = {
    "ETH": {"address": Tokens.WETH, "coingecko": "ethereum"},
    "USDC": {"address": Tokens.USDC, "coingecko": "usd-coin"},
    "USDT": {"address": Tokens.USDT, "coingecko": "tether"},
    "WBTC": {"address": Tokens.WBTC, "coingecko": "wrapped-bitcoin"},
}

PAIRS = [
    ("USDC", "USDT"),
    ("ETH", "USDT"),
    ("WBTC", "ETH"),
    ("WBTC", "USDC"),
]

# Try popular fee tiers (0.05%, 0.3%, 1%)
FEE_CANDIDATES = [500, 3000, 10000]


def _get_prices(symbols):
    """Get prices using shared price service"""
    price_service = get_price_service()
    return price_service.get_multiple_prices(symbols)


def _get_pool_for_pair(w3: Web3, factory_addr: str, a: str, b: str):
    factory = w3.eth.contract(address=factory_addr, abi=UNISWAP_V3_FACTORY_ABI)
    # Try fee tiers in priority order and take first pool with non-zero code
    for fee in FEE_CANDIDATES:
        try:
            pool = factory.functions.getPool(a, b, fee).call()
            if int(pool, 16) == 0:
                continue
            code = w3.eth.get_code(pool)
            if code and len(code) > 0:
                return Web3.to_checksum_address(pool), fee
        except Exception:
            continue
    return None, None


def _erc20_info(w3: Web3, token_addr: str):
    c = w3.eth.contract(address=token_addr, abi=ERC20_ABI)
    try:
        decimals = c.functions.decimals().call()
    except Exception:
        decimals = 18
    try:
        symbol = c.functions.symbol().call()
    except Exception:
        symbol = "?"
    return c, decimals, symbol


def _pool_balances(w3: Web3, pool_addr: str, t0: str, t1: str):
    c0, d0, s0 = _erc20_info(w3, t0)
    c1, d1, s1 = _erc20_info(w3, t1)
    b0 = c0.functions.balanceOf(pool_addr).call() / (10 ** d0)
    b1 = c1.functions.balanceOf(pool_addr).call() / (10 ** d1)
    return {"token": t0, "symbol": s0, "decimals": d0, "amount": b0}, {"token": t1, "symbol": s1, "decimals": d1, "amount": b1}


def get_uniswap_v3_pools(chain_name: str | None = None, *, force_new: bool = False):
    try:
        # Use shared Web3 connection
        w3 = get_web3(timeout=10, chain_name=chain_name, sticky=not force_new, force_new=force_new)

        if not w3 or not w3.is_connected():
            return {"error": "Blockchain connection failed"}

        chain_cfg = get_chain_config(chain_name)
        factory_addr = chain_cfg.get("uniswap_v3_factory")
        if not factory_addr:
            return {"error": "Uniswap V3 is not configured for this chain"}

        # Prepare prices using shared service
        needed_syms = {s for pair in PAIRS for s in pair}
        prices = _get_prices(needed_syms)

        pools = []
        seen_pools = set()  # Track pool addresses to avoid duplicates
        
        for symA, symB in PAIRS:
            addrA = TOKENS[symA]["address"]
            addrB = TOKENS[symB]["address"]
            pool_addr, fee = _get_pool_for_pair(w3, factory_addr, addrA, addrB)
            if not pool_addr:
                pools.append({
                    "pair": f"{symA}/{symB}",
                    "error": "Pool nicht gefunden",
                })
                continue
            
            # Skip if we've already added this pool
            if pool_addr in seen_pools:
                continue
            seen_pools.add(pool_addr)

            pool = w3.eth.contract(address=pool_addr, abi=UNISWAP_V3_POOL_ABI)
            try:
                t0 = pool.functions.token0().call()
                t1 = pool.functions.token1().call()
            except Exception:
                pools.append({"pair": f"{symA}/{symB}", "pool_address": pool_addr, "error": "Pool call fehlgeschlagen"})
                continue

            info0, info1 = _pool_balances(w3, pool_addr, t0, t1)

            p0 = 0.0
            p1 = 0.0
            # Map to configured symbols for price
            for sym, cfg in TOKENS.items():
                if Web3.to_checksum_address(cfg["address"]) == Web3.to_checksum_address(info0["token"]):
                    p0 = prices.get(sym, 0.0)
                if Web3.to_checksum_address(cfg["address"]) == Web3.to_checksum_address(info1["token"]):
                    p1 = prices.get(sym, 0.0)

            # Calculate pool price display
            # For stablecoin pairs, show the ratio close to 1.0
            # For ETH/stablecoin, show ETH price in USD
            # For BTC/ETH, show BTC price in ETH
            pool_price = None
            if p0 > 0 and p1 > 0:
                # Determine which way to show the price
                sym0 = info0["symbol"]
                sym1 = info1["symbol"]
                
                # If token1 is a stablecoin (USDC, USDT, DAI), show price of token0 in USD
                if sym1 in STABLECOINS:
                    price = p0  # USD price of token0
                    pool_price = f"${price:,.2f}"
                # If token0 is a stablecoin, show price of token1 in USD  
                elif sym0 in STABLECOINS:
                    price = p1  # USD price of token1
                    pool_price = f"${price:,.2f}"
                # Otherwise show ratio
                else:
                    price = p0 / p1
                    pool_price = f"{price:.4f}"

            v0 = info0["amount"] * p0
            v1 = info1["amount"] * p1
            tvl = v0 + v1
            
            # Build pair name from actual tokens in pool
            actual_pair = f"{info0['symbol']}/{info1['symbol']}"
            
            # Skip ETH/USDC and USDC/ETH pools explicitly
            if actual_pair in ["ETH/USDC", "USDC/ETH", "WETH/USDC", "USDC/WETH"]:
                continue

            pools.append({
                "pair": actual_pair,
                "pool_address": pool_addr,
                "fee": fee,
                "pool_price": pool_price,
                "token0": {"symbol": info0["symbol"], "amount": info0["amount"], "price_usd": p0, "value_usd": v0},
                "token1": {"symbol": info1["symbol"], "amount": info1["amount"], "price_usd": p1, "value_usd": v1},
                "tvl_usd": tvl,
            })
        # Sort pools by tvl desc where available
        pools.sort(key=lambda x: x.get("tvl_usd", 0), reverse=True)
        return {"protocol": "Uniswap V3", "pools": pools}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import json
    print(json.dumps(get_uniswap_v3_pools(), indent=2))
