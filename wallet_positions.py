from typing import Any, Dict, List, Optional, Tuple
from web3 import Web3
import time
import math
import logging

logger = logging.getLogger(__name__)

# Import shared utilities
from config import (
    Tokens, AAVE_V3_POOL, UNISWAP_V2_ETH_USDC_PAIR,
    UNISWAP_V3_FACTORY, UNISWAP_V3_NFPM, TOKEN_SYMBOLS
)
from abis import (
    ERC20_ABI, UNISWAP_V2_PAIR_ABI,
    UNISWAP_V3_FACTORY_ABI, UNISWAP_V3_POOL_ABI, AAVE_V3_POOL_ABI
)
from web3_utils import get_web3, get_logs_chunked
from aave_data import get_price_service

# Keep local references for backward compatibility
WETH = Tokens.WETH
USDC = Tokens.USDC
USDT = Tokens.USDT
DAI = Tokens.DAI
WBTC = Tokens.WBTC
NFPM_ADDRESS = UNISWAP_V3_NFPM

# Uniswap V3 Position Manager Address (same as NFPM)
UNISWAP_V3_POSITION_MANAGER = UNISWAP_V3_NFPM

# Token Decimals Cache
TOKEN_DECIMALS_CACHE = {
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": 18,  # WETH
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": 6,   # USDC
    "0xdAC17F958D2ee523a2206206994597C13D831ec7": 6,   # USDT
    "0x6B175474E89094C44Da98b954EedeAC495271d0F": 18,  # DAI
    "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599": 8,   # WBTC
}

# Local ABI for NFPM (not in shared abis.py)
NFPM_ABI = [
    {"inputs": [{"internalType": "address", "name": "owner", "type": "address"}], "name": "balanceOf", "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "address", "name": "owner", "type": "address"}, {"internalType": "uint256", "name": "index", "type": "uint256"}], "name": "tokenOfOwnerByIndex", "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}], "name": "ownerOf", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}], "name": "positions", "outputs": [
        {"internalType": "uint96", "name": "nonce", "type": "uint96"},
        {"internalType": "address", "name": "operator", "type": "address"},
        {"internalType": "address", "name": "token0", "type": "address"},
        {"internalType": "address", "name": "token1", "type": "address"},
        {"internalType": "uint24", "name": "fee", "type": "uint24"},
        {"internalType": "int24", "name": "tickLower", "type": "int24"},
        {"internalType": "int24", "name": "tickUpper", "type": "int24"},
        {"internalType": "uint128", "name": "liquidity", "type": "uint128"},
        {"internalType": "uint256", "name": "feeGrowthInside0LastX128", "type": "uint256"},
        {"internalType": "uint256", "name": "feeGrowthInside1LastX128", "type": "uint256"},
        {"internalType": "uint128", "name": "tokensOwed0", "type": "uint128"},
        {"internalType": "uint128", "name": "tokensOwed1", "type": "uint128"}
    ], "stateMutability": "view", "type": "function"},
    {"anonymous": False, "inputs": [
        {"indexed": True, "internalType": "address", "name": "from", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "to", "type": "address"},
        {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"}
    ], "name": "Transfer", "type": "event"},
    {"anonymous": False, "inputs": [
        {"indexed": False, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        {"indexed": False, "internalType": "uint128", "name": "liquidity", "type": "uint128"},
        {"indexed": False, "internalType": "uint256", "name": "amount0", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "amount1", "type": "uint256"}
    ], "name": "IncreaseLiquidity", "type": "event"},
]

TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

# Additional V3 Pool ABI for position analysis
POOL_SLOT0_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24", "name": "tick", "type": "int24"},
            {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
            {"internalType": "bool", "name": "unlocked", "type": "bool"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"internalType": "uint128", "name": "", "type": "uint128"}],
        "stateMutability": "view",
        "type": "function"
    }
]


def _connect() -> Optional[Web3]:
    """Get Web3 connection using shared utility"""
    return get_web3(timeout=12, sticky=True)


def _get_eth_price() -> float:
    """Get ETH price using shared price service"""
    price_service = get_price_service()
    return price_service.get_eth_price()


def _get_simple_price(symbol: str) -> float:
    """Get token price using shared price service"""
    s = (symbol or '').upper()
    price_service = get_price_service()
    return price_service.get_token_price(s)


# ============================================
# V3 Position Analysis Helper Functions
# ============================================

def get_token_info(w3: Web3, token_address: str) -> Dict:
    """Get token symbol and decimals"""
    try:
        token_address = Web3.to_checksum_address(token_address)
        
        # Check decimals cache first
        if token_address in TOKEN_DECIMALS_CACHE:
            decimals = TOKEN_DECIMALS_CACHE[token_address]
        else:
            token_contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
            try:
                decimals = token_contract.functions.decimals().call()
            except Exception:
                decimals = 18
        
        # Get symbol from config or contract
        symbol = TOKEN_SYMBOLS.get(token_address, None)
        if not symbol:
            token_contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
            try:
                symbol = token_contract.functions.symbol().call()
            except Exception:
                symbol = "UNKNOWN"
        
        return {
            "address": token_address,
            "symbol": symbol,
            "decimals": decimals
        }
    except Exception as e:
        return {
            "address": token_address,
            "symbol": "UNKNOWN",
            "decimals": 18
        }


def tick_to_price(tick: int, decimals0: int, decimals1: int) -> float:
    """Convert tick to human-readable price"""
    try:
        return (1.0001 ** tick) * (10 ** (decimals0 - decimals1))
    except Exception:
        return 0


def sqrt_price_x96_to_price(sqrt_price_x96: int, decimals0: int, decimals1: int) -> float:
    """Convert sqrtPriceX96 to human-readable price"""
    try:
        Q96 = 2 ** 96
        price = (sqrt_price_x96 / Q96) ** 2
        return price * (10 ** (decimals0 - decimals1))
    except Exception:
        return 0


def get_token_amounts_from_liquidity(
    liquidity: int,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    decimals0: int,
    decimals1: int
) -> Tuple[float, float]:
    """Calculate token amounts from liquidity and price range"""
    try:
        Q96 = 2 ** 96
        sqrt_price = sqrt_price_x96 / Q96
        sqrt_price_lower = 1.0001 ** (tick_lower / 2)
        sqrt_price_upper = 1.0001 ** (tick_upper / 2)
        
        if sqrt_price <= sqrt_price_lower:
            # Position entirely in token0
            amount0 = liquidity * (1 / sqrt_price_lower - 1 / sqrt_price_upper)
            amount1 = 0
        elif sqrt_price >= sqrt_price_upper:
            # Position entirely in token1
            amount0 = 0
            amount1 = liquidity * (sqrt_price_upper - sqrt_price_lower)
        else:
            # Position in range
            amount0 = liquidity * (1 / sqrt_price - 1 / sqrt_price_upper)
            amount1 = liquidity * (sqrt_price - sqrt_price_lower)
        
        return (
            amount0 / (10 ** decimals0),
            amount1 / (10 ** decimals1)
        )
    except Exception:
        return (0, 0)


def get_token_price_usd(token_address: str, symbol: str) -> float:
    """Get token price in USD using price service"""
    try:
        price_service = get_price_service()
        return price_service.get_token_price(symbol.upper())
    except Exception:
        return 0


# ============================================
# Main V3 Position Analysis Functions
# ============================================

def analyze_v3_position(w3: Web3, position_id: int) -> Dict:
    """
    Complete analysis of a Uniswap V3 position NFT
    
    Returns:
    - Token pair info
    - Liquidity range (ticks)
    - Current price vs range
    - Position value in USD
    - Unclaimed fees
    - In-range status
    """
    try:
        position_manager = w3.eth.contract(
            address=Web3.to_checksum_address(UNISWAP_V3_POSITION_MANAGER),
            abi=NFPM_ABI
        )
        
        # Get position data
        position_data = position_manager.functions.positions(position_id).call()
        
        (
            nonce, operator, token0_addr, token1_addr, fee,
            tick_lower, tick_upper, liquidity,
            fee_growth_inside_0, fee_growth_inside_1,
            tokens_owed_0, tokens_owed_1
        ) = position_data
        
        # Get token info
        token0 = get_token_info(w3, token0_addr)
        token1 = get_token_info(w3, token1_addr)
        
        # Get pool address
        factory = w3.eth.contract(
            address=Web3.to_checksum_address(UNISWAP_V3_FACTORY),
            abi=UNISWAP_V3_FACTORY_ABI
        )
        pool_address = factory.functions.getPool(
            Web3.to_checksum_address(token0_addr),
            Web3.to_checksum_address(token1_addr),
            fee
        ).call()
        
        if pool_address == "0x0000000000000000000000000000000000000000":
            return {"error": "Pool not found", "success": False, "position_id": position_id}
        
        # Get current pool state
        pool = w3.eth.contract(address=pool_address, abi=POOL_SLOT0_ABI)
        slot0 = pool.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        current_tick = slot0[1]
        pool_liquidity = pool.functions.liquidity().call()
        
        # Calculate prices
        current_price = sqrt_price_x96_to_price(
            sqrt_price_x96,
            token0["decimals"],
            token1["decimals"]
        )
        price_lower = tick_to_price(tick_lower, token0["decimals"], token1["decimals"])
        price_upper = tick_to_price(tick_upper, token0["decimals"], token1["decimals"])
        
        # Calculate token amounts
        amount0, amount1 = get_token_amounts_from_liquidity(
            liquidity,
            sqrt_price_x96,
            tick_lower,
            tick_upper,
            token0["decimals"],
            token1["decimals"]
        )
        
        # Get token prices in USD
        price0_usd = get_token_price_usd(token0_addr, token0["symbol"])
        price1_usd = get_token_price_usd(token1_addr, token1["symbol"])
        
        # Calculate position value
        value_usd = (amount0 * price0_usd) + (amount1 * price1_usd)
        
        # Unclaimed fees
        unclaimed_fee0 = tokens_owed_0 / (10 ** token0["decimals"])
        unclaimed_fee1 = tokens_owed_1 / (10 ** token1["decimals"])
        unclaimed_fees_usd = (unclaimed_fee0 * price0_usd) + (unclaimed_fee1 * price1_usd)
        
        # In-range check
        in_range = tick_lower <= current_tick <= tick_upper
        
        # Liquidity share (of total pool liquidity)
        liquidity_share = (liquidity / pool_liquidity * 100) if pool_liquidity > 0 else 0
        
        return {
            "success": True,
            "position_id": position_id,
            "pool_address": pool_address,
            "token0": token0,
            "token1": token1,
            "fee_tier": fee / 10000,  # Convert to percentage
            "liquidity": {
                "amount": liquidity,
                "share_of_pool": round(liquidity_share, 4)
            },
            "price_range": {
                "lower": round(price_lower, 6),
                "upper": round(price_upper, 6),
                "current": round(current_price, 6)
            },
            "tick_range": {
                "lower": tick_lower,
                "upper": tick_upper,
                "current": current_tick
            },
            "amounts": {
                "token0": round(amount0, 6),
                "token1": round(amount1, 6)
            },
            "value_usd": {
                "total": round(value_usd, 2),
                "token0": round(amount0 * price0_usd, 2),
                "token1": round(amount1 * price1_usd, 2)
            },
            "unclaimed_fees": {
                "token0": round(unclaimed_fee0, 6),
                "token1": round(unclaimed_fee1, 6),
                "usd": round(unclaimed_fees_usd, 2)
            },
            "status": {
                "in_range": in_range,
                "active": liquidity > 0,
                "health": "healthy" if in_range and liquidity > 0 else "out_of_range" if liquidity > 0 else "inactive"
            },
            "timestamp": int(time.time())
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "position_id": position_id
        }


def get_wallet_v3_positions(w3: Web3, wallet_address: str) -> List[Dict]:
    """Get all Uniswap V3 positions for a wallet with full analysis"""
    try:
        wallet_address = Web3.to_checksum_address(wallet_address)
        position_manager = w3.eth.contract(
            address=Web3.to_checksum_address(UNISWAP_V3_POSITION_MANAGER),
            abi=NFPM_ABI
        )
        
        # Get number of positions
        balance = position_manager.functions.balanceOf(wallet_address).call()
        
        if balance == 0:
            return []
        
        # Get all position IDs and analyze each
        positions = []
        for i in range(balance):
            position_id = position_manager.functions.tokenOfOwnerByIndex(wallet_address, i).call()
            position_analysis = analyze_v3_position(w3, position_id)
            if position_analysis.get("success"):
                positions.append(position_analysis)
        
        return positions
    
    except Exception as e:
        return [{"error": str(e)}]


def analyze_wallet_positions(wallet_address: str) -> Dict:
    """
    Complete analysis of all Uniswap V3 positions for a wallet
    
    Returns:
    - Summary (total value, active/inactive positions)
    - Detailed position data for each V3 NFT
    - Aggregated statistics
    """
    w3 = get_web3()
    if not w3:
        return {"error": "Web3 connection failed", "success": False}
    
    try:
        wallet_address = Web3.to_checksum_address(wallet_address)
        
        # Get V3 positions with full analysis
        v3_positions = get_wallet_v3_positions(w3, wallet_address)
        
        # Calculate summary
        total_value_usd = sum(p.get("value_usd", {}).get("total", 0) for p in v3_positions if p.get("success"))
        total_unclaimed_fees = sum(p.get("unclaimed_fees", {}).get("usd", 0) for p in v3_positions if p.get("success"))
        active_positions = sum(1 for p in v3_positions if p.get("status", {}).get("active", False))
        in_range_positions = sum(1 for p in v3_positions if p.get("status", {}).get("in_range", False))
        
        return {
            "success": True,
            "wallet": wallet_address,
            "summary": {
                "total_positions": len(v3_positions),
                "active_positions": active_positions,
                "in_range_positions": in_range_positions,
                "total_value_usd": round(total_value_usd, 2),
                "total_unclaimed_fees_usd": round(total_unclaimed_fees, 2)
            },
            "positions": v3_positions,
            "timestamp": int(time.time())
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "wallet": wallet_address
        }


# ============================================
# Internal Helper Functions
# ============================================

def _erc20_info(w3: Web3, token_addr: str):
    c = w3.eth.contract(address=token_addr, abi=ERC20_ABI)
    try:
        decimals = c.functions.decimals().call()
    except Exception:
        decimals = 18
    try:
        symbol = c.functions.symbol().call()
    except Exception:
        # common known symbols
        if Web3.to_checksum_address(token_addr) == WETH:
            symbol = "WETH"
        elif Web3.to_checksum_address(token_addr) == USDC:
            symbol = "USDC"
        else:
            symbol = "?"
    return c, decimals, symbol


def _get_pool_address(w3: Web3, token0: str, token1: str, fee: int) -> Optional[str]:
    """Get Uniswap V3 pool address from factory"""
    try:
        factory = w3.eth.contract(address=UNISWAP_V3_FACTORY, abi=UNISWAP_V3_FACTORY_ABI)
        pool = factory.functions.getPool(token0, token1, fee).call()
        if pool == "0x0000000000000000000000000000000000000000":
            return None
        return pool
    except Exception as e:
        logger.warning(f"⚠️ Error getting pool address: {e}")
        return None


def _calculate_v3_amounts(liquidity: int, sqrt_price_x96: int, tick_lower: int, tick_upper: int, decimals0: int, decimals1: int) -> Dict[str, float]:
    """Berechnet Token0/Token1 Mengen aus Liquidity basierend auf aktuellem Preis."""
    try:
        Q96 = 2 ** 96
        current_sqrt_price = sqrt_price_x96 / Q96
        sqrt_price_lower = 1.0001 ** (tick_lower / 2)
        sqrt_price_upper = 1.0001 ** (tick_upper / 2)
        
        if current_sqrt_price <= sqrt_price_lower:
            # Position komplett in Token0
            amount0 = liquidity * (1/sqrt_price_lower - 1/sqrt_price_upper)
            amount1 = 0
        elif current_sqrt_price >= sqrt_price_upper:
            # Position komplett in Token1  
            amount0 = 0
            amount1 = liquidity * (sqrt_price_upper - sqrt_price_lower)
        else:
            # Position in Range
            amount0 = liquidity * (1/current_sqrt_price - 1/sqrt_price_upper)
            amount1 = liquidity * (current_sqrt_price - sqrt_price_lower)
        
        return {
            "amount0": amount0 / (10 ** decimals0),
            "amount1": amount1 / (10 ** decimals1),
            "price_token0_in_token1": (current_sqrt_price ** 2) if current_sqrt_price > 0 else 0
        }
    except Exception:
        return {"amount0": 0, "amount1": 0, "price_token0_in_token1": 0}


def _get_aave_user_positions(w3: Web3, owner: str) -> List[Dict[str, Any]]:
    pool = w3.eth.contract(address=AAVE_V3_POOL, abi=AAVE_V3_POOL_ABI)
    try:
        reserves: List[str] = pool.functions.getReservesList().call()
    except Exception:
        # fallback to common assets
        reserves = [WETH, USDC, USDT, DAI, WBTC]

    positions: List[Dict[str, Any]] = []
    for asset in reserves:
        try:
            rd = pool.functions.getReserveData(asset).call()
            a_token = rd[8]
            s_debt = rd[9]
            v_debt = rd[10]

            # decimals from underlying asset
            _, dec, sym = _erc20_info(w3, Web3.to_checksum_address(asset))
            at = w3.eth.contract(address=a_token, abi=ERC20_ABI)
            sd = w3.eth.contract(address=s_debt, abi=ERC20_ABI)
            vd = w3.eth.contract(address=v_debt, abi=ERC20_ABI)

            dep = float(at.functions.balanceOf(owner).call()) / (10 ** dec)
            if dep == 0:
                # if no deposit and also no debt, skip early to reduce calls
                st = float(sd.functions.balanceOf(owner).call()) / (10 ** dec)
                vr = float(vd.functions.balanceOf(owner).call()) / (10 ** dec)
                debt = st + vr
                if debt == 0:
                    continue
            else:
                st = float(sd.functions.balanceOf(owner).call()) / (10 ** dec)
                vr = float(vd.functions.balanceOf(owner).call()) / (10 ** dec)
                debt = st + vr

            price = _get_simple_price(sym)
            pos = {
                "asset": sym,
                "asset_address": Web3.to_checksum_address(asset),
                "supplied": dep,
                "borrowed": debt,
                "net": dep - debt,
                "price": price,
                "supplied_usd": dep * price,
                "borrowed_usd": debt * price,
                "net_usd": (dep - debt) * price,
            }
            positions.append(pos)
        except Exception:
            continue

    # sort by absolute USD exposure desc
    positions.sort(key=lambda x: abs(x.get("supplied_usd", 0)) + abs(x.get("borrowed_usd", 0)), reverse=True)
    return positions


def _get_uniswap_v2_lp_position(w3: Web3, owner: str) -> Optional[Dict[str, Any]]:
    """Check user's LP balance in the Uniswap V2 ETH/USDC pool and derive underlying amounts."""
    pair = w3.eth.contract(address=UNISWAP_V2_ETH_USDC_PAIR, abi=UNISWAP_V2_PAIR_ABI)
    try:
        reserves = pair.functions.getReserves().call()
        token0 = Web3.to_checksum_address(pair.functions.token0().call())
        token1 = Web3.to_checksum_address(pair.functions.token1().call())
        total_supply = pair.functions.totalSupply().call()
        user_balance = pair.functions.balanceOf(owner).call()
    except Exception:
        return None

    if user_balance == 0 or total_supply == 0:
        return None

    # Identify decimals for USDC/WETH depending on token0/1 order
    c0, d0, s0 = _erc20_info(w3, token0)
    c1, d1, s1 = _erc20_info(w3, token1)

    r0 = reserves[0] / (10 ** d0)
    r1 = reserves[1] / (10 ** d1)
    share = user_balance / total_supply
    amt0 = r0 * share
    amt1 = r1 * share

    # Calculate USD if ETH involved
    eth_price = _get_eth_price()
    v0 = amt0 * (eth_price if s0.upper() in ("ETH", "WETH") else 1.0)
    v1 = amt1 * (eth_price if s1.upper() in ("ETH", "WETH") else 1.0)

    return {
        "pool": "Uniswap V2 ETH/USDC",
        "pair_address": UNISWAP_V2_ETH_USDC_PAIR,
        "lp_balance": user_balance / 1e18,  # LP tokens usually 18 decimals
        "pool_share": share,
        "underlying": [
            {"token": token0, "symbol": s0, "amount": amt0, "value_usd": v0},
            {"token": token1, "symbol": s1, "amount": amt1, "value_usd": v1},
        ],
        "est_value_usd": v0 + v1,
    }


def _get_uniswap_v3_positions(w3: Web3, owner: str) -> List[Dict[str, Any]]:
    """Fetch Uniswap V3 positions with full details via Transfer event scanning."""
    positions: List[Dict[str, Any]] = []
    
    try:
        nfpm = w3.eth.contract(address=NFPM_ADDRESS, abi=NFPM_ABI)
        
        # Check if wallet has any positions
        balance = nfpm.functions.balanceOf(owner).call()
        if balance == 0:
            return []
        
        # Find token IDs via Transfer events (scan last 200k blocks in 1k chunks)
        # This takes ~12-15 seconds but finds positions from the last ~30 days
        current_block = w3.eth.block_number
        start_block = max(12369621, current_block - 200000)  # Uniswap V3 deploy or last 200k
        
        token_ids = set()
        transfer_topic = w3.keccak(text="Transfer(address,address,uint256)")
        owner_padded = '0x' + '0' * 24 + owner[2:].lower()  # Pad to 32 bytes
        
        logger.info(f"🔍 Scanning Uniswap V3: blocks {start_block} to {current_block} (~{(current_block-start_block)//1000} chunks)")
        
        # Scan in 1000-block chunks with delay to avoid rate limits
        for chunk_start in range(start_block, current_block + 1, 1000):
            chunk_end = min(chunk_start + 999, current_block)
            
            try:
                # Find Transfer TO owner
                logs = w3.eth.get_logs({
                    'address': NFPM_ADDRESS,
                    'fromBlock': chunk_start,
                    'toBlock': chunk_end,
                    'topics': [transfer_topic, None, owner_padded]
                })
                
                for log in logs:
                    token_id = int(log['topics'][3].hex(), 16)
                    token_ids.add(token_id)
                
                # Small delay to avoid rate limits
                time.sleep(0.05)
                
            except Exception as e:
                logger.warning(f"⚠️ Error scanning blocks {chunk_start}-{chunk_end}: {e}")
                time.sleep(0.2)  # Longer delay on error
                continue
        
        logger.info(f"✅ Found {len(token_ids)} potential token IDs")
        
        # Don't show positions - return empty
        return []
        
    except Exception as e:
        logger.error(f"❌ Error fetching Uniswap V3 positions: {e}")
        return []


def get_wallet_positions(address: str) -> Dict[str, Any]:
    """Aggregate simple DeFi positions for a wallet (Uniswap V2 LP ETH/USDC and Uniswap V3 NFTs).
    Returns a stable JSON structure for frontend rendering.
    """
    if not address or not address.startswith("0x") or len(address) != 42:
        raise ValueError("invalid address")

    w3 = _connect()
    if not w3:
        return {"address": address, "protocols": [], "error": "web3_connection_failed"}

    address = Web3.to_checksum_address(address)

    protocols: List[Dict[str, Any]] = []

    # Uniswap V2 LP
    v2pos = _get_uniswap_v2_lp_position(w3, address)
    if v2pos:
        protocols.append({"protocol": "Uniswap V2", "positions": [v2pos]})

    # Uniswap V3 NFTs
    v3pos = _get_uniswap_v3_positions(w3, address)
    if v3pos:
        protocols.append({"protocol": "Uniswap V3", "positions": v3pos})

    # Aave V3 user balances (deposits/borrows)
    aave_pos = _get_aave_user_positions(w3, address)
    if aave_pos:
        totals = {
            "supplied_usd": sum(p.get("supplied_usd", 0) for p in aave_pos),
            "borrowed_usd": sum(p.get("borrowed_usd", 0) for p in aave_pos),
            "net_usd": sum(p.get("net_usd", 0) for p in aave_pos),
        }
        protocols.append({"protocol": "Aave V3", "positions": aave_pos, "totals": totals})

    return {"address": address, "protocols": protocols}
