from typing import Any, Dict, List, Optional
from web3 import Web3
import time
import logging

from web3_utils import get_web3

logger = logging.getLogger(__name__)

# Aave v3 mainnet Pool
POOL_ADDRESS = Web3.to_checksum_address("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")

POOL_ABI = [
    {"inputs": [], "name": "getReservesList", "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "address", "name": "asset", "type": "address"}], "name": "getConfiguration", "outputs": [
        {"components": [{"internalType": "uint256", "name": "data", "type": "uint256"}], "internalType": "struct DataTypes.ReserveConfigurationMap", "name": "", "type": "tuple"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "address", "name": "asset", "type": "address"}], "name": "getReserveData", "outputs": [
        {"internalType": "uint256", "name": "configuration", "type": "uint256"},
        {"internalType": "uint128", "name": "liquidityIndex", "type": "uint128"},
        {"internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128"},
        {"internalType": "uint128", "name": "variableBorrowIndex", "type": "uint128"},
        {"internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128"},
        {"internalType": "uint128", "name": "currentStableBorrowRate", "type": "uint128"},
        {"internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40"},
        {"internalType": "uint16", "name": "id", "type": "uint16"},
        {"internalType": "address", "name": "aTokenAddress", "type": "address"},
        {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
        {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"},
        {"internalType": "address", "name": "interestRateStrategyAddress", "type": "address"},
        {"internalType": "uint128", "name": "accruedToTreasury", "type": "uint128"},
        {"internalType": "uint128", "name": "unbacked", "type": "uint128"},
        {"internalType": "uint128", "name": "isolationModeTotalDebt", "type": "uint128"}
    ], "stateMutability": "view", "type": "function"}
]

ERC20_BASIC_ABI = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
]

_cache: Dict[str, Dict[str, Any]] = {}


def _bits(val: int, start: int, size: int) -> int:
    mask = (1 << size) - 1
    return (val >> start) & mask


def get_aave_risk_snapshot() -> Dict[str, Any]:
    # simple cache (30s)
    now = time.time()
    c = _cache.get("aave_risk")
    if c and (now - c.get("t", 0) < 30):
        return c["v"]

    try:
        w3 = get_web3(timeout=12, sticky=True)
        if not w3 or not w3.is_connected():
            return {"error": "web3_connection_failed"}
    except Exception as e:
        logger.error("Failed to connect to Web3: %s", e)
        return {"error": "web3_connection_failed"}

    pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
    try:
        reserves: List[str] = pool.functions.getReservesList().call()
    except Exception as e:
        return {"error": f"getReservesList_failed: {e}"}

    assets: List[Dict[str, Any]] = []
    utils: List[float] = []

    for asset in reserves:
        try:
            conf = pool.functions.getConfiguration(asset).call()
            # conf may be tuple with single item {data: uint256} or uint256; handle both
            if isinstance(conf, (list, tuple)):
                data_val = int(conf[0] if len(conf) and isinstance(conf[0], (int,)) else conf[-1] if len(conf) else 0)
            else:
                data_val = int(conf)
            ltv_bps = _bits(data_val, 0, 16)
            liq_th_bps = _bits(data_val, 16, 16)
            liq_bonus_bps = _bits(data_val, 32, 16)

            rd = pool.functions.getReserveData(asset).call()
            a_token = rd[8]
            s_debt = rd[9]
            v_debt = rd[10]

            at = w3.eth.contract(address=a_token, abi=ERC20_BASIC_ABI)
            sd = w3.eth.contract(address=s_debt, abi=ERC20_BASIC_ABI)
            vd = w3.eth.contract(address=v_debt, abi=ERC20_BASIC_ABI)

            # decimals: debt tokens typically match underlying decimals; if call fails, use 18
            try:
                dec = int(at.functions.decimals().call())
            except Exception:
                dec = 18

            try:
                total_liq = float(at.functions.totalSupply().call()) / (10 ** dec)
            except Exception:
                total_liq = 0.0
            try:
                sd_sup = float(sd.functions.totalSupply().call()) / (10 ** dec)
            except Exception:
                sd_sup = 0.0
            try:
                vd_sup = float(vd.functions.totalSupply().call()) / (10 ** dec)
            except Exception:
                vd_sup = 0.0

            util = 0.0
            if total_liq > 0:
                util = (sd_sup + vd_sup) / total_liq * 100.0

            try:
                sym = w3.eth.contract(address=asset, abi=ERC20_BASIC_ABI).functions.symbol().call()
            except Exception:
                sym = "ASSET"

            assets.append({
                "symbol": sym,
                "ltv": round(ltv_bps / 100, 2),
                "liq_threshold": round(liq_th_bps / 100, 2),
                "liq_bonus": round(liq_bonus_bps / 100, 2),
                "utilization": round(util, 2),
            })
            utils.append(util)
        except Exception as e:
            logger.debug("Failed to process asset %s: %s", asset[:10] if asset else "unknown", str(e)[:50])
            continue

    avg_util = round(sum(utils) / len(utils), 2) if utils else 0.0

    result = {
        "protocol": "Aave V3 Risk Monitor",
        "assets": assets,
        "avg_utilization": avg_util,
    }

    _cache["aave_risk"] = {"t": now, "v": result}
    return result
