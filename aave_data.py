# Aave V3 data fetcher using Chainlink for pricing
from web3 import Web3
from datetime import datetime, timezone
import time
import logging

# Import shared utilities
from config import Tokens, get_chain_config
from abis import AAVE_V3_POOL_ABI, ERC20_ABI
from web3_utils import get_web3
from chainlink_price_utils import ChainlinkPriceFetcher

logger = logging.getLogger(__name__)

# Chainlink-based price service (replaces CoinGecko)
class ChainlinkPriceService:
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._fetcher = None
        
    def _get_fetcher(self):
        """Lazy init Chainlink fetcher"""
        if self._fetcher is None:
            self._fetcher = ChainlinkPriceFetcher()
        return self._fetcher
        
    def get_multiple_prices(self, symbols):
        """Fetch current prices from Chainlink with 30s cache"""
        now = time.time()
        prices = {}
        
        try:
            w3 = get_web3(timeout=10, sticky=True)
            if not w3 or not w3.is_connected():
                logger.warning("Web3 not connected, using cached prices")
                return {sym: self._cache.get(sym, 1.0) for sym in symbols}
            
            current_block = w3.eth.block_number
            fetcher = self._get_fetcher()
            
            for symbol in symbols:
                # Check cache (30s TTL)
                if symbol in self._cache and (now - self._cache_time.get(symbol, 0)) < 30:
                    prices[symbol] = self._cache[symbol]
                    continue
                
                # Fetch from Chainlink at current block
                try:
                    price = fetcher.get_price(symbol, current_block)
                    if price and price > 0:
                        self._cache[symbol] = price
                        self._cache_time[symbol] = now
                        prices[symbol] = price
                    else:
                        # Fallback to cached or 1.0
                        prices[symbol] = self._cache.get(symbol, 1.0)
                except Exception as e:
                    logger.debug("Chainlink price fetch failed for %s: %s", symbol, str(e)[:50])
                    prices[symbol] = self._cache.get(symbol, 1.0)
                    
        except Exception as e:
            logger.error("Price service error: %s", str(e)[:100])
            # Return cached prices or defaults
            return {sym: self._cache.get(sym, 1.0) for sym in symbols}
                
        return prices

_price_service_instance = None

def get_price_service():
    """Get or create Chainlink price service instance"""
    global _price_service_instance
    if _price_service_instance is None:
        _price_service_instance = ChainlinkPriceService()
    return _price_service_instance

def get_aave_data(chain_name: str | None = None, *, force_new: bool = False):
    try:
        # Use shared Web3 connection
        w3 = get_web3(timeout=10, chain_name=chain_name, sticky=not force_new, force_new=force_new)

        if not w3 or not w3.is_connected():
            return {"error": "Blockchain connection failed"}
        chain_cfg = get_chain_config(chain_name)
        pool_address = chain_cfg.get("aave_pool")
        if not pool_address:
            return {"error": "Aave is not configured for this chain"}
        pool_contract = w3.eth.contract(address=pool_address, abi=AAVE_V3_POOL_ABI)
        
        # 🔧 Hardcoded Token-Adressen für Mainnet (funktioniert immer!)
        assets = {
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
        }
        
        # Use Chainlink-based price service
        price_service = get_price_service()
        prices = price_service.get_multiple_prices(list(assets.keys()))
        
        result = {"protocol": "Aave V3", "assets": [], "total_liquidity_usd": 0, "total_borrowed_usd": 0, "total_tvl_usd": 0}
        RAY = 10 ** 27; SECONDS_PER_YEAR = 31536000
        for name, addr in assets.items():
            try:
                rd = pool_contract.functions.getReserveData(addr).call()
                asset = w3.eth.contract(address=addr, abi=ERC20_ABI)
                atok = w3.eth.contract(address=rd[8], abi=ERC20_ABI)
                debt = w3.eth.contract(address=rd[10], abi=ERC20_ABI)
                decimals = asset.functions.decimals().call()
                total_liq = atok.functions.totalSupply().call() / (10 ** decimals)
                total_bor = debt.functions.totalSupply().call() / (10 ** decimals)
                dep_rate = rd[2] / RAY; bor_rate = rd[4] / RAY
                dep_apy = (pow(1 + (dep_rate / SECONDS_PER_YEAR), SECONDS_PER_YEAR) - 1) * 100
                bor_apy = (pow(1 + (bor_rate / SECONDS_PER_YEAR), SECONDS_PER_YEAR) - 1) * 100
                util = (total_bor / total_liq * 100) if total_liq > 0 else 0
                price = prices.get(name, 1.0)
                liq_usd = total_liq * price; bor_usd = total_bor * price
                result["total_liquidity_usd"] += liq_usd; result["total_borrowed_usd"] += bor_usd
                result["assets"].append({
                    "name": name,
                    "address": addr,
                    "total_liquidity": total_liq,
                    "total_borrowed": total_bor,
                    "available": total_liq - total_bor,
                    "utilization_rate": util,
                    "deposit_apy": dep_apy,
                    "borrow_apy": bor_apy,
                    "price_usd": price,
                    "liquidity_usd": liq_usd,
                    "borrowed_usd": bor_usd,
                    "last_update": datetime.fromtimestamp(rd[6], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                })
            except Exception:
                continue
        result["total_tvl_usd"] = result["total_liquidity_usd"]
        result["avg_utilization"] = (result["total_borrowed_usd"] / result["total_liquidity_usd"] * 100) if result["total_liquidity_usd"] > 0 else 0
        return result
    except Exception as e:
        return {"error": str(e)}
