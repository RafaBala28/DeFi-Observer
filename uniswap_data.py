# Copied from root version (lean)
from datetime import datetime, timezone

from config import UNISWAP_V2_ETH_USDC_PAIR
from aave_data import get_price_service
from web3_utils import get_web3

def get_uniswap_data():
    try:
        w3 = get_web3(timeout=10, sticky=True)
        if not w3 or not w3.is_connected():
            return {"error": "Blockchain connection failed"}
        contract_address = UNISWAP_V2_ETH_USDC_PAIR
        abi = [{
            "constant": True,
            "inputs": [],
            "name": "getReserves",
            "outputs": [
                {"internalType": "uint112", "name": "_reserve0", "type": "uint112"},
                {"internalType": "uint112", "name": "_reserve1", "type": "uint112"},
                {"internalType": "uint32", "name": "_blockTimestampLast", "type": "uint32"}
            ],
            "payable": False,
            "stateMutability": "view",
            "type": "function"
        }]
        contract = w3.eth.contract(address=contract_address, abi=abi)
        reserves = contract.functions.getReserves().call()
        usdc_reserve = reserves[0] / 1e6
        eth_reserve = reserves[1] / 1e18
        timestamp = reserves[2]
        
        # Berechne ETH-Preis aus Pool: USDC_reserve / ETH_reserve
        eth_price_from_pool = usdc_reserve / eth_reserve if eth_reserve > 0 else 0

        price_service = get_price_service()
        eth_price_usd = price_service.get_token_price("WETH") or eth_price_from_pool

        tvl_usd = eth_reserve * eth_price_usd + usdc_reserve
        update_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return {
            "pool": "Uniswap V2 ETH/USDC",
            "usdc_reserve": usdc_reserve,
            "eth_reserve": eth_reserve,
            "eth_price": eth_price_usd,
            "tvl_usd": tvl_usd,
            "timestamp": update_time.strftime('%Y-%m-%d %H:%M:%S UTC')
        }
    except Exception as e:
        return {"error": str(e)}
