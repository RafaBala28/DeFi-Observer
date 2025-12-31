"""
DeFi Observer 2.0 - Centralized Configuration
Single source of truth for all constants, addresses, and settings
"""
import os
from web3 import Web3

# ========== OPTIONAL API KEYS (from environment) ==========
# You can set these in a .env file or as environment variables
ALCHEMY_API_KEY = os.environ.get('ALCHEMY_API_KEY', '')
INFURA_API_KEY = os.environ.get('INFURA_API_KEY', '')

# Build RPC list dynamically (prioritize user's API keys if available)
def _build_ethereum_rpcs():
    rpcs = []
    if ALCHEMY_API_KEY:
        rpcs.append(f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}")
    if INFURA_API_KEY:
        rpcs.append(f"https://mainnet.infura.io/v3/{INFURA_API_KEY}")
    # Public RPCs (always available, no API key needed)
    rpcs.extend([
        "https://ethereum.publicnode.com",
        "https://eth.llamarpc.com",
        "https://cloudflare-eth.com",
        "https://rpc.ankr.com/eth"
    ])
    return rpcs

# ========== MULTI-CHAIN CONFIGURATION ==========
CHAINS = {
    'ethereum': {
        'name': 'Ethereum',
        'chain_id': 1,
        'icon': '',
        'rpc': _build_ethereum_rpcs(),
        'explorer': 'https://etherscan.io',
        'aave_pool': Web3.to_checksum_address("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),  # Aave V3 Pool
        'uniswap_v2_factory': Web3.to_checksum_address("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"),
        'uniswap_v2_pair': Web3.to_checksum_address("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"),
        'uniswap_v3_factory': Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
        'uniswap_v3_nfpm': Web3.to_checksum_address("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
        'uniswap_v3_pool': Web3.to_checksum_address("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"),
        'chainlink_eth_usd': Web3.to_checksum_address("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"),
    },
    'arbitrum': {
        'name': 'Arbitrum',
        'chain_id': 42161,
        'icon': '🔷',
        'rpc': [
            "https://arb1.arbitrum.io/rpc",
            "https://arbitrum.llamarpc.com",
            "https://arbitrum-one.publicnode.com"
        ],
        'explorer': 'https://arbiscan.io',
        'aave_pool': Web3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD"),
        'uniswap_v2_factory': None,
        'uniswap_v2_pair': None,
        'uniswap_v3_factory': Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
        'uniswap_v3_nfpm': Web3.to_checksum_address("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
        'uniswap_v3_pool': Web3.to_checksum_address("0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"),
        'chainlink_eth_usd': None,
    },
    'optimism': {
        'name': 'Optimism',
        'chain_id': 10,
        'icon': '🔴',
        'rpc': [
            "https://mainnet.optimism.io",
            "https://optimism.llamarpc.com",
            "https://optimism.publicnode.com"
        ],
        'explorer': 'https://optimistic.etherscan.io',
        'aave_pool': Web3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD"),
        'uniswap_v2_factory': None,
        'uniswap_v2_pair': None,
        'uniswap_v3_factory': Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
        'uniswap_v3_nfpm': Web3.to_checksum_address("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
        'uniswap_v3_pool': Web3.to_checksum_address("0x85149247691df622eaF1a8Bd0CaFd40BC45154a9"),
        'chainlink_eth_usd': None,
    },
    'base': {
        'name': 'Base',
        'chain_id': 8453,
        'icon': '🟦',
        'rpc': [
            "https://mainnet.base.org",
            "https://base.llamarpc.com",
            "https://base.publicnode.com"
        ],
        'explorer': 'https://basescan.org',
        'aave_pool': Web3.to_checksum_address("0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"),
        'uniswap_v2_factory': None,
        'uniswap_v2_pair': None,
        'uniswap_v3_factory': Web3.to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD"),
        'uniswap_v3_nfpm': Web3.to_checksum_address("0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"),
        'uniswap_v3_pool': Web3.to_checksum_address("0xd0b53D9277642d899DF5C87A3966A349A798F224"),
        'chainlink_eth_usd': None,
    }
}

# Default active chain
ACTIVE_CHAIN = 'ethereum'

def get_chain_config(chain_name=None):
    """Get configuration for specified chain or active chain"""
    chain = chain_name or ACTIVE_CHAIN
    return CHAINS.get(chain, CHAINS['ethereum'])

# ========== LEGACY COMPATIBILITY ==========
RPC_PROVIDERS = CHAINS['ethereum']['rpc']

# ========== CONTRACT ADDRESSES ==========
# Aave V3
AAVE_V3_POOL = CHAINS['ethereum']['aave_pool']

# Uniswap V2
UNISWAP_V2_FACTORY = CHAINS['ethereum']['uniswap_v2_factory']
UNISWAP_V2_ETH_USDC_PAIR = CHAINS['ethereum']['uniswap_v2_pair']

# Uniswap V3
UNISWAP_V3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
UNISWAP_V3_NFPM = Web3.to_checksum_address("0xC36442b4a4522E871399CD717aBDD847Ab11FE88")
UNISWAP_V3_ETH_USDC_POOL = Web3.to_checksum_address("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640")

# Chainlink
CHAINLINK_ETH_USD_FEED = Web3.to_checksum_address("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

# ========== TOKEN ADDRESSES ==========
class Tokens:
    WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
    USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
    USDT = Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7")
    DAI = Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F")
    WBTC = Web3.to_checksum_address("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599")
    LINK = Web3.to_checksum_address("0x514910771af9ca656af840dff83e8264ecf986ca")

# Token Symbol Mapping (lowercase addresses -> symbols)
TOKEN_SYMBOLS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "AAVE",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "UNI",
    "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39": "HEX",
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": "MKR",
    "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": "SHIB",
    "0x4d224452801aced8b2f0aebe155379bb5d594381": "APE",
    "0x6982508145454ce325ddbe47a25d4ec3d2311933": "PEPE",
    "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72": "ENS",
    "0x5a98fcbea516cf06857215779fd812ca3bef1b32": "LDO",
    "0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0": "FXS",
    "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e": "YFI",
    "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f": "SNX",
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": "stETH",
    "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0": "MATIC",
    "0xba100000625a3754423978a60c9317c58a424e3d": "BAL",
    "0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b": "DPI",
    "0x0d8775f648430679a709e98d2b0cb6250d2887ef": "BAT",
    "0xa693b19d2931d498c5b318df961919bb4aee87a5": "UST",
    "0x8e870d67f660d95d5be530380d0ec0bd388289e1": "USDP"
}

# CoinGecko ID Mapping
COINGECKO_IDS = {
    "WETH": "ethereum",
    "ETH": "ethereum",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "WBTC": "wrapped-bitcoin",
    "LINK": "chainlink",
    "AAVE": "aave",
    "UNI": "uniswap"
}

# Stablecoins for price display logic
STABLECOINS = {"USDC", "USDT", "DAI", "USDP", "UST"}

# ========== CACHE SETTINGS ==========
CACHE_TTL_SECONDS = 30
PRICE_CACHE_TTL_SECONDS = 60

# ========== LIQUIDATIONS CHAINLINK ENRICHMENT (opt-in) ==========
# When True the scanner will attempt to fetch Chainlink historical prices
# at scan time and store them with the liquidation events. Default False
# to avoid extra RPC/contract load on free public providers.
LIQUIDATIONS_CHAINLINK_ENRICH_AT_SCAN = False
# If True attempt a background backfill on startup (use with care)
LIQUIDATIONS_CHAINLINK_BACKFILL_ON_STARTUP = False
# Tuning for enrichment batching to avoid provider throttling
LIQUIDATIONS_CHAINLINK_ENRICH_BATCH_SIZE = 100
LIQUIDATIONS_CHAINLINK_ENRICH_BATCH_SLEEP_MS = 50

# ========== STORAGE SETTINGS ==========
DATA_DIR = "data"
MAX_PRICE_HISTORY_DAYS = 30
MAX_LIQUIDATION_HISTORY_DAYS = 30
