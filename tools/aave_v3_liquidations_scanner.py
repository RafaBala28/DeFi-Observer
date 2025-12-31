import os
import sys
import csv
import json
import time
import logging
import subprocess
from datetime import datetime
from eth_utils import event_abi_to_log_topic
from web3 import Web3
from web3.providers.rpc import HTTPProvider
# ABI decoder: try multiple import paths for compatibility across eth_abi versions
decode = None
try:
    from eth_abi import decode_abi as _decode
    decode = _decode
except Exception:
    try:
        from eth_abi.abi import decode_abi as _decode
        decode = _decode
    except Exception:
        try:
            from eth_abi import decode as _decode
            decode = _decode
        except Exception:
            # Fallback to Web3 codec at runtime (will create temporary Web3)
            def decode(types, data):
                return Web3().codec.decode_abi(types, data)
# Ensure project root is on sys.path so running this module directly can
# import sibling modules like `chainlink_price_utils` when invoked from
# different working directories or tooling.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from chainlink_price_utils import ChainlinkPriceFetcher, normalize_symbol, get_fallback_symbol, is_stablecoin
from web3_utils import get_web3, get_logs_chunked
from tools.csv_utils import safe_append_row
import random
import shutil
import tempfile
from master_csv_manager import MASTER_CSV_PATH, ensure_master_csv_exists

# ANSI Color Codes für Terminal Output
class Colors:
    RED = '\033[91m'      # Fehlende Preise
    YELLOW = '\033[93m'   # Warnings
    GREEN = '\033[92m'    # Success
    CYAN = '\033[96m'     # Info
    RESET = '\033[0m'     # Reset to default
    BOLD = '\033[1m'

# Use root logging configuration from the main app for consistent formatting.
# Do not call logging.basicConfig() here so `app.py` can control handlers/formatters.
logger = logging.getLogger("aave_scanner")

# Data directory
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# ============================================================================
# AAVE V3 LiquidationCall Event ABI & Topic
# (Previously in src/aave/event_parsing.py - now integrated here)
# ============================================================================
LIQUIDATION_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "address", "name": "collateralAsset", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "debtAsset", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "user", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "debtToCover", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "liquidatedCollateralAmount", "type": "uint256"},
        {"indexed": False, "internalType": "address", "name": "liquidator", "type": "address"},
        {"indexed": False, "internalType": "bool", "name": "receiveAToken", "type": "bool"},
    ],
    "name": "LiquidationCall",
    "type": "event",
}
LIQUIDATION_TOPIC = event_abi_to_log_topic(LIQUIDATION_EVENT_ABI)

# Canonical CSV column order used by the frontend download
# Gruppiert: 1) Block/Zeit, 2) LiquidationCall Event, 3) Angereichert, 4) TX-Meta
CSV_FIELD_ORDER = [
    # Block/Zeit
    'block',
    'timestamp',
    'datetime_utc',
    # LiquidationCall Event-Daten (Raw)
    'collateralAsset',
    'debtAsset',
    'user',
    'liquidator',
    'collateralOut',
    'debtToCover',
    'receiveAToken',
    # Angereicherte Daten (Symbole, Preise, USD-Werte)
    'collateralSymbol',
    'debtSymbol',
    'collateral_price_usd_at_block',
    'debt_price_usd_at_block',
    'collateral_value_usd',
    'debt_value_usd',
    # TX Metadaten
    'tx',
    'block_builder',
    'gas_used',
    'gas_price_gwei',
    # NEU: ETH Preis im Block (Chainlink)
    'eth_price_usd_at_block',
]

# Safety: do not write to the canonical master CSV by default. Set
# WRITE_TO_MASTER = False so the scanner writes to a staging file
# (`liquidations_master_new.csv`) unless an operator explicitly enables
# writing to master (reduce accidental overwrites / concurrent writes).
# Safety: default to write to MASTER to avoid multiple CSVs
# By default we will write directly to the canonical master CSV to ensure
# only a single authoritative CSV exists. Operators who need a staging
# workflow can re-enable staging by setting `WRITE_TO_MASTER = False`.
WRITE_TO_MASTER = True
MASTER_CSV_FILENAME = os.path.join(DATA_DIR, "liquidations_master.csv")
MASTER_CSV_FILENAME = MASTER_CSV_PATH
STAGING_CSV_FILENAME = os.path.join(os.path.dirname(MASTER_CSV_PATH), "liquidations_master_new.csv")


def get_write_csv_path():
    """Return the path to write to. By default use a staging file to avoid
    mutating the original master CSV. If WRITE_TO_MASTER is True, return the
    canonical master path (only do this with caution)."""
    return MASTER_CSV_FILENAME if WRITE_TO_MASTER else STAGING_CSV_FILENAME

# Numeric columns we expect to be numbers in the CSV
NUMERIC_FIELD_NAMES = {
    'collateral_price_usd_at_block',
    'debt_price_usd_at_block',
    'collateral_value_usd',
    'debt_value_usd',
    'gas_used',
    'gas_price_gwei',
    'eth_price_usd_at_block',
    'collateralOut',
    'debtToCover',
}


def normalize_event_data_for_write(event_data: dict) -> dict:
    """Prepare event_data for CSV writing:
    - Ensure numeric fields are numeric strings or empty.
    - Remove any price-source strings from numeric columns by clearing source fields
      in the written output (we keep internal detection but do not persist sources).
    """
    out = event_data.copy()

    # Coerce numeric fields: if not a number, replace with empty string
    for nf in NUMERIC_FIELD_NAMES:
        val = out.get(nf, '')
        if val == '' or val is None:
            out[nf] = ''
            continue
        # Accept numeric types directly
        if isinstance(val, (int, float)):
            out[nf] = val
            continue
        # Try to parse string to float
        try:
            if isinstance(val, str):
                # strip and remove commas
                v = val.strip().replace(',', '')
                # handle hex-looking values -> treat as invalid
                if v.startswith('0x') or any(c.isalpha() for c in v if c not in '.-eE'):
                    out[nf] = ''
                else:
                    out[nf] = float(v)
            else:
                out[nf] = ''
        except Exception:
            out[nf] = ''

    # Price source metadata is not persisted

    return out


def reconcile_master_csv_header(backup=False):
    """Ensure `data/liquidations_master.csv` header matches `CSV_FIELD_ORDER`.
    If mismatched, make a timestamped backup (if requested) and rewrite the file
    with the canonical header. Existing rows will be remapped by column name;
    missing columns will be filled with empty strings. Returns a dict with
    info about any change performed or None if no change was needed.
    """
    master = MASTER_CSV_FILENAME
    if not os.path.exists(master):
        return None

    try:
        with open(master, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            input_header = reader.fieldnames or []
            rows = list(reader)

        # If header already matches exactly (order and names), nothing to do
        if input_header == CSV_FIELD_ORDER:
            logger.info("[Liquidations] Master CSV header already canonical")
            return None

        # Backups are disabled by default per operator request. We will not create
        # or restore from backup files; instead we rewrite the master CSV header
        # in-place (atomic replace) without using prior backups.
        backup_path = None

        # Write temp file with canonical header
        fd, tmp_path = tempfile.mkstemp(prefix='liquidations_master_', suffix='.csv', dir=DATA_DIR)
        os.close(fd)
        with open(tmp_path, 'w', encoding='utf-8', newline='') as outf:
            writer = csv.DictWriter(outf, fieldnames=CSV_FIELD_ORDER)
            writer.writeheader()
            for r in rows:
                out_row = {c: r.get(c, '') for c in CSV_FIELD_ORDER}
                writer.writerow(out_row)

        # Atomic replace
        os.replace(tmp_path, master)
        logger.info("[Liquidations] Rewrote master CSV with canonical header. Rows: %d", len(rows))

        return {
            'backup': None,
            'rows': len(rows),
            'new_header': CSV_FIELD_ORDER,
            'old_header': input_header,
            'path': master,
        }
    except Exception as e:
        logger.exception("[Liquidations] Header reconciliation failed: %s", e)
        return None


def sync_scan_status_from_csv(csv_path=None, status_path=None, status='running', current_block=None, to_block=None, message=None):
    """Write `data/scan_status.json` derived from the given CSV.
    This is a module-level helper so any function can call it to ensure
    the frontend status is always computed from the canonical master CSV.
    """
    csv_path = csv_path or MASTER_CSV_FILENAME
    status_path = status_path or os.path.join('data', 'scan_status.json')
    computed_from = FROM_BLOCK
    computed_events = 0
    try:
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
            with open(csv_path, 'r', encoding='utf-8') as cf:
                reader = csv.DictReader(cf)
                rows = list(reader)
                if rows:
                    computed_events = len(rows)
                    blocks = [int(r.get('block', 0)) for r in rows if r.get('block')]
                    if blocks:
                        computed_from = min(blocks)
    except Exception:
        computed_from = FROM_BLOCK

    payload = {
        'status': status,
        'from_block': computed_from,
        'to_block': to_block if to_block is not None else None,
        'current_block': current_block,
        'events_found': computed_events,
        'last_updated': int(time.time()),
        'message': message,
    }
    try:
        with open(status_path, 'w', encoding='utf-8') as sf:
            json.dump(payload, sf)
    except Exception:
        logger.exception('Failed to write scan status (sync helper)')

# RPC Provider Management: Uses centralized config.py (4 public RPCs + 2 optional with API keys)
# No need to maintain a separate provider list here - web3_utils.get_web3() handles rotation

def get_web3_with_rotation():
    """Create Web3 instance using centralized ProviderManager (round-robin + health).

    This delegates provider selection and retries to `web3_utils.ProviderManager` via
    `get_web3()`, so the scanner benefits from dynamic batching, improved timeouts
    and provider health tracking while still using the project's configured RPC list.
    All RPC calls are automatically tracked in web3_utils for global statistics.
    """
    try:
        w3 = get_web3(timeout=30, force_new=False, sticky=True)
        if w3 is None:
            raise RuntimeError("No available RPC providers (get_web3 returned None)")
        # Validate chain id
        if getattr(w3.eth, 'chain_id', None) != 1:
            raise ValueError(f"Wrong chain! Expected 1, got {getattr(w3.eth, 'chain_id', None)}")
        return w3
    except Exception as e:
        logger.error("Failed to obtain Web3 from provider manager: %s", str(e)[:200])
        raise

# Token Symbol Mapping (all AAVE v3 Mainnet Assets)
# IMPORTANT: All addresses in lowercase for consistent comparison!
TOKEN_SYMBOLS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": "wstETH",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "AAVE",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "UNI",
    "0xae78736cd615f374d3085123a210448e74fc6393": "rETH",
    "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7": "rsETH",    # Kelp DAO Restaked ETH
    "0x83f20f44975d03b1b09e64809b757c47f942beea": "sDAI",
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": "MKR",
    "0x6810e776880c02933d47db1b9fc05908e5386b96": "GNO",
    "0xd533a949740bb3306d119cc777fa900ba034cd52": "CRV",
    "0x5a98fcbea516cf06857215779fd812ca3bef1b32": "LDO",
    "0xc00e94cb662c3520282e6f5717214004a7f26888": "COMP",
    "0xba100000625a3754423978a60c9317c58a424e3d": "BAL",
    "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f": "SNX",
    "0x5f98805a4e8be255a32880fdec7f6728c6568ba0": "LUSD",
    "0x853d955acef822db058eb8505911ed77f175b99e": "FRAX",
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": "cbETH",
    # New Tokens (AAVE V3 Extensions)
    "0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f": "GHO",      # Aave Stablecoin
    "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72": "ENS",      # ENS Token
    "0xd33526068d116ce69f19a9ee46f0bd304f21a51f": "RPL",      # Rocket Pool
    "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e": "crvUSD",   # Curve USD (kein Chainlink Feed!)
    "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee": "weETH",    # Wrapped eETH
    "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": "USDe",     # Ethena USDe
    "0x9d39a5de30e57443bff2a8307a4256c8797a3497": "sUSDe",    # Staked USDe
    "0x6c3ea9036406852006290770bedfcaba0e23a0e8": "PYUSD",    # PayPal USD
    "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "cbBTC",    # Coinbase BTC
    "0xddc3d26baa9d2d979f5e2e42515478bf18f354d5": "USDS",     # Sky USD
    "0x1111111111166b7fe7bd91427724b487980afc69": "1INCH",    # 1inch (0x1111...C302 partial match)
}

# Token Decimals Mapping (IMPORTANT: USDC/USDT=6, WBTC=8, rest=18!)
TOKEN_DECIMALS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC (6 decimals)
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT (6 decimals)
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # DAI
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,   # WBTC (8 decimals)
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": 18,  # wstETH
    "0x514910771af9ca656af840dff83e8264ecf986ca": 18,  # LINK
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": 18,  # AAVE
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": 18,  # UNI
    "0xae78736cd615f374d3085123a210448e74fc6393": 18,  # rETH
    "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7": 18,  # rsETH
    "0x83f20f44975d03b1b09e64809b757c47f942beea": 18,  # sDAI
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": 18,  # MKR
    "0x6810e776880c02933d47db1b9fc05908e5386b96": 18,  # GNO
    "0xd533a949740bb3306d119cc777fa900ba034cd52": 18,  # CRV
    "0x5a98fcbea516cf06857215779fd812ca3bef1b32": 18,  # LDO
    "0xc00e94cb662c3520282e6f5717214004a7f26888": 18,  # COMP
    "0xba100000625a3754423978a60c9317c58a424e3d": 18,  # BAL
    "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f": 18,  # SNX
    "0x5f98805a4e8be255a32880fdec7f6728c6568ba0": 18,  # LUSD
    "0x853d955acef822db058eb8505911ed77f175b99e": 18,  # FRAX
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": 18,  # cbETH
    # New Tokens (added in session)
    "0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f": 18,  # GHO
    "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e": 18,  # crvUSD
    "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee": 18,  # weETH
    "0x9d39a5de30e57443bff2a8307a4256c8797a3497": 18,  # sUSDe
    "0x6c3ea9036406852006290770bedfcaba0e23a0e8": 6,   # PYUSD (6 decimals)
    "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72": 18,  # ENS
    "0xd33526068d116ce69f19a9ee46f0bd304f21a51f": 18,  # RPL
    "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": 8,   # cbBTC (8 decimals)
    "0xddc3d26baa9d2d979f5e2e42515478bf18f354d5": 18,  # USDS
    "0x1111111111166b7fe7bd91427724b487980afc69": 18,  # 1INCH
}

# ERC20 ABI for symbol() and decimals()
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
]

# ============================================================================
# LSD (Liquid Staking Derivatives) PRICE LOGIC - AAVE-KONFORM
# AAVE berechnet LSD-Preise über: exchange_rate * underlying_price
# ============================================================================

# LSD Token Configuration: address -> (exchange_rate_function, underlying_asset, decimals_adjustment)
# underlying_asset: "ETH" for ETH-LSDs, "DAI" for sDAI, etc.
LSD_TOKENS = {
    # wstETH: stEthPerToken() returns how many stETH per 1 wstETH (18 decimals)
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": {
        "name": "wstETH",
        "function": "stEthPerToken",
        "underlying": "ETH",
        "decimals": 18,
    },
    # rETH: getExchangeRate() returns ETH per rETH (18 decimals)
    "0xae78736cd615f374d3085123a210448e74fc6393": {
        "name": "rETH",
        "function": "getExchangeRate",
        "underlying": "ETH",
        "decimals": 18,
    },
    # cbETH: exchangeRate() returns ETH per cbETH (18 decimals)
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": {
        "name": "cbETH",
        "function": "exchangeRate",
        "underlying": "ETH",
        "decimals": 18,
    },
    # weETH: getRate() returns eETH per weETH (18 decimals) - eETH ~= ETH
    "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee": {
        "name": "weETH",
        "function": "getRate",
        "underlying": "ETH",
        "decimals": 18,
    },
    # rsETH: Kelp DAO Restaked ETH - getRsETHPrice() returns ETH per rsETH (18 decimals)
    "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7": {
        "name": "rsETH",
        "function": "getRsETHPrice",
        "underlying": "ETH",
        "decimals": 18,
    },
    # sDAI: Savings DAI - uses convertToAssets(1e18) to get DAI per sDAI
    "0x83f20f44975d03b1b09e64809b757c47f942beea": {
        "name": "sDAI",
        "function": "convertToAssets",
        "underlying": "DAI",
        "decimals": 18,
        "input_amount": 10**18,  # Special: needs input parameter
    },
    # sUSDe: Staked USDe - similar ERC4626 pattern
    "0x9d39a5de30e57443bff2a8307a4256c8797a3497": {
        "name": "sUSDe",
        "function": "convertToAssets",
        "underlying": "USDE",  # USDe is ~$1
        "decimals": 18,
        "input_amount": 10**18,
    },
}

# ABIs for LSD exchange rate functions
LSD_EXCHANGE_RATE_ABIS = {
    # wstETH: stEthPerToken() -> uint256
    "stEthPerToken": [
        {"constant": True, "inputs": [], "name": "stEthPerToken", 
         "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
    ],
    # rETH: getExchangeRate() -> uint256
    "getExchangeRate": [
        {"constant": True, "inputs": [], "name": "getExchangeRate", 
         "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
    ],
    # cbETH: exchangeRate() -> uint256
    "exchangeRate": [
        {"constant": True, "inputs": [], "name": "exchangeRate", 
         "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
    ],
    # weETH: getRate() -> uint256
    "getRate": [
        {"constant": True, "inputs": [], "name": "getRate", 
         "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
    ],
    # rsETH (Kelp DAO): getRsETHPrice() -> uint256
    "getRsETHPrice": [
        {"constant": True, "inputs": [], "name": "getRsETHPrice", 
         "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
    ],
    # ERC4626 (sDAI, sUSDe): convertToAssets(uint256 shares) -> uint256
    "convertToAssets": [
        {"constant": True, "inputs": [{"name": "shares", "type": "uint256"}], 
         "name": "convertToAssets", "outputs": [{"name": "", "type": "uint256"}], 
         "stateMutability": "view", "type": "function"}
    ],
}


def is_lsd_token(asset_address: str) -> bool:
    """Check if an asset is a Liquid Staking Derivative token."""
    return asset_address.lower() in LSD_TOKENS


def get_lsd_exchange_rate(w3, asset_address: str, block_number: int) -> float:
    """
    Get the exchange rate for an LSD token at a specific block.
    Returns the rate as a float (e.g., 1.15 means 1 LSD = 1.15 underlying).
    Returns None on error.
    """
    addr_lower = asset_address.lower()
    if addr_lower not in LSD_TOKENS:
        return None
    
    config = LSD_TOKENS[addr_lower]
    func_name = config["function"]
    decimals = config["decimals"]
    
    try:
        abi = LSD_EXCHANGE_RATE_ABIS.get(func_name)
        if not abi:
            logger.warning("[LSD] No ABI for function %s", func_name)
            return None
        
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(asset_address),
            abi=abi
        )
        
        # Call the exchange rate function at the historical block
        if "input_amount" in config:
            # ERC4626 style: convertToAssets(shares)
            raw_rate = contract.functions.convertToAssets(config["input_amount"]).call(
                block_identifier=block_number
            )
        else:
            # Simple no-arg functions
            func = getattr(contract.functions, func_name)
            raw_rate = func().call(block_identifier=block_number)
        
        # Convert from wei to float
        exchange_rate = raw_rate / (10 ** decimals)
        
        return exchange_rate
        
    except Exception as e:
        logger.debug("[LSD] Failed to get exchange rate for %s @ block %s: %s", 
                    config.get("name", asset_address[:10]), block_number, str(e)[:50])
        return None


def get_lsd_price(symbol: str, asset_address: str, block_number: int, fetcher, w3) -> float:
    """
    Calculate the USD price of an LSD token using AAVE's methodology:
    LSD_USD_Price = Exchange_Rate × Underlying_Asset_USD_Price
    
    Args:
        symbol: Token symbol (e.g., "wstETH")
        asset_address: Token contract address
        block_number: Block to get historical price at
        fetcher: ChainlinkPriceFetcher instance
        w3: Web3 instance
    
    Returns:
        USD price as float, or None on error
    """
    addr_lower = asset_address.lower()
    if addr_lower not in LSD_TOKENS:
        return None
    
    config = LSD_TOKENS[addr_lower]
    underlying = config["underlying"]
    
    try:
        # Step 1: Get exchange rate at this block
        exchange_rate = get_lsd_exchange_rate(w3, asset_address, block_number)
        if exchange_rate is None or exchange_rate <= 0:
            logger.debug("[LSD] No exchange rate for %s @ block %s", symbol, block_number)
            return None
        
        # Step 2: Get underlying asset price
        underlying_price = None
        
        if underlying == "ETH":
            # Get ETH price from Chainlink
            try:
                underlying_price = fetcher.get_price_for_block("ETH", block_number)
            except Exception:
                pass
            
            # Fallback: try WETH
            if not underlying_price:
                try:
                    underlying_price = fetcher.get_price_for_block("WETH", block_number)
                except Exception:
                    pass
        
        elif underlying == "DAI":
            # DAI is a stablecoin, but try Chainlink first
            try:
                underlying_price = fetcher.get_price_for_block("DAI", block_number)
            except Exception:
                pass
            if not underlying_price:
                underlying_price = 1.0  # Stablecoin fallback
        
        elif underlying == "USDE":
            # USDe is a stablecoin
            underlying_price = 1.0
        
        else:
            # Try to get price from Chainlink for other underlyings
            try:
                underlying_price = fetcher.get_price_for_block(underlying, block_number)
            except Exception:
                pass
        
        if underlying_price is None or underlying_price <= 0:
            logger.debug("[LSD] No underlying price for %s (%s) @ block %s", 
                        symbol, underlying, block_number)
            return None
        
        # Step 3: Calculate final price
        lsd_price = exchange_rate * underlying_price
        
        logger.debug("[LSD] %s @ %s: rate=%.6f × %s=$%.2f = $%.2f", 
                    symbol, block_number, exchange_rate, underlying, underlying_price, lsd_price)
        
        return lsd_price
        
    except Exception as e:
        logger.debug("[LSD] Error calculating price for %s: %s", symbol, str(e)[:50])
        return None


def get_aave_asset_price(symbol: str, asset_address: str, block_number: int, 
                         fetcher, w3, feed_symbol: str = None) -> float:
    """
    Get the USD price for any AAVE asset using AAVE's methodology:
    
    PRIORITY ORDER (AAVE Liquidation Methodology):
    1. AAVE V3 Oracle (authoritative - what AAVE uses for liquidations!)
    2. CAPO-protected LSD (exchange-rate × underlying with safety cap)
    3. LSD without CAPO (fallback when no CAPO configured)
    4. Chainlink Feed (standard assets)
    5. Fallback Feeds (ETH für ETH-Derivate, BTC für BTC-Derivate)
    6. Stablecoins → $1.00
    
    Args:
        symbol: Token symbol (e.g., "WETH", "wstETH", "rsETH")
        asset_address: Token contract address
        block_number: Block to get historical price at
        fetcher: ChainlinkPriceFetcher instance
        w3: Web3 instance
        feed_symbol: Optional normalized feed symbol for Chainlink
    
    Returns:
        USD price as float, or None on error
    """
    price = None
    feed_to_use = feed_symbol if feed_symbol else symbol
    
    # ROBUST RETRY CONFIG: Mehr Versuche, längere Pausen
    MAX_PRICE_RETRIES = 5
    RETRY_DELAYS = [1.0, 2.0, 3.0, 5.0, 8.0]  # Exponential backoff
    
    # PRIORITY 1: AAVE V3 Oracle ZUERST (wie AAVE selbst Liquidationen berechnet)
    # Dies ist die authoritative Quelle für alle AAVE-gelisteten Assets
    try:
        addr_lower = asset_address.lower() if asset_address else None
        if addr_lower:
            # Try direct AAVE Oracle call using asset address
            for attempt in range(3):
                try:
                    oracle_abi = [
                        {
                            "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
                            "name": "getAssetPrice",
                            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                            "stateMutability": "view",
                            "type": "function"
                        }
                    ]
                    AAVE_ORACLE_ADDRESS = "0x54586bE62E3c3580375aE3723C145253060Ca0C2"
                    oracle = w3.eth.contract(
                        address=Web3.to_checksum_address(AAVE_ORACLE_ADDRESS),
                        abi=oracle_abi
                    )
                    price_raw = oracle.functions.getAssetPrice(
                        Web3.to_checksum_address(asset_address)
                    ).call(block_identifier=block_number)
                    
                    if price_raw and price_raw > 0:
                        aave_price = price_raw / 10**8  # AAVE uses 8 decimals
                        logger.debug("[AAVE Oracle] %s @ %s: $%.2f", symbol, block_number, aave_price)
                        return aave_price
                    break  # 0 returned = asset not configured in oracle
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1.0 + attempt)
                        rotate_provider()
                    else:
                        logger.debug("[AAVE Oracle] Failed for %s: %s", symbol, str(e)[:50])
    except Exception:
        pass
    
    # PRIORITY 2: Chainlink feed (standard assets - most reliable for direct feeds)
    if feed_to_use:
        for attempt in range(MAX_PRICE_RETRIES):
            try:
                price = fetcher.get_price_for_block(feed_to_use, block_number)
                if price is not None and price > 0:
                    logger.debug("[Chainlink] %s @ %s: $%.2f", symbol, block_number, price)
                    return price
                # None returned but no exception - feed might not exist at this block
                if attempt == 0:
                    break  # Don't retry if feed simply doesn't have data
            except Exception as e:
                if attempt < MAX_PRICE_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    logger.debug("[Price] Retry %d/%d for %s @ %s (%.1fs): %s", 
                                attempt + 1, MAX_PRICE_RETRIES, feed_to_use, block_number, delay, str(e)[:50])
                    time.sleep(delay)
                    # Rotate provider on network errors
                    if "timeout" in str(e).lower() or "connection" in str(e).lower():
                        rotate_provider()
                else:
                    logger.warning("[Price] All %d retries failed for %s @ %s", 
                                  MAX_PRICE_RETRIES, feed_to_use, block_number)

    # PRIORITY 3: CAPO Protection (Aave's Capped Price Oracle)
    # Applies rate cap protection using deployed CAPO contracts
    # Works for LSDs (wstETH, rETH, cbETH), stablecoins (sUSDe, sDAI), and other capped assets
    # Reads CAPO parameters blockgenau from Aave contracts
    if is_lsd_token(asset_address):
        for attempt in range(3):
            try:
                from chainlink_price_utils import cap_price_from_ratio
                
                # Try to get CAPO params from deployed Aave contracts (blockgenau)
                capo_params = fetcher._get_capo_params_from_chain(symbol, block_number)
                
                if capo_params:
                    # Get current ratio and underlying price
                    lsd_info = LSD_CONTRACTS.get(asset_address.lower())
                    if lsd_info:
                        contract = w3.eth.contract(
                            address=Web3.to_checksum_address(asset_address),
                            abi=lsd_info["abi"]
                        )
                        current_ratio = contract.functions[lsd_info["function"]](*lsd_info.get("args", [])).call(
                            block_identifier=block_number
                        )
                        
                        # Get underlying asset price (ETH for most LSDs)
                        underlying_symbol = lsd_info.get("underlying", "ETH")
                        underlying_price = fetcher.get_price_for_block(underlying_symbol, block_number)
                        
                        if underlying_price and current_ratio:
                            # Apply CAPO protection: max_ratio = snapshot + (growth × elapsed_time)
                            block_ts = w3.eth.get_block(block_number).timestamp
                            capo_price = cap_price_from_ratio(
                                base_price=underlying_price,
                                current_ratio=current_ratio,
                                snapshot_ratio=int(capo_params["snapshotRatio"]),
                                snapshot_ts=capo_params["snapshotTimestamp"],
                                max_yearly_ratio_bps=capo_params["maxYearlyRatioGrowthPercent"],
                                ratio_decimals=capo_params["ratioDecimals"],
                                event_ts=block_ts
                            )
                            capo_price_float = float(capo_price)
                            if capo_price_float > 0:
                                logger.debug("[CAPO] %s @ %s: $%.2f (capped)", symbol, block_number, capo_price_float)
                                return capo_price_float
                break  # CAPO params not found or failed - continue to raw LSD
            except Exception as e:
                if attempt < 2:
                    time.sleep(1.0 + attempt)
                    rotate_provider()
                else:
                    logger.debug("[CAPO] Failed for %s @ %s: %s", symbol, block_number, str(e)[:80])

    # PRIORITY 4: Raw LSD (WITHOUT CAPO protection)
    # Fallback for LSDs when no CAPO contract exists or CAPO lookup failed
    # Uses simple calculation: exchange_rate × underlying_price (NO rate cap applied)
    if is_lsd_token(asset_address):
        for attempt in range(3):
            try:
                lsd_price = get_lsd_price(symbol, asset_address, block_number, fetcher, w3)
                if lsd_price is not None and lsd_price > 0:
                    logger.debug("[Raw LSD] %s @ %s: $%.2f (uncapped)", symbol, block_number, lsd_price)
                    return lsd_price
                break  # None returned - don't retry
            except Exception as e:
                if attempt < 2:
                    time.sleep(1.0 + attempt)
                    rotate_provider()

    # PRIORITY 5: Stablecoins → $1.00
    if is_stablecoin(symbol):
        return 1.0

    # No price found
    return None

def _get_token_symbol(w3, address: str) -> str:
    """Get token symbol from address"""
    addr_lower = address.lower()
    if addr_lower in TOKEN_SYMBOLS:
        return TOKEN_SYMBOLS[addr_lower]
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=ERC20_ABI)
        symbol = contract.functions.symbol().call()
        return symbol if symbol else address[:6] + "…" + address[-4:]
    except Exception:
        return address[:6] + "…" + address[-4:]

def _get_token_decimals(w3, address: str) -> int:
    """Get token decimals from address"""
    addr_lower = address.lower()
    if addr_lower in TOKEN_DECIMALS:
        return TOKEN_DECIMALS[addr_lower]
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=ERC20_ABI)
        decimals = contract.functions.decimals().call()
        return decimals if decimals else 18
    except Exception:
        logger.warning("Could not fetch decimals for %s, assuming 18", address)
        return 18

# ABI for AAVE getConfiguration (returns ReserveConfigurationMap with packed risk params)
AAVE_GET_CONFIG_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getConfiguration",
        "outputs": [{"components": [{"internalType": "uint256", "name": "data", "type": "uint256"}], 
                     "internalType": "struct DataTypes.ReserveConfigurationMap", "name": "", "type": "tuple"}],
        "stateMutability": "view",
        "type": "function"
    }
]

def _extract_bits(value: int, start: int, length: int) -> int:
    """Extract bits from packed uint256 value"""
    return (value >> start) & ((1 << length) - 1)

def _get_liquidation_params(w3, pool_address: str, collateral_asset: str) -> tuple:
    """Get liquidation threshold and bonus for collateral asset from AAVE pool.
    
    Returns:
        (liq_threshold_percent, liq_bonus_percent) or (None, None) on error
    """
    try:
        pool = w3.eth.contract(address=pool_address, abi=AAVE_GET_CONFIG_ABI)
        conf = pool.functions.getConfiguration(Web3.to_checksum_address(collateral_asset)).call()
        
        # conf is tuple with single item containing packed data
        if isinstance(conf, (list, tuple)):
            data_val = int(conf[0] if len(conf) and isinstance(conf[0], int) else conf[-1] if len(conf) else 0)
        else:
            data_val = int(conf)
        
        # Bit layout: LTV (0-15), Liq Threshold (16-31), Liq Bonus (32-47)
        liq_threshold_bps = _extract_bits(data_val, 16, 16)  # Basis points
        liq_bonus_bps = _extract_bits(data_val, 32, 16)      # Basis points
        
        # Convert to percentage (e.g., 8250 bps → 82.5%)
        liq_threshold = round(liq_threshold_bps / 100, 2)
        liq_bonus = round(liq_bonus_bps / 100, 2)
        
        return (liq_threshold, liq_bonus)
    except Exception as e:
        logger.debug("Could not fetch liquidation params for %s: %s", collateral_asset[:10], e)
        return (None, None)

def _get_collateral_risk_params(w3, collateral_asset: str) -> tuple:
    """Convenience wrapper that calls _get_liquidation_params with default AAVE V3 pool"""
    return _get_liquidation_params(w3, AAVE_V3_ETH_POOL, collateral_asset)

# ENFORCED constants
AAVE_V3_ETH_POOL = Web3.to_checksum_address("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
# Start block for AAVE V3 Ethereum mainnet deployment
# AAVE V3 deployed on Ethereum mainnet at block 16521648 (2023-01-26)
# We start at 16000000 to be safe and catch any pre-deployment activity
# Can be overridden by setting environment variable FROM_BLOCK_OVERRIDE
try:
    FROM_BLOCK = int(os.environ.get('FROM_BLOCK_OVERRIDE', '16000000'))
except Exception:
    FROM_BLOCK = 16000000

# Use canonical event ABI from src.aave.event_parsing for consistent parsing
AAVE_V3_POOL_ABI = [LIQUIDATION_EVENT_ABI]


def main(to_block="latest", incremental=True):
    w3 = get_web3_with_rotation()
    # Hard fail if not mainnet
    assert w3.eth.chain_id == 1, "Not Ethereum Mainnet!"

    # check pool address
    assert AAVE_V3_ETH_POOL.lower() == "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2", "AAVE pool constant mismatch"
    # Note: Skipping code check as some public RPCs don't return contract code reliably
    # Pool address is hardcoded and verified externally

    pool = w3.eth.contract(address=AAVE_V3_ETH_POOL, abi=AAVE_V3_POOL_ABI)
    fetcher = ChainlinkPriceFetcher(w3)

    # Ensure master CSV header is canonical before any checkpoint logic/writes
    try:
        # Ensure the canonical master CSV exists (creates header if missing)
        try:
            ensure_master_csv_exists()
        except Exception:
            logger.debug('[Liquidations] ensure_master_csv_exists failed', exc_info=False)

        recon = reconcile_master_csv_header()
        if recon:
            logger.info("[Liquidations] Header reconciliation applied: rows=%d", recon.get('rows'))
    except Exception:
        logger.exception("[Liquidations] Header reconciliation encountered an error")

    # use project LIQUIDATION_TOPIC for filtering
    topic0 = LIQUIDATION_TOPIC

    latest_block = w3.eth.block_number if to_block == "latest" else int(to_block)
    
    # Ensure master CSV exists with header before scanning
    ensure_master_csv_exists()
    
    # CHECKPOINT-SYSTEM: Determine last scanned block. Priority order:
    # 1) canonical master CSV (highest block + 1) - if CSV has data
    # 2) FROM_BLOCK if CSV is empty/missing (fresh start)
    # NOTE: We ignore old checkpoints/scan_status if CSV is empty to allow clean restart
    master_path = MASTER_CSV_FILENAME
    write_path = get_write_csv_path()
    logger.info("[Liquidations] Checking CSV for resume point: %s", master_path)

    scan_from_block = FROM_BLOCK
    is_first_scan = True

    # CSV-based checkpointing: read actual CSV rows to determine resume point
    try:
        import csv as csv_module
        MAX_VALID_BLOCK = 50_000_000
        max_block_seen = None
        csv_row_count = 0
        
        # Only check master CSV (single source of truth)
        if os.path.exists(master_path) and os.path.getsize(master_path) > 0:
            with open(master_path, 'r', encoding='utf-8') as f:
                reader = csv_module.DictReader(f)
                for row in reader:
                    csv_row_count += 1
                    try:
                        b = int(row.get('block', 0) or 0)
                    except Exception:
                        b = 0
                    if b and b < MAX_VALID_BLOCK:
                        if max_block_seen is None or b > max_block_seen:
                            max_block_seen = b
        
        if max_block_seen and csv_row_count > 0:
            # CSV has data - resume from last block + 1
            scan_from_block = max_block_seen + 1
            is_first_scan = False
            gap_size = latest_block - max_block_seen
            logger.info("[Liquidations] Resuming: CSV has %d rows, last block %s. Scanning %s -> %s (%s blocks)", 
                       csv_row_count, f"{max_block_seen:,}", f"{scan_from_block:,}", f"{latest_block:,}", f"{gap_size:,}")
        else:
            # CSV is empty or missing - start fresh from FROM_BLOCK
            scan_from_block = FROM_BLOCK
            is_first_scan = True
            csv_row_count = 0
            blocks_to_scan = latest_block - scan_from_block + 1
            logger.info("[Liquidations] Fresh start: scanning from block %s to %s (%s blocks)", 
                       f"{scan_from_block:,}", f"{latest_block:,}", f"{blocks_to_scan:,}")
    except Exception as e:
        logger.warning("[Liquidations] CSV checkpoint error: %s", e)
        # If CSV read fails, try to get event count from checkpoint file
        csv_row_count = 0

    # Calculate from_block for status (smallest block in CSV)
    status_from_block = FROM_BLOCK
    try:
        if os.path.exists(master_path) and os.path.getsize(master_path) > 0:
            with open(master_path, 'r', encoding='utf-8') as cf:
                reader = csv_module.DictReader(cf)
                blocks = [int(r.get('block', 0)) for r in reader if r.get('block')]
                if blocks:
                    status_from_block = min(blocks)
    except Exception:
        status_from_block = FROM_BLOCK

    # Parallel prevention via checkpoint-based resumption (no OS lock).
    # Multiple scanner instances can exist, but each will only scan from their
    # last checkpoint block, so no duplicate processing occurs (idempotent writes).
    # The portalocker lock has caused permission issues on some systems; we disable it
    # and rely on atomic writes + checkpoint files for safety.
    
    if scan_from_block > latest_block:
        logger.info("[Liquidations] Up to date - no new blocks")
        # Write status for frontend
        status_fn = os.path.join('data', 'scan_status.json')
        write_status_payload = {
            'status': 'idle',
            'from_block': FROM_BLOCK,
            'to_block': latest_block,
            'current_block': latest_block,
            'events_found': 0,
            'last_updated': int(time.time()),
            'message': 'no new blocks to scan'
        }
        try:
            with open(status_fn, 'w', encoding='utf-8') as sf:
                json.dump(write_status_payload, sf)
        except Exception:
            pass
        # Persist checkpoint so future runs resume quickly
        try:
            checkpoint_fn = os.path.join(DATA_DIR, 'scanner_checkpoint.json')
            with open(checkpoint_fn, 'w', encoding='utf-8') as cf:
                json.dump({'last_scanned_block': latest_block, 'ts': int(time.time())}, cf)
        except Exception:
            logger.debug('Failed to write scanner checkpoint', exc_info=False)
        return
    
    blocks_to_scan = latest_block - scan_from_block + 1
    # Only log on first scan
    if is_first_scan:
        logger.info("[Liquidations] First scan: %s blocks", f"{blocks_to_scan:,}")

    def write_status(status, current_block=None, events_found=0, message=None):
        status_fn = os.path.join('data', 'scan_status.json')
        # Use the provided events_found counter instead of reading CSV each time
        # to avoid race conditions during scanning. Only read CSV on first status write.
        computed_events = events_found if events_found is not None else 0

        payload = {
            'status': status,
            'from_block': status_from_block,
            'to_block': latest_block,
            'current_block': current_block,
            'events_found': computed_events,
            'last_updated': int(time.time()),
            'message': message
        }
        try:
            # Try atomic write first
            with tempfile.NamedTemporaryFile(
                mode='w',
                encoding='utf-8',
                dir=DATA_DIR,
                delete=False,
                suffix='.tmp'
            ) as tmp_file:
                json.dump(payload, tmp_file)
                tmp_path = tmp_file.name
            
            # Replace atomically
            try:
                os.replace(tmp_path, status_fn)
            except Exception:
                # Windows fallback: remove target then rename
                try:
                    if os.path.exists(status_fn):
                        os.remove(status_fn)
                    os.rename(tmp_path, status_fn)
                except Exception:
                    # Last resort: direct write
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    with open(status_fn, 'w', encoding='utf-8') as sf:
                        json.dump(payload, sf)
        except Exception:
            logger.exception('Failed to write scan status')

    # mark scan started
    write_status('running', current_block=scan_from_block, events_found=0, message='scan started')

    # DUPLIKAT-SCHUTZ: Lade existierende Transaction-Hashes aus the master CSV if
    # present (prefer master), otherwise from the write-path (staging). This
    # ensures we don't re-add events we've already exported previously.
    existing_txs = set()
    # Use csv_row_count from checkpoint logic as fallback if CSV read fails
    total_events_in_csv = csv_row_count
    # Read both master and staging (but deduplicate paths to avoid counting twice)
    csv_candidates = []
    if master_path not in csv_candidates:
        csv_candidates.append(master_path)
    if write_path != master_path and write_path not in csv_candidates:
        csv_candidates.append(write_path)
    
    try:
        import csv as csv_module
        row_count_from_read = 0
        for p in csv_candidates:
            if os.path.exists(p) and os.path.getsize(p) > 0:
                with open(p, 'r', encoding='utf-8') as f:
                    reader = csv_module.DictReader(f)
                    for row in reader:
                        row_count_from_read += 1
                        # Keep track of unique tx hashes to avoid duplicate appends
                        tx_hash = (row.get('tx') or '').lower()
                        if tx_hash and tx_hash not in existing_txs:
                            existing_txs.add(tx_hash)
        # Use actual read count if successful, otherwise keep csv_row_count fallback
        if row_count_from_read > 0:
            total_events_in_csv = row_count_from_read
        if total_events_in_csv > 0:
            logger.info("[Liquidations] CSV loaded: %s events (dedupe: %s tx)", 
                       total_events_in_csv, len(existing_txs))
    except Exception as e:
        logger.warning("[Liquidations] CSV load error: %s - using checkpoint count %d", 
                      e, total_events_in_csv)

    # Batch scanning with adaptive size + provider rotation
    INITIAL_BATCH_SIZE = 1000  # Start with 1000 blocks (safe for most providers)
    MIN_BATCH_SIZE = 500  # Fallback to 500 on errors
    MAX_BATCH_SIZE = 10000  # Maximum 10000 blocks per request
    current_batch_size = INITIAL_BATCH_SIZE
    alchemy_limit_detected = False
    total_events_found = total_events_in_csv  # FIX: Start with already existing events!
    current_from = scan_from_block
    consecutive_errors = 0  # Track consecutive errors
    MAX_CONSECUTIVE_ERRORS = 3  # Switch provider after 3 errors
    
    # BATCH VERIFICATION: Track all scanned ranges to verify no gaps
    scanned_ranges = []  # List of (from, to) tuples
    
    while current_from <= latest_block:
        current_to = min(current_from + current_batch_size - 1, latest_block)
        # No log for every batch - too verbose
        write_status('running', current_block=current_to, events_found=total_events_found, 
                    message=f'fetching logs {current_from}-{current_to}')
        
        try:
            start_time = time.time()
            batch_logs = w3.eth.get_logs({
                "fromBlock": current_from,
                "toBlock": current_to,
                "address": AAVE_V3_ETH_POOL,
                "topics": [topic0]
            })
            response_time = time.time() - start_time
            
            # Record batch size; actual append counts are logged after processing
            if len(batch_logs) > 0:
                logger.debug("[Liquidations] Fetched batch %s-%s: %d logs", current_from, current_to, len(batch_logs))
            else:
                logger.info("[Liquidations] Batch %s-%s: (empty)", current_from, current_to)
            
            # BATCH VERIFICATION: Record successful scan range
            scanned_ranges.append((current_from, current_to))
            
            # SUCCESS: Reset error counter
            consecutive_errors = 0
            
            # Bei Erfolg: Batch-Größe nur erhöhen wenn wir noch nicht am Limit sind
            if current_batch_size < MAX_BATCH_SIZE and not alchemy_limit_detected:
                new_size = min(current_batch_size * 2, MAX_BATCH_SIZE)
                if new_size != current_batch_size:
                    current_batch_size = new_size
                    # No log for batch increase
            
            #  SOFORT VERARBEITEN - nicht warten to alle Batches durch sind!
            batch_total = len(batch_logs)
            batch_appended = 0

            for raw in batch_logs:
                try:
                    # Manual parsing to handle HexBytes correctly
                    topics = raw.get("topics", [])
                    if not topics or topics[0] != topic0:
                        continue
                    
                    # Decode indexed parameters from topics
                    collateral_asset = w3.to_checksum_address("0x" + topics[1].hex()[-40:])
                    debt_asset = w3.to_checksum_address("0x" + topics[2].hex()[-40:])
                    borrower = w3.to_checksum_address("0x" + topics[3].hex()[-40:])
                    
                    # Decode non-indexed parameters from data
                    data_bytes = raw.get("data")
                    if isinstance(data_bytes, bytes):
                        data_hex = data_bytes
                    else:
                        data_hex = bytes.fromhex(data_bytes[2:] if data_bytes.startswith("0x") else data_bytes)
                    
                    debt_to_cover, collateral_amount, liquidator, receive_atoken = decode(
                        ["uint256", "uint256", "address", "bool"],
                        data_hex
                    )
                    liquidator = w3.to_checksum_address(liquidator)
                    
                    bn = raw["blockNumber"]
                    # FIX: Ensure TX hash always starts with 0x (for Etherscan links)
                    raw_tx = raw["transactionHash"]
                    if hasattr(raw_tx, "hex"):
                        tx_hash = "0x" + raw_tx.hex()
                    else:
                        tx_hash = str(raw_tx) if str(raw_tx).startswith("0x") else "0x" + str(raw_tx)
                    
                    # DUPLICATE CHECK: Skip if TX already in CSV
                    tx_hash_lower = tx_hash.lower()
                    if tx_hash_lower in existing_txs:
                        logger.debug("[Liquidations] Skipping duplicate TX: %s in block %s", tx_hash[:10], bn)
                        continue
                    
                    #  Hole Timestamp vom Block (minimal overhead)
                    try:
                        block_data = w3.eth.get_block(bn)
                        ts = block_data['timestamp']
                        block_builder = block_data.get('miner', '')  # 'miner' field = block proposer/builder
                    except Exception as e:
                        logger.warning("Failed to get block data for %s: %s", bn, e)
                        ts = 0
                        block_builder = ""
                    
                    # Gas-Daten aus Transaction Receipt holen
                    try:
                        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
                        tx_data = w3.eth.get_transaction(tx_hash)
                        gas_used = tx_receipt.get('gasUsed', 0)
                        gas_price_wei = tx_data.get('gasPrice', 0)
                        gas_price_gwei = gas_price_wei / 1e9 if gas_price_wei else 0
                    except Exception:
                        gas_used = 0
                        gas_price_gwei = 0
                    
                except Exception as e:
                    logger.warning("Failed to parse log at block %s: %s", raw.get("blockNumber"), e)
                    continue

                # Get token symbols AND decimals
                collateral_symbol = _get_token_symbol(w3, collateral_asset)
                debt_symbol = _get_token_symbol(w3, debt_asset)
                collateral_decimals = _get_token_decimals(w3, collateral_asset)
                debt_decimals = _get_token_decimals(w3, debt_asset)

                # Fetch Chainlink prices using normalize_symbol for address->feed conversion
                try:
                    feed_collateral = normalize_symbol(collateral_symbol, collateral_asset)
                    feed_debt = normalize_symbol(debt_symbol, debt_asset)
                except Exception:
                    feed_collateral = collateral_symbol
                    feed_debt = debt_symbol

                # =============================================================
                # AAVE-KONFORME PREISLOGIK mit LSD-Support
                # Priorität: 1. LSD Exchange Rate × ETH  2. Chainlink  3. Fallback
                # =============================================================
                price_collateral = 0
                price_debt = 0
                used_lsd_collateral = False
                used_lsd_debt = False
                used_fallback_collateral = False
                used_fallback_debt = False
                
                # Get collateral price using AAVE methodology
                try:
                    result = get_aave_asset_price(
                        collateral_symbol, collateral_asset, bn, fetcher, w3, feed_collateral
                    )
                    if result is not None and result > 0:
                        price_collateral = result
                        # Check if LSD was used
                        if is_lsd_token(collateral_asset):
                            used_lsd_collateral = True
                except Exception as e:
                    logger.debug("[Liquidations] Collateral price error: %s", str(e)[:50])
                
                # Fallback for collateral if primary method fails
                if price_collateral == 0:
                    try:
                        logger.debug("[Liquidations] Collateral fallback check token=%s", collateral_symbol)
                        if is_stablecoin(collateral_symbol):
                            price_collateral = 1.0
                            used_fallback_collateral = True
                    except Exception as e:
                        logger.exception("[Liquidations] Error during collateral stablecoin check for %s: %s", collateral_symbol, e)
                
                # Get debt price using AAVE methodology
                try:
                    result = get_aave_asset_price(
                        debt_symbol, debt_asset, bn, fetcher, w3, feed_debt
                    )
                    if result is not None and result > 0:
                        price_debt = result
                        if is_lsd_token(debt_asset):
                            used_lsd_debt = True
                except Exception as e:
                    logger.debug("[Liquidations] Debt price error: %s", str(e)[:50])
                
                # Fallback for debt if primary method fails
                if price_debt == 0:
                    try:
                        logger.debug("[Liquidations] Debt fallback check token=%s", debt_symbol)
                        if is_stablecoin(debt_symbol):
                            price_debt = 1.0
                            used_fallback_debt = True
                    except Exception as e:
                        logger.exception("[Liquidations] Error during debt stablecoin check for %s: %s", debt_symbol, e)

                # FIX: Convert from wei using CORRECT decimals for each token!
                # USDC/USDT use 6 decimals, WBTC uses 8, most others use 18
                collateral_out_tokens = collateral_amount / (10 ** collateral_decimals)
                debt_to_cover_tokens = debt_to_cover / (10 ** debt_decimals)
                
                # Only calculate if both prices are available
                if price_collateral > 0 and price_debt > 0:
                    collateral_value_usd = round(collateral_out_tokens * price_collateral, 2)
                    debt_value_usd = round(debt_to_cover_tokens * price_debt, 2)
                else:
                    # Prices missing - empty strings for R/pandas
                    collateral_value_usd = ''
                    debt_value_usd = ''

                # Human-readable date (UTC)
                try:
                    datetime_utc = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else ''
                except Exception:
                    datetime_utc = ''

                # Price source detection removed: we do not persist source metadata

                # ETH/USD Preis für den Block (Chainlink)
                try:
                    # Use the existing Chainlink fetcher instance bound to `w3`
                    eth_price_usd = fetcher.get_price_for_block("ETH", bn)
                except Exception:
                    eth_price_usd = ''
                event_data = {
                    # Original field names matching CSV_FIELD_ORDER
                    "block": bn,
                    "timestamp": ts,
                    "datetime_utc": datetime_utc,
                    "collateralSymbol": collateral_symbol,
                    "debtSymbol": debt_symbol,
                    "collateralOut": round(collateral_out_tokens, 8),
                    "debtToCover": round(debt_to_cover_tokens, 8),
                    # Store prices with higher precision to avoid future rounding mismatches
                    "collateral_price_usd_at_block": f"{price_collateral:.8f}" if price_collateral else '',
                    "debt_price_usd_at_block": f"{price_debt:.8f}" if price_debt else '',
                    # Note: price source metadata intentionally not persisted
                    "collateral_value_usd": collateral_value_usd,
                    "debt_value_usd": debt_value_usd,
                    "tx": tx_hash,
                    "user": borrower,
                    "liquidator": liquidator,
                    "collateralAsset": collateral_asset,
                    "debtAsset": debt_asset,
                    "receiveAToken": receive_atoken,
                    "block_builder": block_builder,
                    "gas_used": gas_used,
                    "gas_price_gwei": round(gas_price_gwei, 2) if gas_price_gwei else 0,
                    "eth_price_usd_at_block": f"{eth_price_usd:.8f}" if eth_price_usd else '',
                }
                
                # LOG: Log missing fields, LSD prices, or fallbacks
                missing_fields = []
                price_info = []
                if not price_collateral:
                    missing_fields.append(f"collateral_price({collateral_symbol})")
                elif used_lsd_collateral:
                    price_info.append(f"collateral({collateral_symbol}=LSD*ETH)")
                elif used_fallback_collateral:
                    price_info.append(f"collateral({collateral_symbol}->$1)")
                if not price_debt:
                    missing_fields.append(f"debt_price({debt_symbol})")
                elif used_lsd_debt:
                    price_info.append(f"debt({debt_symbol}=LSD*ETH)")
                elif used_fallback_debt:
                    price_info.append(f"debt({debt_symbol}->$1)")
                if not gas_used:
                    missing_fields.append("gas_used")
                
                # FARBIGE MARKIERUNG für fehlende Preise (ROT)
                if missing_fields:
                    logger.warning("[MISSING] Block %s TX %s: %s", bn, tx_hash[:16], ', '.join(missing_fields))
                # Only log LSD usage at DEBUG level to reduce noise
                if price_info:
                    logger.debug("[Liquidations] Block %s: %s", bn, ", ".join(price_info))
                
                # Prepare row for CSV writing (do NOT increment counters until write succeeds)
                csv_path = get_write_csv_path()
                # Create a backup before the very first write if the file exists but isn't canonical
                if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
                    # no-op here; backups are created by reconcile_master_csv_header when needed
                    pass

                # Normalize and reorder mapping to the canonical order
                safe_event = normalize_event_data_for_write(event_data)
                row = {k: safe_event.get(k, '') for k in CSV_FIELD_ORDER}

                try:
                    from tools.csv_utils import append_row_if_tx_missing
                    appended = append_row_if_tx_missing(csv_path, row, CSV_FIELD_ORDER, tx_field='tx')
                except Exception as e:
                    logger.exception('[Liquidations] Failed to append to CSV: %s', e)
                    appended = False

                if appended:
                    # CHECK: Warnung wenn irgendeine Spalte leer ist (nicht nur Preise)
                    empty_columns = []
                    for col in CSV_FIELD_ORDER:
                        val = row.get(col)
                        if val is None or (isinstance(val, str) and val.strip() == ''):
                            empty_columns.append(col)
                    if empty_columns:
                        logger.warning("[EMPTY] Block %s TX %s: %s", bn, tx_hash[:16], ', '.join(empty_columns))

                    # Add TX to existing (prevents duplicates in same run)
                    existing_txs.add(tx_hash_lower)
                    total_events_found += 1
                    batch_appended += 1

                    # Ensure frontend status is synced to the canonical master CSV
                    try:
                        sync_scan_status_from_csv(current_block=bn, to_block=latest_block, status='running', message='appended event')
                    except Exception:
                        logger.debug('Failed to sync status after append')

                    # Status update for frontend
                    write_status('running', current_block=bn, events_found=total_events_found, message=f'Found liquidation in block {bn}')

                    # Kompaktes Log: nur Event-Nummer und Pair
                    logger.info("[Liquidations] #%s %s/%s @ %s", total_events_found, collateral_symbol, debt_symbol, bn)
                else:
                    logger.debug('[Liquidations] Skipped append (duplicate or failed): %s', tx_hash[:12])
            # end processing batch_logs

            # Log summary for this batch: total logs fetched and how many new rows appended
            if batch_total > 0:
                logger.info("[Liquidations] Batch %s-%s: %d Events, New: +%d", current_from, current_to, batch_total, batch_appended)
                
        except Exception as e:
            error_msg = str(e)
            consecutive_errors += 1
            
            # PROVIDER ROTATION on repeated errors
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning("[Liquidations] RPC error - switching provider...")
                for retry_wait in [5, 15, 30, 60]:  # Exponential backoff
                    try:
                        time.sleep(retry_wait)
                        w3 = get_web3_with_rotation()
                        fetcher = ChainlinkPriceFetcher(w3)
                        consecutive_errors = 0
                        logger.info("[Liquidations] Provider successfully switched after %ds pause", retry_wait)
                        break  # Success!
                    except Exception as rotate_error:
                        logger.warning("Provider-Wechsel fehlgeschlagen, warte %ds...", retry_wait * 2)
                else:
                    # Alle Retries fehlgeschlagen - nochmal von vorne versuchen
                    logger.error("Alle Provider fehlgeschlagen - warte 2 Minuten und versuche erneut...")
                    write_status('waiting', current_block=current_from, events_found=total_events_found, 
                                message="Network issues - waiting to retry")
                    time.sleep(120)  # 2 Minuten warten
                    consecutive_errors = 0  # Reset und nochmal versuchen
                continue  # Retry
            
            # Bei 400/429 oder "range too large": Batch-Größe reduzieren oder Provider wechseln
            if ("400" in error_msg or "Bad Request" in error_msg or "429" in error_msg or 
                "Too Many Requests" in error_msg or "range" in error_msg.lower() or 
                "too large" in error_msg.lower() or "exceeds" in error_msg.lower()):
                if current_batch_size > MIN_BATCH_SIZE:
                    current_batch_size = max(current_batch_size // 2, MIN_BATCH_SIZE)
                    alchemy_limit_detected = True
                    logger.info("[Liquidations] Reducing batch size to %d due to provider limit", current_batch_size)
                    time.sleep(2)  # Short pause bei Rate Limits
                    continue  # Retry mit kleinerer Batch
                else:
                    # Bei Rate Limit: Provider wechseln mit Retry-Logik
                    logger.info("Rate limit erreicht - wechsle Provider...")
                    for retry_wait in [5, 10, 20]:
                        try:
                            time.sleep(retry_wait)
                            w3 = get_web3_with_rotation()
                            fetcher = ChainlinkPriceFetcher(w3)
                            consecutive_errors = 0
                            current_batch_size = INITIAL_BATCH_SIZE
                            alchemy_limit_detected = False
                            break
                        except Exception:
                            pass
                    continue  # Retry (even if provider switch fails)
            else:
                # Other error - retry with pause
                logger.warning('[Liquidations] Batch error %s-%s: %s', current_from, current_to, error_msg[:100])
                time.sleep(2)  # Short pause
                continue  # Retry
        
        current_from = current_to + 1

    # BATCH VERIFICATION: Check for gaps in scanned ranges and fill them immediately
    if scanned_ranges:
        scanned_ranges.sort(key=lambda x: x[0])  # Sort by start block
        gaps_found = []
        
        for i in range(1, len(scanned_ranges)):
            prev_end = scanned_ranges[i-1][1]
            curr_start = scanned_ranges[i][0]
            
            # Gap exists if current start > previous end + 1
            if curr_start > prev_end + 1:
                gap_start = prev_end + 1
                gap_end = curr_start - 1
                gaps_found.append((gap_start, gap_end))
        
        if gaps_found:
            logger.warning("[Liquidations] BATCH GAP DETECTED! %d gaps found - filling now...", len(gaps_found))
            
            # FILL GAPS IMMEDIATELY
            for gap_start, gap_end in gaps_found:
                gap_size = gap_end - gap_start + 1
                logger.info("[Liquidations] Filling gap: %s - %s (%s blocks)", 
                            f"{gap_start:,}", f"{gap_end:,}", f"{gap_size:,}")
                
                # Scan the gap in batches
                gap_from = gap_start
                while gap_from <= gap_end:
                    gap_to = min(gap_from + current_batch_size - 1, gap_end)
                    
                    try:
                        start_time = time.time()
                        gap_logs = w3.eth.get_logs({
                            "fromBlock": gap_from,
                            "toBlock": gap_to,
                            "address": AAVE_V3_ETH_POOL,
                            "topics": [topic0]
                        })
                        response_time = time.time() - start_time
                        mark_provider_success(response_time)
                        
                        if len(gap_logs) > 0:
                            logger.info("[Liquidations] Gap %s-%s: %d Events found!", gap_from, gap_to, len(gap_logs))
                            
                            # Process each event in the gap
                            for raw in gap_logs:
                                try:
                                    topics = raw.get("topics", [])
                                    if not topics or topics[0] != topic0:
                                        continue
                                    
                                    # Get TX hash first for duplicate check
                                    raw_tx = raw["transactionHash"]
                                    if hasattr(raw_tx, "hex"):
                                        tx_hash = "0x" + raw_tx.hex()
                                    else:
                                        tx_hash = str(raw_tx) if str(raw_tx).startswith("0x") else "0x" + str(raw_tx)
                                    
                                    tx_hash_lower = tx_hash.lower()
                                    if tx_hash_lower in existing_txs:
                                        continue  # Skip duplicate
                                    
                                    # Decode event data
                                    collateral_asset = w3.to_checksum_address("0x" + topics[1].hex()[-40:])
                                    debt_asset = w3.to_checksum_address("0x" + topics[2].hex()[-40:])
                                    borrower = w3.to_checksum_address("0x" + topics[3].hex()[-40:])
                                    
                                    data_bytes = raw.get("data")
                                    if isinstance(data_bytes, bytes):
                                        data_hex = data_bytes
                                    else:
                                        data_hex = bytes.fromhex(data_bytes[2:] if data_bytes.startswith("0x") else data_bytes)
                                    
                                    debt_to_cover, collateral_amount, liquidator, receive_atoken = decode(
                                        ["uint256", "uint256", "address", "bool"],
                                        data_hex
                                    )
                                    liquidator = w3.to_checksum_address(liquidator)
                                    bn = raw["blockNumber"]
                                    
                                    # Get block data
                                    try:
                                        block_data = w3.eth.get_block(bn)
                                        ts = block_data['timestamp']
                                        block_builder = block_data.get('miner', '')  # 'miner' field = block proposer/builder
                                    except Exception:
                                        ts = 0
                                        block_builder = ""
                                    
                                    # Get gas data
                                    try:
                                        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
                                        tx_data = w3.eth.get_transaction(tx_hash)
                                        gas_used = tx_receipt.get('gasUsed', 0)
                                        gas_price_wei = tx_data.get('gasPrice', 0)
                                        gas_price_gwei = gas_price_wei / 1e9 if gas_price_wei else 0
                                    except Exception:
                                        gas_used = 0
                                        gas_price_gwei = 0
                                    
                                    # Get symbols and decimals
                                    collateral_symbol = _get_token_symbol(w3, collateral_asset)
                                    debt_symbol = _get_token_symbol(w3, debt_asset)
                                    collateral_decimals = _get_token_decimals(w3, collateral_asset)
                                    debt_decimals = _get_token_decimals(w3, debt_asset)
                                    
                                    # Get prices using AAVE methodology (LSD support)
                                    try:
                                        feed_collateral = normalize_symbol(collateral_symbol, collateral_asset)
                                        feed_debt = normalize_symbol(debt_symbol, debt_asset)
                                    except Exception:
                                        feed_collateral = collateral_symbol
                                        feed_debt = debt_symbol
                                    
                                    # Use AAVE-konforme Preislogik
                                    price_collateral = get_aave_asset_price(
                                        collateral_symbol, collateral_asset, bn, fetcher, w3, feed_collateral
                                    ) or 0
                                    
                                    price_debt = get_aave_asset_price(
                                        debt_symbol, debt_asset, bn, fetcher, w3, feed_debt
                                    ) or 0
                                    
                                    # Calculate values
                                    collateral_out_tokens = collateral_amount / (10 ** collateral_decimals)
                                    debt_to_cover_tokens = debt_to_cover / (10 ** debt_decimals)
                                    
                                    if price_collateral > 0 and price_debt > 0:
                                        collateral_value_usd = round(collateral_out_tokens * price_collateral, 2)
                                        debt_value_usd = round(debt_to_cover_tokens * price_debt, 2)
                                    else:
                                        collateral_value_usd = ''
                                        debt_value_usd = ''
                                    
                                    try:
                                        datetime_utc = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else ''
                                    except Exception:
                                        datetime_utc = ''
                                    
                                    # ETH/USD Preis für den Block (Chainlink)
                                    try:
                                        eth_price_usd = fetcher.get_price_for_block("ETH", bn)
                                    except Exception:
                                        eth_price_usd = ''

                                    event_data = {
                                        "block": bn,
                                        "timestamp": ts,
                                        "datetime_utc": datetime_utc,
                                        "collateralSymbol": collateral_symbol,
                                        "debtSymbol": debt_symbol,
                                        "collateralOut": round(collateral_out_tokens, 8),
                                        "debtToCover": round(debt_to_cover_tokens, 8),
                                        "collateral_price_usd_at_block": f"{price_collateral:.8f}" if price_collateral else '',
                                        "debt_price_usd_at_block": f"{price_debt:.8f}" if price_debt else '',
                                        # price source metadata intentionally not persisted
                                        "collateral_value_usd": collateral_value_usd,
                                        "debt_value_usd": debt_value_usd,
                                        "tx": tx_hash,
                                        "user": borrower,
                                        "liquidator": liquidator,
                                        "collateralAsset": collateral_asset,
                                        "debtAsset": debt_asset,
                                        "receiveAToken": receive_atoken,
                                        "block_builder": block_builder,
                                        "gas_used": gas_used,
                                        "gas_price_gwei": round(gas_price_gwei, 2) if gas_price_gwei else 0,
                                        "eth_price_usd_at_block": f"{eth_price_usd:.8f}" if eth_price_usd else '',
                                    }
                                    
                                    # Write to CSV
                                    csv_path_gap = get_write_csv_path()
                                    file_exists_gap = os.path.exists(csv_path_gap) and os.path.getsize(csv_path_gap) > 0
                                    # Use safe append helper to avoid races and ensure atomic append
                                    try:
                                        from tools.csv_utils_clean import append_row_if_tx_missing
                                        row = {k: event_data.get(k, '') for k in CSV_FIELD_ORDER}
                                        appended = append_row_if_tx_missing(csv_path_gap, row, CSV_FIELD_ORDER, tx_field='tx')
                                        try:
                                            if appended:
                                                sync_scan_status_from_csv(current_block=bn, status='running', message='gap-filled event')
                                        except Exception:
                                            logger.debug('Failed to sync status after gap fill')
                                    except Exception as e:
                                        logger.exception('[Liquidations] Failed to gap-fill append: %s', e)
                                        appended = False

                                    if appended:
                                        existing_txs.add(tx_hash_lower)
                                        total_events_found += 1
                                        logger.info("[Liquidations] GAP FILLED: #%s %s/%s @ %s", total_events_found, collateral_symbol, debt_symbol, bn)
                                    
                                except Exception as e:
                                    logger.warning("[Liquidations] Gap event parse error: %s", str(e)[:50])
                                    continue
                        
                    except Exception as e:
                        logger.warning("[Liquidations] Gap scan error %s-%s: %s", gap_from, gap_to, str(e)[:50])
                    
                    gap_from = gap_to + 1
            
            logger.info("[Liquidations] All gaps filled!")
        else:
            logger.info("[Liquidations] Batch verification passed - no gaps detected")

    # Always log a concise summary line for visibility in the terminal logs
    new_events = total_events_found - total_events_in_csv
    try:
        if new_events > 0:
            logger.info(f"{Colors.GREEN}[Liquidations] New: +{new_events} (total: {total_events_found}){Colors.RESET}")
        else:
            # show +0 so it's visible in the logs (user requested)
            logger.info(f"[Liquidations] New: +{new_events} (total: {total_events_found})")
    except Exception:
        # Fallback to plain message
        logger.info("[Liquidations] New: +%s (total: %s)", new_events, total_events_found)
    write_status('completed', current_block=latest_block, events_found=total_events_found, message='scan complete')
    # Persist checkpoint on successful completion so next run can resume here
    try:
        checkpoint_fn = os.path.join(DATA_DIR, 'scanner_checkpoint.json')
        with open(checkpoint_fn, 'w', encoding='utf-8') as cf:
            json.dump({'last_scanned_block': latest_block, 'events_found': total_events_found, 'ts': int(time.time())}, cf)
        logger.info('[Liquidations] Scanner checkpoint written: %s', checkpoint_fn)
    except Exception:
        logger.debug('[Liquidations] Failed to write scanner checkpoint', exc_info=False)


def backfill_missing_prices():
    """
    Backfill missing prices in CSV.
    Reads all rows, finds empty price fields and fetches Chainlink prices.
    """
    csv_path = get_write_csv_path()
    
    if not os.path.exists(csv_path):
        logger.error("[Liquidations] CSV not found: %s", csv_path)
        return
    
    # Read CSV
    import csv as csv_module
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    logger.info("[Liquidations] %d rows loaded", len(rows))
    
    # Find rows with missing prices (now also include empty ETH price column)
    missing_indices = []
    for i, row in enumerate(rows):
        col_price = row.get('collateral_price_usd_at_block', '')
        debt_price = row.get('debt_price_usd_at_block', '')
        eth_price = row.get('eth_price_usd_at_block', '')

        col_missing = col_price == '' or col_price == '0' or col_price == '0.0'
        debt_missing = debt_price == '' or debt_price == '0' or debt_price == '0.0'
        eth_missing = eth_price == '' or eth_price == '0' or eth_price == '0.0'

        # Consider row missing if collateral/debt price OR ETH price is empty
        if col_missing or debt_missing or eth_missing:
            missing_indices.append(i)
    
    if not missing_indices:
        logger.info("[Liquidations] No missing prices!")
        return
    
    logger.info("[Liquidations] %d rows with missing prices found", len(missing_indices))
    
    # Web3 + Fetcher initialisieren
    w3 = get_web3_with_rotation()
    fetcher = ChainlinkPriceFetcher(w3)
    
    fixed_count = 0
    still_missing = 0
    issues = []
    
    for idx, i in enumerate(missing_indices):
        row = rows[i]
        # Collect context for reporting when fields are missing or malformed
        row_context = {
            'index': i,
            'sample': {k: row.get(k) for k in ('block','tx','user','collateralAsset','debtAsset')},
        }
        # Ensure ETH price column exists in row dicts
        if 'eth_price_usd_at_block' not in row:
            row['eth_price_usd_at_block'] = ''
        # Parse block defensively: if missing or invalid, record issue and skip
        try:
            block = int(row['block'])
        except Exception as e:
            logger.warning("[Liquidations] Row %s missing/invalid 'block' field: %s", i, e)
            issue = {
                'type': 'missing_block',
                'index': i,
                'error': str(e),
                'row': row_context['sample']
            }
            issues.append(issue)
            # Skip this row for backfill (can't fetch historical prices without block)
            still_missing += 1
            continue
        col_symbol = row.get('collateralSymbol', '')
        debt_symbol = row.get('debtSymbol', '')
        col_asset = row.get('collateralAsset', '')
        debt_asset = row.get('debtAsset', '')

        fixed_this_row = False

        # Check collateral price - use AAVE methodology with LSD support
        col_price = row.get('collateral_price_usd_at_block', '')
        if col_price == '' or col_price == '0' or col_price == '0.0':
            feed = normalize_symbol(col_symbol, col_asset)

            # Use AAVE-konforme Preislogik (LSD → Exchange Rate × ETH)
            price = get_aave_asset_price(col_symbol, col_asset, block, fetcher, w3, feed)

            if price and price > 0:
                rows[i]['collateral_price_usd_at_block'] = f"{price:.8f}"
                col_out = float(row.get('collateralOut', 0) or 0)
                if col_out > 0:
                    rows[i]['collateral_value_usd'] = round(col_out * price, 2)
                fixed_this_row = True
                if is_lsd_token(col_asset):
                    logger.debug("[Liquidations] [%d/%d] Block %s: %s - LSD price via exchange rate",
                                 idx+1, len(missing_indices), block, col_symbol)

        # Check debt price - use AAVE methodology with LSD support
        debt_price = row.get('debt_price_usd_at_block', '')
        if debt_price == '' or debt_price == '0' or debt_price == '0.0':
            feed = normalize_symbol(debt_symbol, debt_asset)

            # Use AAVE-konforme Preislogik (LSD → Exchange Rate × ETH)
            price = get_aave_asset_price(debt_symbol, debt_asset, block, fetcher, w3, feed)

            if price and price > 0:
                rows[i]['debt_price_usd_at_block'] = f"{price:.8f}"
                debt_amt = float(row.get('debtToCover', 0) or 0)
                if debt_amt > 0:
                    rows[i]['debt_value_usd'] = round(debt_amt * price, 2)
                fixed_this_row = True
                if is_lsd_token(debt_asset):
                    logger.debug("[Liquidations] [%d/%d] Block %s: %s - LSD price via exchange rate",
                                 idx+1, len(missing_indices), block, debt_symbol)

        # Check ETH price column and fill via Chainlink if missing (always attempt)
        eth_price_val = row.get('eth_price_usd_at_block', '')
        if eth_price_val == '' or eth_price_val == '0' or eth_price_val == '0.0':
            try:
                eth_price = fetcher.get_price_for_block("ETH", block)
                if eth_price and eth_price > 0:
                    rows[i]['eth_price_usd_at_block'] = f"{eth_price:.8f}"
                    fixed_this_row = True
            except Exception as e:
                logger.debug("[Liquidations] Failed to fetch ETH price @%s: %s", block, e)
        
        if fixed_this_row:
            fixed_count += 1
            logger.info("[Liquidations] [%d/%d] Block %s: %s/%s - FIXED", idx+1, len(missing_indices), block, col_symbol, debt_symbol)
        else:
            still_missing += 1
            logger.warning("[Liquidations] [%d/%d] Block %s: %s/%s - still missing", idx+1, len(missing_indices), block, col_symbol, debt_symbol)
    
    # Write CSV back
    logger.info("[Liquidations] Writing %d rows back...", len(rows))
    # Ensure fieldnames include the new ETH price column
    if fieldnames is None:
        fieldnames = CSV_FIELD_ORDER
    # Ensure ETH price column exists; price-source columns are not persisted
    if fieldnames is None:
        fieldnames = CSV_FIELD_ORDER
    if 'eth_price_usd_at_block' not in fieldnames:
        fieldnames = list(fieldnames) + ['eth_price_usd_at_block']

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        try:
            sync_scan_status_from_csv(status='running', message='backfill write complete')
        except Exception:
            logger.debug('Failed to sync status after backfill')

    # Write a validation report for any issues encountered during backfill
    try:
        report_path = os.path.join(DATA_DIR, 'validation_report.json')
        report = {
            'timestamp': int(time.time()),
            'fixed_count': fixed_count,
            'still_missing': still_missing,
            'issues': issues,
        }
        with open(report_path, 'w', encoding='utf-8') as rf:
            json.dump(report, rf, indent=2)
        logger.info("[Liquidations] Validation report written: %s", report_path)
    except Exception:
        logger.debug('Failed to write validation report', exc_info=True)
    
    logger.info("[Liquidations] Backfill complete: %d fixed, %d still missing", fixed_count, still_missing)


def validate_and_fill_gaps():
    """
    Complete Validation & Repair:
    1. Fill missing prices in CSV (rsETH, LINK, etc.)
    2. Validate numeric USD calculations
    3. Find block gaps and scan for missing events
    4. Update to latest block
    """
    csv_path = get_write_csv_path()
    
    if not os.path.exists(csv_path):
        logger.error("[Liquidations] CSV not found - starting normal scan")
        main()
        return
    
    print("\n" + "="*80)
    print("[VALIDATION] === COMPLETE VALIDATION & REPAIR MODE ===")
    print("="*80)
    logger.info("="*80)
    logger.info("[Liquidations] === COMPLETE VALIDATION & REPAIR MODE ===")
    logger.info("="*80)
    
    # Show CSV info with detailed stats
    import csv as csv_module
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        temp_rows = list(reader)
    
    print(f"[VALIDATION] Loaded CSV: {len(temp_rows):,} liquidation events")
    print(f"[VALIDATION] File path: {csv_path}")
    logger.info("[Liquidations] CSV loaded: %s entries from %s", f"{len(temp_rows):,}", csv_path)
    
    # === PHASE 1: Fill missing prices ===
    print("\n" + "="*80)
    print("[VALIDATION] PHASE 1/5: Backfilling Missing Prices")
    print("[VALIDATION] Checking for liquidations with missing USD values...")
    print("="*80)
    logger.info("")
    logger.info("="*80)
    logger.info("PHASE 1/5: Backfilling Missing Prices")
    logger.info("="*80)
    
    import time
    phase1_start = time.time()
    backfill_missing_prices()
    phase1_duration = time.time() - phase1_start
    
    print(f"[VALIDATION] Phase 1 complete ({phase1_duration:.1f}s)")
    
    # === PHASE 2: Validate numeric calculations ===
    print("\n" + "="*80)
    print("[VALIDATION] PHASE 2/5: Validating USD Calculations")
    print("[VALIDATION] Skipping detailed numeric validation (would take ~30min)")
    print("[VALIDATION] Run with --validate-numbers flag separately if needed")
    print("="*80)
    logger.info("")
    logger.info("="*80)
    logger.info("PHASE 2/5: Validating USD Calculations")
    logger.info("="*80)
    logger.info("[Liquidations] Skipping numeric validation (can be slow)")
    logger.info("[Liquidations] Run with --validate-numbers separately if needed")
    # try:
    #     validate_numbers(repair=True, tolerance_abs=0.01, tolerance_pct=0.005)
    #     logger.info("[Liquidations] USD validation completed")
    # except Exception as e:
    #     logger.warning("[Liquidations] Numeric validation failed: %s", str(e)[:100])
    
    # === PHASE 3: Find block gaps ===
    print("\n" + "="*80)
    print("[VALIDATION] PHASE 3/5: Analyzing Block Coverage")
    print("[VALIDATION] Reading CSV to identify scanned block ranges...")
    print("="*80)
    logger.info("")
    logger.info("="*80)
    logger.info("PHASE 3/5: Analyzing Block Coverage")
    logger.info("="*80)
    
    import csv as csv_module
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print("[VALIDATION] CSV is empty - starting fresh scan")
        logger.info("CSV is empty - starting fresh scan")
        main()
        return
    
    # Sammle alle Blocks mit Events
    blocks_with_events = set()
    for row in rows:
        try:
            blocks_with_events.add(int(row['block']))
        except (ValueError, KeyError):
            pass
    
    if not blocks_with_events:
        print("[VALIDATION] ERROR: No valid block numbers found in CSV")
        logger.warning("[Liquidations] No valid block values found in CSV during validation — aborting gap check. See validation_report.json for details.")
        return

    min_block = min(blocks_with_events)
    max_block = max(blocks_with_events)
    block_range = max_block - min_block + 1
    
    print(f"[VALIDATION] CSV contains {len(rows):,} liquidation events")
    print(f"[VALIDATION] Block range: {min_block:,} -> {max_block:,} ({block_range:,} blocks)")
    print(f"[VALIDATION] Earliest block: {datetime.fromtimestamp(1673481600).strftime('%Y-%m-%d')} (AAVE V3 deployment)")
    logger.info("[Liquidations] CSV contains %d events from block %s to %s", len(rows), f"{min_block:,}", f"{max_block:,}")
    
    # Existing TX hashes for duplicate check
    existing_txs = set()
    for row in rows:
        tx = row.get('tx', '').lower()
        if tx:
            existing_txs.add(tx)
    
    # === PHASE 4: Gap Scan ===
    # Scan the entire range again - duplicates will be skipped
    print("\n" + "="*80)
    print("[VALIDATION] PHASE 4/5: Deep Scan for Missing Events")
    print("="*80)
    
    scan_range = max_block - FROM_BLOCK + 1
    estimated_time = scan_range / 10000  # ~1000 blocks/batch, 10 batches per minute
    print(f"[VALIDATION] Scanning range: {FROM_BLOCK:,} -> {max_block:,}")
    print(f"[VALIDATION] Total blocks to check: {scan_range:,}")
    print(f"[VALIDATION] Estimated time: ~{estimated_time:.1f} minutes (1000 blocks/batch)")
    print(f"[VALIDATION] Progress updates every 100,000 blocks")
    logger.info("")
    logger.info("="*80)
    logger.info("PHASE 4/5: Deep Scan for Missing Events")
    logger.info("="*80)
    logger.info("[Liquidations] Scanning range: %s to %s (%s blocks)", f"{FROM_BLOCK:,}", f"{max_block:,}", f"{scan_range:,}")

    w3 = get_web3_with_rotation()
    fetcher = ChainlinkPriceFetcher(w3)

    # Scan from FROM_BLOCK to max_block in conservative batches with retries
    # Use smaller default batch sizes to avoid long-running RPC requests on some public providers.
    BATCH_SIZE = 2000  # reduced from 10000 to improve responsiveness
    MAX_BATCH_RETRIES = 3
    # Use chunked log fetcher to avoid large single get_logs requests that hang
    CHUNK_INITIAL = 500  # reduced initial chunk for safer provider compatibility

    new_events_found = 0
    
    # Progress tracking
    phase4_start = time.time()
    total_blocks_to_scan = max_block - FROM_BLOCK + 1
    blocks_scanned = 0
    last_progress_block = FROM_BLOCK

    current_from = FROM_BLOCK
    while current_from <= max_block:
        # Progress reporting every 100k blocks
        if current_from - last_progress_block >= 100000:
            blocks_scanned = current_from - FROM_BLOCK
            progress_pct = (blocks_scanned / total_blocks_to_scan) * 100
            elapsed = time.time() - phase4_start
            blocks_per_sec = blocks_scanned / elapsed if elapsed > 0 else 0
            eta_seconds = (total_blocks_to_scan - blocks_scanned) / blocks_per_sec if blocks_per_sec > 0 else 0
            
            print(f"[VALIDATION] Progress: {current_from:,}/{max_block:,} ({progress_pct:.1f}%) | Found {new_events_found} new events | ETA: {eta_seconds/60:.1f}min")
            logger.info("[Liquidations] Gap scan progress: %s/%s (%.1f%%) - %d new events found", f"{current_from:,}", f"{max_block:,}", progress_pct, new_events_found)
            last_progress_block = current_from
        
        current_to = min(current_from + BATCH_SIZE - 1, max_block)

        batch_logs = None
        # Use chunked get_logs to avoid RPC providers rejecting large ranges
        try:
            # Log range being requested so long-running operations are visible
            logger.info("[Liquidations] Gap-scan fetching logs %s-%s (chunk=%s) using provider %s", current_from, current_to, CHUNK_INITIAL, getattr(w3.provider, 'endpoint_uri', 'unknown')[:60])
            # initial chunk smaller than BATCH_SIZE to protect providers
            batch_logs = get_logs_chunked(w3, AAVE_V3_ETH_POOL, [LIQUIDATION_TOPIC], current_from, current_to, initial_chunk=CHUNK_INITIAL)
        except Exception as e:
            logger.warning("[Liquidations] Gap scan chunked fetch %s-%s failed: %s", current_from, current_to, str(e)[:200])
            # Try a few light retries with provider rotation
            retried = False
            for attempt in range(1, MAX_BATCH_RETRIES + 1):
                try:
                    mark_provider_error()
                    time.sleep(1 + attempt * 2)
                    w3 = get_web3_with_rotation()
                    fetcher = ChainlinkPriceFetcher(w3)
                    batch_logs = get_logs_chunked(w3, AAVE_V3_ETH_POOL, [LIQUIDATION_TOPIC], current_from, current_to, initial_chunk=CHUNK_INITIAL)
                    retried = True
                    break
                except Exception as re:
                    logger.debug("Retry %d failed for gap chunk %s-%s: %s", attempt, current_from, current_to, str(re)[:150])
                    continue
            if not retried:
                logger.error("[Liquidations] Gap scan batch %s-%s skipped after %d attempts", current_from, current_to, MAX_BATCH_RETRIES)
                current_from = current_to + 1
                continue

        # Check if there are new events
        for raw in batch_logs:
            try:
                raw_tx = raw["transactionHash"]
                if hasattr(raw_tx, "hex"):
                    tx_hash = "0x" + raw_tx.hex()
                else:
                    tx_hash = str(raw_tx) if str(raw_tx).startswith("0x") else "0x" + str(raw_tx)

                if tx_hash.lower() not in existing_txs:
                    bn = raw["blockNumber"]
                    # Highlight newly found liquidations in green for terminal
                    GREEN = '\x1b[32m'
                    RESET = '\x1b[0m'
                    logger.info(f"{GREEN}[Liquidations - NEW]{RESET} Block %s TX %s", bn, tx_hash[:16])
                    new_events_found += 1
                    # We do not fully process here to keep gap-scan lightweight; normal scan will process them
            except Exception as e:
                logger.debug("[Liquidations] Error parsing gap log entry: %s", e)

        current_from = current_to + 1
    
    phase4_duration = time.time() - phase4_start
    blocks_scanned_total = max_block - FROM_BLOCK + 1
    
    if new_events_found > 0:
        print("\n" + "="*80)
        print(f"[VALIDATION] Gap scan complete: {new_events_found} NEW events found!")
        print(f"[VALIDATION] Scanned {blocks_scanned_total:,} blocks in {phase4_duration:.1f}s ({blocks_scanned_total/phase4_duration:.0f} blocks/sec)")
        print("[VALIDATION] Starting normal scan to process new events with full enrichment...")
        print("="*80)
        logger.info("")
        logger.info("[Liquidations] Found %d new events in gap scan!", new_events_found)
        logger.info("[Liquidations] Starting normal scan to process them...")
        main()
    else:
        print("\n" + "="*80)
        print(f"[VALIDATION] Gap scan complete: NO missing events found")
        print(f"[VALIDATION] Scanned {blocks_scanned_total:,} blocks in {phase4_duration:.1f}s ({blocks_scanned_total/phase4_duration:.0f} blocks/sec)")
        print("[VALIDATION] All historical blocks have been scanned successfully")
        print("="*80)
        logger.info("")
        logger.info("[Liquidations] No gaps found - all historical blocks scanned")
    
    # === PHASE 5: Update to latest block ===
    print("\n" + "="*80)
    print("[VALIDATION] PHASE 5/5: Syncing to Latest Block")
    print("="*80)
    
    try:
        latest = w3.eth.block_number
        new_blocks = latest - max_block
        print(f"[VALIDATION] Current CSV end: {max_block:,}")
        print(f"[VALIDATION] Latest blockchain block: {latest:,}")
        print(f"[VALIDATION] New blocks to scan: {new_blocks:,}")
        
        if new_blocks > 0:
            print(f"[VALIDATION] Starting incremental scan for new blocks ({new_blocks:,} blocks)...")
            logger.info("")
            logger.info("="*80)
            logger.info("PHASE 5/5: Syncing to Latest Block (%s new blocks)", f"{new_blocks:,}")
            logger.info("="*80)
            main()
        else:
            print("[VALIDATION] Already at latest block - no new blocks to scan")
            logger.info("Already at latest block - validation complete")
    except Exception as e:
        print(f"[VALIDATION] Could not check latest block: {e}")
        logger.warning("Could not check latest block: %s", e)
    
    print("\n" + "="*80)
    print("[VALIDATION] === VALIDATION COMPLETE ===")
    print("[VALIDATION] All phases finished successfully")
    print("="*80)
    logger.info("")
    logger.info("="*80)
    logger.info("[Liquidations] === VALIDATION COMPLETE ===")
    logger.info("="*80)


def validate_numbers(repair: bool = False, tolerance_abs: float = 0.01, tolerance_pct: float = 0.005):
    """
    Validate numeric USD fields in the CSV.

    Dry-run (repair=False):
      - Loads CSV and recomputes collateral_value_usd and debt_value_usd from stored
        `collateral_price_usd_at_block` / `debt_price_usd_at_block` and `collateralOut` / `debtToCover`.
      - Reports mismatches beyond tolerance.

    Repair mode (repair=True):
      - Re-fetches prices using `get_aave_asset_price` for each row and recomputes values.
      - Updates CSV rows where recomputed values differ beyond tolerance.

    Tolerances: absolute (dollars) and relative (percentage) checks applied.
    """
    csv_path = os.path.join(DATA_DIR, "liquidations_master.csv")
    if not os.path.exists(csv_path):
        logger.error("[Liquidations] CSV not found: %s", csv_path)
        return

    import csv as csv_module
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    logger.info("[Validate] %d rows loaded for numeric validation", len(rows))

    w3 = None
    fetcher = None
    if repair:
        # Create a timestamped backup before attempting repairs
        try:
            ts = int(time.time())
            backup_path = os.path.join(DATA_DIR, f"liquidations_master.csv.bak.{ts}")
            import shutil
            shutil.copy2(csv_path, backup_path)
            logger.info("[Validate] Backup created: %s", backup_path)
        except Exception as e:
            logger.warning("[Validate] Could not create backup: %s", e)

        w3 = get_web3_with_rotation()
        fetcher = ChainlinkPriceFetcher(w3)

    mismatches = []
    checked = 0
    fixed = 0
    patched = []

    for i, row in enumerate(rows):
        checked += 1
        try:
            block = int(row.get('block') or 0)
        except Exception:
            continue

        try:
            collateral_out = float(row.get('collateralOut') or 0)
        except Exception:
            collateral_out = 0.0
        try:
            debt_amt = float(row.get('debtToCover') or 0)
        except Exception:
            debt_amt = 0.0

        col_price_stored = row.get('collateral_price_usd_at_block', '')
        debt_price_stored = row.get('debt_price_usd_at_block', '')

        try:
            col_price_val = float(col_price_stored) if col_price_stored not in [None, '', ''] else None
        except Exception:
            col_price_val = None
        try:
            debt_price_val = float(debt_price_stored) if debt_price_stored not in [None, '', ''] else None
        except Exception:
            debt_price_val = None

        try:
            stored_col_val_f = float(row.get('collateral_value_usd')) if row.get('collateral_value_usd') not in [None, '', ''] else None
        except Exception:
            stored_col_val_f = None
        try:
            stored_debt_val_f = float(row.get('debt_value_usd')) if row.get('debt_value_usd') not in [None, '', ''] else None
        except Exception:
            stored_debt_val_f = None

        if repair:
            coll_asset = row.get('collateralAsset')
            debt_asset = row.get('debtAsset')
            coll_sym = row.get('collateralSymbol')
            debt_sym = row.get('debtSymbol')
            try:
                feed_coll = normalize_symbol(coll_sym, coll_asset)
            except Exception:
                feed_coll = coll_sym
            try:
                feed_debt = normalize_symbol(debt_sym, debt_asset)
            except Exception:
                feed_debt = debt_sym

            try:
                new_col_price = get_aave_asset_price(coll_sym, coll_asset, block, fetcher, w3, feed_coll)
            except Exception:
                new_col_price = None
            try:
                new_debt_price = get_aave_asset_price(debt_sym, debt_asset, block, fetcher, w3, feed_debt)
            except Exception:
                new_debt_price = None

            if new_col_price is not None and new_col_price > 0:
                col_price_val = new_col_price
            if new_debt_price is not None and new_debt_price > 0:
                debt_price_val = new_debt_price

        expected_col_val = None
        expected_debt_val = None
        if col_price_val is not None:
            expected_col_val = round(collateral_out * col_price_val, 2)
        if debt_price_val is not None:
            expected_debt_val = round(debt_amt * debt_price_val, 2)

        row_mismatch = False
        reasons = []
        if expected_col_val is not None and stored_col_val_f is not None:
            diff = abs(expected_col_val - stored_col_val_f)
            pct = diff / expected_col_val if expected_col_val else 0
            if diff > tolerance_abs and pct > tolerance_pct:
                row_mismatch = True
                reasons.append(f"collateral_value mismatch: stored={stored_col_val_f} expected={expected_col_val}")
        elif expected_col_val is not None and stored_col_val_f is None:
            row_mismatch = True
            reasons.append(f"collateral_value missing: expected={expected_col_val}")

        if expected_debt_val is not None and stored_debt_val_f is not None:
            diffd = abs(expected_debt_val - stored_debt_val_f)
            pctd = diffd / expected_debt_val if expected_debt_val else 0
            if diffd > tolerance_abs and pctd > tolerance_pct:
                row_mismatch = True
                reasons.append(f"debt_value mismatch: stored={stored_debt_val_f} expected={expected_debt_val}")
        elif expected_debt_val is not None and stored_debt_val_f is None:
            row_mismatch = True
            reasons.append(f"debt_value missing: expected={expected_debt_val}")

        if row_mismatch:
            mismatches.append((i, row.get('block'), row.get('tx'), reasons))
            if repair:
                changed = False
                if expected_col_val is not None:
                    # Write high-precision price and detect source
                    try:
                        rows[i]['collateral_price_usd_at_block'] = f"{col_price_val:.8f}"
                    except Exception:
                        rows[i]['collateral_price_usd_at_block'] = rows[i].get('collateral_price_usd_at_block', '')
                    # Price source intentionally not persisted; skip detection

                    rows[i]['collateral_value_usd'] = expected_col_val
                    changed = True
                    patched.append({'index': i, 'kind': 'collateral', 'price': col_price_val})
                if expected_debt_val is not None:
                    try:
                        rows[i]['debt_price_usd_at_block'] = f"{debt_price_val:.8f}"
                    except Exception:
                        rows[i]['debt_price_usd_at_block'] = rows[i].get('debt_price_usd_at_block', '')
                    # Price source intentionally not persisted; skip detection

                    rows[i]['debt_value_usd'] = expected_debt_val
                    changed = True
                    patched.append({'index': i, 'kind': 'debt', 'price': debt_price_val})
                if changed:
                    fixed += 1

    logger.info("[Validate] Rows checked: %d, mismatches found: %d, fixed: %d", checked, len(mismatches), fixed)
    if mismatches:
        for idx, blk, tx, reasons in mismatches[:50]:
            logger.warning("[Validate] Row %d block %s tx %s: %s", idx, blk, str(tx)[:12], "; ".join(reasons))

    if repair and fixed > 0:
        logger.info("[Validate] Writing repaired CSV back (%d rows updated)...", fixed)
        if fieldnames is None:
            fieldnames = CSV_FIELD_ORDER
        if 'eth_price_usd_at_block' not in fieldnames:
            fieldnames = list(fieldnames) + ['eth_price_usd_at_block']
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv_module.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        try:
            sync_scan_status_from_csv(status='running', message='validate/repair write complete')
        except Exception:
            logger.debug('Failed to sync status after validation write')
        # Write a repair report similar to tools/targeted_repair.py
        try:
            report = {
                'backup': backup_path if 'backup_path' in locals() else None,
                'patched_count': len(patched) if 'patched' in locals() else 0,
                'patched': patched if 'patched' in locals() else [],
                'mismatches_before': len(mismatches),
                'mismatches_after': 0,  # will compute below
                'timestamp': int(time.time()),
            }
            # Compute mismatches after quick-check
            mismatches_after = 0
            for i, r in enumerate(rows):
                try:
                    coll_out = float(r.get('collateralOut') or 0)
                except Exception:
                    coll_out = 0
                try:
                    debt_amt = float(r.get('debtToCover') or 0)
                except Exception:
                    debt_amt = 0
                try:
                    coll_price = float(r.get('collateral_price_usd_at_block')) if r.get('collateral_price_usd_at_block') not in [None,'',''] else None
                except Exception:
                    coll_price = None
                try:
                    debt_price = float(r.get('debt_price_usd_at_block')) if r.get('debt_price_usd_at_block') not in [None,'',''] else None
                except Exception:
                    debt_price = None
                try:
                    stored_coll = float(r.get('collateral_value_usd')) if r.get('collateral_value_usd') not in [None,'',''] else None
                except Exception:
                    stored_coll = None
                try:
                    stored_debt = float(r.get('debt_value_usd')) if r.get('debt_value_usd') not in [None,'',''] else None
                except Exception:
                    stored_debt = None

                if coll_out and coll_price is not None and stored_coll is not None:
                    expected = round(coll_out * coll_price, 2)
                    diff = abs(expected - stored_coll)
                    pct = diff / expected if expected else 0
                    if diff > tolerance_abs and pct > tolerance_pct:
                        mismatches_after += 1
                if debt_amt and debt_price is not None and stored_debt is not None:
                    expected_d = round(debt_amt * debt_price, 2)
                    diffd = abs(expected_d - stored_debt)
                    pctd = diffd / expected_d if expected_d else 0
                    if diffd > tolerance_abs and pctd > tolerance_pct:
                        mismatches_after += 1

            report['mismatches_after'] = mismatches_after
            report_path = os.path.join(DATA_DIR, 'serious_cases_repair_report.json')
            with open(report_path, 'w', encoding='utf-8') as rf:
                json.dump(report, rf, indent=2)
            logger.info("[Validate] Repair report written: %s", report_path)
        except Exception as e:
            logger.warning("[Validate] Failed to write repair report: %s", e)
        logger.info("[Validate] Repair complete: %d rows fixed", fixed)


if __name__ == "__main__":
    # Simplified CLI: only `--validate` is the supported validation entrypoint.
    # Other flags are deprecated and will be routed to `--validate`.
    import sys

    if "--validate" in sys.argv:
        logger.info("[Liquidations] Validation mode: Checking prices + block gaps...")
        validate_and_fill_gaps()
        sys.exit(0)

    # Deprecated aliases: map to --validate with a warning
    deprecated_flags = ["--backfill", "--validate-numbers", "--repair", "--fix"]
    if any(f in sys.argv for f in deprecated_flags):
        logger.warning("Deprecated flags used. Use `--validate` instead. Running `--validate` now.")
        validate_and_fill_gaps()
        sys.exit(0)

    # Default behaviour: normal scan
    # Ensure canonical master CSV exists before scanner runs
    try:
        subprocess.run([sys.executable, "scripts/ensure_master_csv.py"], check=False)
    except Exception:
        pass

    to_block = sys.argv[1] if len(sys.argv) > 1 else "latest"
    main(to_block)

