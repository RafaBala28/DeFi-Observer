"""
Shared Chainlink price feed utilities for enrichment and CSV export.
"""
from typing import Optional

from web3 import Web3
import logging

from web3_utils import get_web3
import time
from decimal import Decimal, getcontext

# Use higher precision for ratio math (CAPO calculations)
getcontext().prec = 36

PERCENTAGE_FACTOR = Decimal(10_000)  # solidity constant
SCALING_FACTOR = Decimal(10**6)
SECONDS_PER_YEAR = Decimal(365 * 24 * 3600)

CHAINLINK_FEEDS = {
    "ETH": Web3.to_checksum_address("0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"),
    "BTC": Web3.to_checksum_address("0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"),
    "DAI": Web3.to_checksum_address("0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9"),
    "USDC": Web3.to_checksum_address("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"),
    "USDT": Web3.to_checksum_address("0x3E7d1eAB13ad0104d2750B8863b489D65364e32D"),
    "AAVE": Web3.to_checksum_address("0x547a514d5e3769680Ce22B2361c10Ea13619e8a9"),
    "LINK": Web3.to_checksum_address("0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c"),
    # MKR ist ETH-basiert (MKR/ETH) -> ETH_BASED_FEEDS
    "UNI": Web3.to_checksum_address("0x553303d460EE0afb37EdFf9bE42922D8FF63220e"),
    "CRV": Web3.to_checksum_address("0xCd627aA160A6fA45EB793D19Ef54f5062F20f33f"),
    # GNO Feed ist kaputt (execution reverted) - entfernt
    "COMP": Web3.to_checksum_address("0xdbd020CAeF83eFd542f4De03e3cF0C28A4428bd5"),
    "WSTETH": Web3.to_checksum_address("0x164b276057258d81941e97B0a900D4C7B358bCe0"),  # wstETH/USD
    # RETH und CBETH sind ETH-basiert -> ETH_BASED_FEEDS (X/ETH * ETH/USD)
    # Neue Feeds hinzugefügt für vollständige AAVE V3 Abdeckung
    "GHO": Web3.to_checksum_address("0x3f12643D3f6f874d39C2a4c9f2Cd6f2DbAC877FC"),   # GHO/USD
    "LUSD": Web3.to_checksum_address("0x3D7aE7E594f2f2091Ad8798313450130d0Aba3a0"),  # LUSD/USD
    "RPL": Web3.to_checksum_address("0x4E155eD98aFE9034b7A5962f6C84c86d869daA9d"),   # RPL/USD
    "ENS": Web3.to_checksum_address("0x5C00128d4d1c2F4f652C267d7bcdD7Ac99C16E16"),   # ENS/USD
    # CBETH ist ETH-basiert (cbETH/ETH) -> ETH_BASED_FEEDS
    "FRAX": Web3.to_checksum_address("0xB9E1E3A9fEff48998E45Fa90847ed4D467E8BcfD"),  # FRAX/USD
    "SNX": Web3.to_checksum_address("0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699"),   # SNX/USD
    "BAL": Web3.to_checksum_address("0xdF2917806E30300537aEB49A7663062F4d1F2b5F"),   # BAL/USD
    "FXS": Web3.to_checksum_address("0x6Ebc52C8C1089be9eB3945C4350B68B8E4C2233f"),   # FXS/USD (Frax Share)
    "1INCH": Web3.to_checksum_address("0xc929ad75B72593967DE83E7F7CdA0493458261D9"), # 1INCH/USD
    "CBBTC": Web3.to_checksum_address("0x2665701293fCbEB223D11A08D826563EDcCE423A"), # cbBTC/USD (seit 2024)
    # Stablecoin Feeds (alle haben jetzt Chainlink!)
    "PYUSD": Web3.to_checksum_address("0x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1"), # PYUSD/USD (PayPal)
    "CRVUSD": Web3.to_checksum_address("0xEEf0C605546958c1f899b6fB336C20671f9cD49F"), # crvUSD/USD (Curve)
        # tBTC: treat as BTC (tokenized Bitcoin variant)
        "TBTC": "BTC",
    "USDS": Web3.to_checksum_address("0xfF30586cD0F29eD462364C7e81375FC0C71219b1"), # USDS/USD (Sky/MakerDAO)
    "USDE": Web3.to_checksum_address("0xa569d910839Ae8865Da8F8e70FfFb0cBA869F961"), # USDe/USD (Ethena)
}

# EUR/USD Chainlink feed (used to price EUR-pegged tokens like EURC)
# Chainlink EUR/USD Aggregator (mainnet)
CHAINLINK_FEEDS["EUR"] = Web3.to_checksum_address("0xb49f677943BC038e9857d61E7d053CaA2C1734C1")

TOKEN_ALIASES = {
    "WETH": "ETH",
    "ETH": "ETH",
    "WBTC": "BTC",
    "TBTC": "BTC",
    "BTC": "BTC",
    "DAI": "DAI",
    "USDC": "USDC",
    "USDT": "USDT",
    "AAVE": "AAVE",
    "LINK": "LINK",
    "MKR": "MKR",  # ETH-basiert -> ETH_BASED_FEEDS
    "UNI": "UNI",
    "CRV": "CRV",
    "GNO": "GNO",  # AAVE Oracle Fallback
    "STG": "STG",  # AAVE Oracle Fallback (Stargate)
    "COMP": "COMP",
    "WSTETH": "WSTETH",
    "STETH": "WSTETH",
    "RETH": "RETH",
    "LDO": "LDO",
    # Neue Aliases
    "GHO": "GHO",
    "LUSD": "LUSD",
    "RPL": "RPL",
    "ENS": "ENS",
    "CBETH": "CBETH",
    "FRAX": "FRAX",
    "SNX": "SNX",
    "BAL": "BAL",
    "FXS": "FXS",
    "1INCH": "1INCH",
    "CBBTC": "CBBTC",
    # Stablecoin Aliases
    "PYUSD": "PYUSD",
    "CRVUSD": "CRVUSD",
    "USDS": "USDS",
    "USDE": "USDE",
    # EUR stable token mapping - treat EURC as EUR for pricing (EUR -> USD via Chainlink EUR/USD)
    "EURC": "EUR",
}

# Add USDB alias so symbol normalization recognizes it
TOKEN_ALIASES["USDB"] = "USDB"
# Additional aliases for tokens seen in CSV that should map to existing feeds
TOKEN_ALIASES["ETHX"] = "ETH"
TOKEN_ALIASES["WEETH"] = "ETH"
TOKEN_ALIASES["SDAI"] = "DAI"
TOKEN_ALIASES["SUSDE"] = "USDE"

# 🔄 FALLBACK: Nur für Tokens OHNE eigenen Feed
# ETH-Derivate werden NICHT approximiert (besser leer als falsch für Statistik)
# Nur Stablecoins bekommen $1 Fallback wenn Feed fehlt
PRICE_FALLBACKS = {
    # ETH-Derivate: KEIN Fallback - Preis bleibt leer wenn Feed nicht verfügbar
    # "WSTETH": "ETH",  # DEAKTIVIERT - wstETH ist ~15% mehr wert als ETH
    # "RETH": "ETH",    # DEAKTIVIERT - rETH ist ~10% mehr wert als ETH
    # "CBETH": "ETH",   # DEAKTIVIERT - cbETH hat eigene Exchange Rate
    # "CBBTC": "BTC",   # NICHT MEHR NÖTIG - cbBTC hat jetzt eigenen Chainlink Feed!
    "ETHX": "ETH",     # Fallback für ETHx wenn kein Feed verfügbar
}

# Stablecoins die immer ~$1 sind (NUR als LETZTER Fallback wenn Chainlink fehlt)
# HINWEIS: Die meisten Stablecoins haben jetzt Chainlink Feeds!
# Dieser Fallback ist nur für historische Blöcke VOR dem Feed-Launch
STABLE_TOKENS = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "GHO", "PYUSD", "USDS", "CRVUSD", "USDE", "USDB"}

# Add RLUSD as known USD stablecoin fallback (project-specific stable)
STABLE_TOKENS.add("RLUSD")

# ============================================================================
# AAVE V3 ORACLE - Fallback für Tokens ohne Chainlink Feed (z.B. STG, GNO)
# AAVE verwendet eigene Oracle-Preise die für alle gelisteten Assets verfügbar sind
# ============================================================================
AAVE_V3_ORACLE = Web3.to_checksum_address("0x54586bE62E3c3580375aE3723C145253060Ca0C2")
AAVE_ORACLE_BASE_UNIT = 10 ** 8  # AAVE Oracle gibt Preise in 8 Decimals zurück

# AAVE Oracle ABI
AAVE_ORACLE_ABI = [
    {"inputs": [{"name": "asset", "type": "address"}], "name": "getAssetPrice", 
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
]

# Tokens die den AAVE Oracle als Fallback nutzen (kein funktionierender Chainlink Feed)
AAVE_ORACLE_TOKENS = {
    "STG": Web3.to_checksum_address("0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6"),   # Stargate
    "GNO": Web3.to_checksum_address("0x6810e776880C02933D47DB1b9fc05908e5386b96"),   # Gnosis
}

# Extra tokens found in CSV that are covered by AAVE V3 Oracle and
# should be used as safe fallbacks when Chainlink feeds are missing.
# These were verified via getAssetPrice(...) on representative blocks.
ADDITIONAL_AAVE_ORACLE_TOKENS = {
    "USDtb": Web3.to_checksum_address("0xC139190F447e929f090Edeb554D95AbB8b18aC1C"),
    "rsETH": Web3.to_checksum_address("0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"),
    "LBTC": Web3.to_checksum_address("0x8236a87084f8B84306f72007F36F2618A5634494"),
    "osETH": Web3.to_checksum_address("0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38"),
    "XAUt": Web3.to_checksum_address("0x68749665FF8D2d112Fa859AA293F07A622782F38"),
    "FBTC": Web3.to_checksum_address("0xC96dE26018A54D51c097160568752c4E3BD6C364"),
    "eBTC": Web3.to_checksum_address("0x657e8C867D8B37dCC18fA4Caead9C45EB088C642"),
    "KNC": Web3.to_checksum_address("0xdeFA4e8a7bcBA345F687a2f1456F5Edd9CE97202"),
    "PT-eUSDE-14AUG2025": Web3.to_checksum_address("0x14Bdc3A3AE09f5518b923b69489CBcAfB238e617"),
    "PT-sUSDE-25SEP2025": Web3.to_checksum_address("0x9F56094C450763769BA0EA9Fe2876070c0fD5F77"),
}

# Merge the additional mappings into the main AAVE_ORACLE_TOKENS dict
AAVE_ORACLE_TOKENS.update(ADDITIONAL_AAVE_ORACLE_TOKENS)

# ============================================================================
# AAVE CAPO (Capped Price Oracle) Adapters - ALL Ethereum Mainnet Deployments
# Diese Contracts enthalten die offiziellen CAPO Parameter (snapshot, growth rate)
# Source: Queried from Aave V3 Oracle (0x54586bE62E3c3580375aE3723C145253060Ca0C2)
# ALL 7 CAPO adapters currently used by Aave V3 on Ethereum mainnet
# Updated: Dec 30, 2024 - Added OSETH, verified all addresses
# ============================================================================
CAPO_ADAPTERS = {
    "WSTETH": Web3.to_checksum_address("0xe1D97bF61901B075E9626c8A2340a7De385861Ef"),  # WstETHPriceCapAdapter (9.68% yearly)
    "RETH": Web3.to_checksum_address("0x6929706c42d637DF5Ebf7F0BcfF2aF47F84Ea69D"),    # RETHPriceCapAdapter (9.30% yearly)
    "CBETH": Web3.to_checksum_address("0x889399C34461b25d70d43931e6cE9E40280E617B"),   # CbETHPriceCapAdapter (8.12% yearly)
    "WEETH": Web3.to_checksum_address("0x87625393534d5C102cADB66D37201dF24cc26d4C"),   # WeETHPriceCapAdapter (8.75% yearly)
    "RSETH": Web3.to_checksum_address("0x7292C95A5f6A501a9c4B34f6393e221F2A0139c3"),   # RsETHPriceCapAdapter (9.83% yearly)
    "OSETH": Web3.to_checksum_address("0x2b86D519eF34f8Adfc9349CDeA17c09Aa9dB60E2"),   # OsETHPriceCapAdapter (8.75% yearly)
    "SUSDE": Web3.to_checksum_address("0x42bc86f2f08419280a99d8fbEa4672e7c30a86ec"),   # SUSDePriceCapAdapter (50.00% yearly)
}

# CAPO Adapter ABI (PriceCapAdapterBase interface)
# Source: https://github.com/bgd-labs/aave-capo/blob/main/src/contracts/PriceCapAdapterBase.sol
CAPO_ADAPTER_ABI = [
    {"name": "getSnapshotRatio", "inputs": [], "outputs": [{"name": "", "type": "uint256", "internalType": "uint256"}], "stateMutability": "view", "type": "function"},
    {"name": "getSnapshotTimestamp", "inputs": [], "outputs": [{"name": "", "type": "uint256", "internalType": "uint256"}], "stateMutability": "view", "type": "function"},
    {"name": "getMaxYearlyGrowthRatePercent", "inputs": [], "outputs": [{"name": "", "type": "uint256", "internalType": "uint256"}], "stateMutability": "view", "type": "function"},
    {"name": "RATIO_DECIMALS", "inputs": [], "outputs": [{"name": "", "type": "uint8", "internalType": "uint8"}], "stateMutability": "view", "type": "function"},
    {"name": "getRatio", "inputs": [], "outputs": [{"name": "", "type": "int256", "internalType": "int256"}], "stateMutability": "view", "type": "function"},
    {"name": "latestAnswer", "inputs": [], "outputs": [{"name": "", "type": "int256", "internalType": "int256"}], "stateMutability": "view", "type": "function"},
]

# LSD Exchange Rate Contracts für historische Preisberechnung
# Wenn direkter USD-Feed nicht existiert, berechnen wir: underlying_price × exchange_rate
# LSD Exchange Rate Contracts - BACKUP wenn Chainlink Feed nicht verfügbar
# Diese werden nur für wstETH Fallback verwendet (stETH × ratio)
# osETH hinzugefügt (Dec 30, 2024)
LSD_CONTRACTS = {
    "WSTETH": {
        "contract": Web3.to_checksum_address("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"),
        "method": "stEthPerToken",  # Returns stETH per wstETH
        "underlying": "STETH",       # Underlying token for price
        "decimals": 18,
    },
    "RETH": {
        "contract": Web3.to_checksum_address("0xae78736Cd615f374D3085123A210448E74Fc6393"),
        "method": "getExchangeRate",  # Returns ETH per rETH
        "underlying": "ETH",
        "decimals": 18,
    },
    "CBETH": {
        "contract": Web3.to_checksum_address("0xBe9895146f7AF43049ca1c1AE358B0541Ea49704"),
        "method": "exchangeRate",  # Returns ETH per cbETH
        "underlying": "ETH",
        "decimals": 18,
    },
    "RSETH": {
        "contract": Web3.to_checksum_address("0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"),
        "method": "rsETHPrice",  # Returns ETH per rsETH (Oracle contract)
        "underlying": "ETH",
        "decimals": 18,
    },
    "WEETH": {
        "contract": Web3.to_checksum_address("0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee"),
        "method": "getRate",  # Returns eETH per weETH
        "underlying": "ETH",
        "decimals": 18,
    },
    "OSETH": {
        "contract": Web3.to_checksum_address("0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38"),
        "method": "convertToAssets",  # Returns ETH per osETH (ERC4626 style, needs 1e18 input)
        "underlying": "ETH",
        "decimals": 18,
        "input_amount": 10 ** 18,
    },
    # ERC4626-style staked USDe (sUSDe)
    "SUSDE": {
        "contract": Web3.to_checksum_address("0x9d39a5de30e57443bff2a8307a4256c8797a3497"),
        "method": "convertToAssets",
        "underlying": "USDE",
        "decimals": 18,
        "input_amount": 10 ** 18,
    },
}


# ---------------------------------------------------------------------------
# CAPO helper functions (migrated from tools/capo.py)
# ---------------------------------------------------------------------------
def cap_price_from_ratio(base_price: Decimal, current_ratio: Decimal, snapshot_ratio: Decimal,
                         snapshot_ts: int, max_yearly_ratio_bps: int, ratio_decimals: int,
                         event_ts: int = None) -> Decimal:
    if event_ts is None:
        event_ts = int(time.time())

    if snapshot_ratio is None or snapshot_ratio == 0:
        return Decimal(0)

    base_price = Decimal(base_price)
    current_ratio = Decimal(current_ratio)
    snapshot_ratio = Decimal(snapshot_ratio)
    max_yearly = Decimal(int(max_yearly_ratio_bps))

    maxRatioGrowthPerSecondScaled = (snapshot_ratio * max_yearly * SCALING_FACTOR) / (PERCENTAGE_FACTOR * SECONDS_PER_YEAR)

    elapsed = Decimal(max(0, int(event_ts) - int(snapshot_ts)))

    max_ratio = snapshot_ratio + (maxRatioGrowthPerSecondScaled * elapsed) / SCALING_FACTOR

    effective_ratio = current_ratio if current_ratio <= max_ratio else max_ratio

    price = (base_price * effective_ratio) / (Decimal(10) ** Decimal(ratio_decimals))

    return price.quantize(Decimal('0.00000001'))


def cap_price_for_stable(base_price: Decimal, price_cap: int, decimals: int) -> Decimal:
    cap = Decimal(price_cap) / (Decimal(10) ** Decimal(decimals))
    base_price = Decimal(base_price)
    return base_price if base_price <= cap else cap


# Feeds die ETH-basiert sind (X/ETH statt X/USD) - müssen mit ETH/USD multipliziert werden
# Formel: Token_USD = (Token/ETH) × (ETH/USD)
# Note: RETH and CBETH are handled via LSD_CONTRACTS (exchange rate logic), not here
ETH_BASED_FEEDS = {
    "LDO": Web3.to_checksum_address("0x4e844125952D32AcdF339BE976c98E22F6F318dB"),   # LDO/ETH
    "MKR": Web3.to_checksum_address("0x24551a8Fb2A7211A25a17B1481f043A8a8adC7f2"),   # MKR/ETH
}

# stETH/USD Feed für wstETH Fallback
STETH_USD_FEED = Web3.to_checksum_address("0xCfE54B5cD566aB89272946F602D76Ea879CAb4a8")

ADDRESS_TO_SYMBOL = {
    Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"): "WETH",
    Web3.to_checksum_address("0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): "USDC",
    Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7"): "USDT",
    Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F"): "DAI",
    Web3.to_checksum_address("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"): "WBTC",
    Web3.to_checksum_address("0x7Fc66500C84A76Ad7e9c93437bFc5Ac33E2DDaE9"): "AAVE",
    Web3.to_checksum_address("0x514910771AF9Ca656af840dff83E8264EcF986CA"): "LINK",
    Web3.to_checksum_address("0x9f8F72aA9304c8B593d555F12ef6589cC3A579A2"): "MKR",
    Web3.to_checksum_address("0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"): "UNI",
    Web3.to_checksum_address("0xD533a949740bb3306d119CC777fa900bA034cd52"): "CRV",
    Web3.to_checksum_address("0x6810e776880C02933D47DB1b9fc05908e5386b96"): "GNO",
    Web3.to_checksum_address("0xc00e94Cb662C3520282E6f5717214004A7f26888"): "COMP",
    Web3.to_checksum_address("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"): "WSTETH",
    Web3.to_checksum_address("0xae78736Cd615f374D3085123A210448E74Fc6393"): "RETH",
    Web3.to_checksum_address("0x5A98FcBEA516Cf06857215779Fd812CA3bef1B32"): "LDO",
    # Neue Token-Adressen
    Web3.to_checksum_address("0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"): "GHO",
        Web3.to_checksum_address("0x18084fba666a33d37592fa2633fd49a74dd93a88"): "tBTC",
    Web3.to_checksum_address("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"): "LUSD",
    Web3.to_checksum_address("0xD33526068D116cE69F19A9ee46F0bd304F21A51f"): "RPL",
    Web3.to_checksum_address("0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72"): "ENS",
    Web3.to_checksum_address("0xBe9895146f7AF43049ca1c1AE358B0541Ea49704"): "CBETH",
    Web3.to_checksum_address("0x853d955aCEf822Db058eb8505911ED77F175b99e"): "FRAX",
    Web3.to_checksum_address("0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F"): "SNX",
    Web3.to_checksum_address("0xba100000625a3754423978a60c9317c58a424e3D"): "BAL",
    # AAVE Oracle Tokens (kein Chainlink Feed)
    Web3.to_checksum_address("0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6"): "STG",  # Stargate
}

# Known external token addresses (non-exhaustive).
# Note: USDB currently exists on Blast (Blast-chain address shown). We add the
# address mapping so that if the scanner ever sees this exact address it can
# resolve the symbol to `USDB`. The project primarily targets Ethereum
# mainnet; Chainlink/Aave oracles for USDB on mainnet were not found.
try:
    ADDRESS_TO_SYMBOL[Web3.to_checksum_address("0x4300000000000000000000000000000000000003")] = "USDB"
except Exception:
    # ignore checksum errors in environments without Web3 properly configured
    pass

AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint80", "name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint16", "name": "_phaseId", "type": "uint16"}],
        "name": "phaseAggregators",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "phaseId",
        "outputs": [{"internalType": "uint16", "name": "", "type": "uint16"}],
        "stateMutability": "view",
        "type": "function"
    },
]


def normalize_symbol(symbol: Optional[str], asset: Optional[str]) -> Optional[str]:
    """Try to resolve the asset into one of the supported Chainlink feed symbols."""
    if symbol:
        sym = TOKEN_ALIASES.get(symbol.upper())
        if sym:
            return sym
    if asset:
        try:
            checksum = Web3.to_checksum_address(asset)
        except Exception:
            checksum = None
        if checksum and checksum in ADDRESS_TO_SYMBOL:
            return ADDRESS_TO_SYMBOL[checksum]
    return None


def get_fallback_symbol(symbol: Optional[str]) -> Optional[str]:
    """Get fallback symbol for tokens without their own Chainlink feed.
    
    Used when the primary feed doesn't have historical data for a block.
    ETH-derivatives → ETH, BTC-derivatives → BTC
    """
    if not symbol:
        return None
    return PRICE_FALLBACKS.get(symbol.upper())


def is_stablecoin(symbol: Optional[str]) -> bool:
    """Check if token is a USD stablecoin (can use $1 as fallback)."""
    if not symbol:
        return False
    return symbol.upper() in STABLE_TOKENS


class ChainlinkPriceFetcher:
    """Fetch historical Chainlink USD prices using the AggregatorV3 interface."""

    def __init__(self, w3: Web3, use_capo_contracts: bool = True):
        self.w3 = w3
        # Enforce Mainnet-only operation
        try:
            chain_id = getattr(self.w3.eth, 'chain_id', None)
        except Exception:
            chain_id = None
        assert chain_id == 1, "Not Ethereum Mainnet!"

        self.contracts = {}
        self.decimals = {}
        self.latest_cache = {}
        self.round_cache = {}
        self.call_retries = 3
        self.call_timeout = 10
        self.logger = logging.getLogger(__name__)
        # Toggle between live contract queries (Option 3) or static JSON (Option 1)
        self.use_capo_contracts = use_capo_contracts
        # Safety budget to avoid excessive getRoundData RPC calls during historical search
        # Raised to 300 to allow deeper historical bisection when needed.
        self.default_round_call_budget = 300
        self._round_calls_remaining = {}
        # Track feeds that were invalidated during validation
        self.BROKEN_FEEDS = {}
        # Skip feed validation - feeds are already verified to work
        # try:
        #     self.validate_feeds()
        # except Exception as e:
        #     # validation may fail transiently if RPC is flaky; log and continue
        #     self.logger.warning("Chainlink feed validation failed during init: %s", e)

    def _get_feed_addr(self, feed_symbol: str):
        """Resolve a feed symbol to a working aggregator proxy address.

        Steps:
        - First check TOKEN_ALIASES to resolve symbol (e.g., WETH -> ETH)
        - Use the configured `CHAINLINK_FEEDS` entry if present.
        - If the address has no contract code on-chain, attempt to resolve via ENS
          using the standard Chainlink naming convention `<symbol>-usd.data.eth`.
        - If ENS resolution succeeds, update `CHAINLINK_FEEDS` in-place and
          return the checksum address.
        Returns `None` when no usable address can be found.
        """
        # WICHTIG: Erst Alias auflösen (WETH -> ETH, WBTC -> BTC, etc.)
        resolved_symbol = TOKEN_ALIASES.get(feed_symbol.upper(), feed_symbol.upper())
        
        if resolved_symbol not in CHAINLINK_FEEDS:
            return None

        addr = CHAINLINK_FEEDS[resolved_symbol]
        try:
            code = self.w3.eth.get_code(addr)
            if code and len(code) > 0:
                return addr
            else:
                # record as broken and return None
                self.logger.warning("Configured feed %s -> %s has no on-chain code; marking broken", feed_symbol, addr)
                self.BROKEN_FEEDS[feed_symbol] = addr
                return None
        except Exception as e:
            self.logger.warning("get_code failed for %s (%s): %s", feed_symbol, addr, e)
            self.BROKEN_FEEDS[feed_symbol] = addr
            return None

    def _check_feed_contract(self, addr: str) -> bool:
        """Return True if address has code and supports phaseId/phaseAggregators.

        This verifies the proxy implements the OCR2 phase helpers required for
        correct historical traversal.
        """
        try:
            code = self.w3.eth.get_code(addr)
            if not code or len(code) == 0:
                return False
            contract = self.w3.eth.contract(address=addr, abi=AGGREGATOR_ABI)
            # probe required functions
            try:
                _ = contract.functions.phaseId().call()
                _ = contract.functions.phaseAggregators(0).call()
            except Exception:
                return False
            return True
        except Exception:
            return False

    def validate_feeds(self):
        """Validate `CHAINLINK_FEEDS` entries and remove invalid ones.

        After this call, `CHAINLINK_FEEDS` will only contain addresses that are
        deployed on-chain and implement the required phase ABI. Invalid entries
        are moved into `self.BROKEN_FEEDS` for operator review.
        """
        to_remove = []
        for sym, addr in list(CHAINLINK_FEEDS.items()):
            ok = False
            try:
                ok = self._check_feed_contract(addr)
            except Exception:
                ok = False
            if not ok:
                self.logger.warning("Removing invalid Chainlink feed %s -> %s", sym, addr)
                self.BROKEN_FEEDS[sym] = addr
                to_remove.append(sym)
        for sym in to_remove:
            del CHAINLINK_FEEDS[sym]

    def _get_contract(self, feed_address: str):
        # Always (re)create contract objects using current `self.w3` to avoid stale providers
        try:
            contract = self.w3.eth.contract(address=feed_address, abi=AGGREGATOR_ABI)
            self.contracts[feed_address] = contract
            return contract
        except Exception as e:
            self.logger.warning("Contract init failed for %s: %s", feed_address, e)
            return None

    def _get_capo_params_from_chain(self, symbol: str, block_number: int) -> Optional[dict]:
        """
        Lese CAPO Parameter LIVE aus deployed AAVE Contract (blockgenau!).
        
        Option 3: Dynamisches Abrufen statt statischem JSON File.
        Gibt die gleichen Parameter zurück wie capo_params.json, aber 
        historisch korrekt für den angegebenen Block.
        
        Returns: dict mit snapshotRatio, snapshotTimestamp, maxYearlyRatioGrowthPercent, ratioDecimals
                 oder None wenn Token nicht konfiguriert oder Abruf fehlschlägt
        """
        symbol_upper = symbol.upper() if symbol else None
        if not symbol_upper or symbol_upper not in CAPO_ADAPTERS:
            return None
        
        adapter_address = CAPO_ADAPTERS[symbol_upper]
        
        try:
            contract = self.w3.eth.contract(address=adapter_address, abi=CAPO_ADAPTER_ABI)
            
            # BLOCKGENAU: Parameter zum Zeitpunkt des Events abrufen
            snapshot_ratio = contract.functions.getSnapshotRatio().call(block_identifier=block_number)
            snapshot_ts = contract.functions.getSnapshotTimestamp().call(block_identifier=block_number)
            max_growth = contract.functions.getMaxYearlyGrowthRatePercent().call(block_identifier=block_number)
            ratio_decimals = contract.functions.RATIO_DECIMALS().call(block_identifier=block_number)
            
            self.logger.debug(f"[CAPO CONTRACT] {symbol_upper} @ block {block_number}: ratio={snapshot_ratio}, ts={snapshot_ts}, growth={max_growth}bps")
            
            return {
                "type": "ratio",
                "snapshotRatio": str(snapshot_ratio),  # Als String wie im JSON
                "snapshotTimestamp": int(snapshot_ts),
                "maxYearlyRatioGrowthPercent": int(max_growth),
                "ratioDecimals": int(ratio_decimals),
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to get CAPO params from contract for {symbol_upper} @ block {block_number}: {e}")
            return None

    def _rotate_provider(self):
        """Attempt to obtain a fresh Web3 provider and clear contract cache."""
        self.logger.info("Rotating provider, requesting new Web3 (timeout=%ss)", self.call_timeout)
        new_w3 = get_web3(timeout=self.call_timeout, force_new=True, sticky=True)
        if new_w3 and new_w3.is_connected():
            self.w3 = new_w3
            self.contracts = {}
            self.logger.info("Provider rotated successfully")
            return True
        self.logger.warning("Provider rotation failed; no healthy providers available")
        return False

    def _safe_call(self, call_fn, feed_address: str):
        """Call a chain function with retries and provider rotation on failure."""
        last_exc = None
        for attempt in range(1, self.call_retries + 1):
            try:
                return call_fn()
            except Exception as e:
                last_exc = e
                self.logger.debug("Call attempt %s/%s for %s failed: %s", attempt, self.call_retries, feed_address, e)
                # Try to rotate provider and retry
                rotated = self._rotate_provider()
                if not rotated:
                    break
        self.logger.warning("All call attempts failed for %s: %s", feed_address, last_exc)
        raise last_exc

    def _get_decimals(self, feed_address: str) -> int:
        if feed_address not in self.decimals:
            contract = self._get_contract(feed_address)
            if not contract:
                return 18
            try:
                result = self._safe_call(lambda: contract.functions.decimals().call(), feed_address)
                self.decimals[feed_address] = int(result)
            except Exception as e:
                self.logger.warning("decimals() call failed for %s: %s", feed_address, e)
                self.decimals[feed_address] = 18
        return self.decimals.get(feed_address, 18)

    def _call_latest(self, feed_address: str):
        if feed_address not in self.latest_cache:
            contract = self._get_contract(feed_address)
            if not contract:
                self.logger.warning("No contract for %s", feed_address)
                return {"answer": None, "updatedAt": 0, "roundId": None}
            try:
                result = self._safe_call(lambda: contract.functions.latestRoundData().call(), feed_address)
                self.latest_cache[feed_address] = self._format_round(result)
            except Exception as e:
                self.logger.warning("latestRoundData() failed for %s: %s", feed_address, e)
                self.latest_cache[feed_address] = {"answer": None, "updatedAt": 0, "roundId": None}
        return self.latest_cache[feed_address]

    def _get_round(self, feed_address: str, round_id: int):
        cache = self.round_cache.setdefault(feed_address, {})
        if round_id in cache:
            return cache[round_id]
        # enforce a budget for getRoundData calls per feed to avoid runaway RPC loops
        remaining = self._round_calls_remaining.get(feed_address, self.default_round_call_budget)
        if remaining <= 0:
            self.logger.warning("Round call budget exhausted for %s", feed_address)
            return {"roundId": round_id, "answer": None, "startedAt": 0, "updatedAt": 0, "answeredInRound": None}
        self._round_calls_remaining[feed_address] = remaining - 1
        contract = self._get_contract(feed_address)
        if not contract:
            self.logger.warning("No contract for %s when fetching round %s", feed_address, round_id)
            return {"roundId": round_id, "answer": None, "startedAt": 0, "updatedAt": 0, "answeredInRound": None}
        try:
            data = self._safe_call(lambda: contract.functions.getRoundData(round_id).call(), feed_address)
            formatted = self._format_round(data)
            cache[round_id] = formatted
            return formatted
        except Exception as e:
            self.logger.warning("getRoundData(%s) failed for %s: %s", round_id, feed_address, e)
            return {"roundId": round_id, "answer": None, "startedAt": 0, "updatedAt": 0, "answeredInRound": None}

    @staticmethod
    def _format_round(data):
        round_id, answer, started_at, updated_at, answered_in_round = data
        return {
            "roundId": int(round_id),
            "answer": int(answer),
            "startedAt": int(started_at),
            "updatedAt": int(updated_at),
            "answeredInRound": int(answered_in_round),
        }

    def get_price_at_timestamp(self, feed_symbol: str, target_timestamp: int) -> Optional[float]:
        if not feed_symbol or not target_timestamp:
            return None
        feed_addr = self._get_feed_addr(feed_symbol)
        if not feed_addr:
            return None
        # reset per-call round search budget
        self._round_calls_remaining[feed_addr] = self.default_round_call_budget
        decimals = self._get_decimals(feed_addr)
        round_data = self._find_round_before(feed_addr, target_timestamp)
        if not round_data or round_data["answer"] <= 0:
            return None
        return round_data["answer"] / (10 ** decimals)

    def get_price_for_block(self, feed_symbol: str, block_number: int) -> Optional[float]:
        """
        Get Chainlink price at a specific historical block.
        
        SIMPLE & CORRECT: Uses block_identifier to query historical state directly.
        This is the most reliable method as it queries the exact contract state
        at that block, rather than trying to traverse rounds manually.

        For LSDs (wstETH, rETH) where direct USD feed may not exist historically,
        calculates: underlying_price × exchange_rate
        
        For ETH-based feeds (LDO/ETH), calculates: ratio × ETH/USD

        Returns the exact Chainlink price valid at that block.
        """
        symbol_upper = feed_symbol.upper() if feed_symbol else None
        if not symbol_upper:
            return None
        
        # Canonical fallback order:
        # 1) Direct Chainlink USD feed (most reliable, first choice)
        # 2) AAVE V3 Oracle (authoritative - what AAVE uses for liquidations!)
        # 3) CAPO-protected LSD (LSD calculation WITH safety cap - PREFERRED)
        # 4) LSD without CAPO (LSD calculation WITHOUT safety cap - FALLBACK)
        # 5) ETH/BTC Composition feeds (X/ETH × ETH/USD or X/BTC × BTC/USD)
        # 6) No price (None)

        # 1) Try direct Chainlink USD feed first
        feed_addr = self._get_feed_addr(feed_symbol)
        if feed_addr:
            decimals = self._get_decimals(feed_addr)
            if decimals is None:
                decimals = 8

            try:
                contract = self.w3.eth.contract(address=feed_addr, abi=AGGREGATOR_ABI)
                round_data = contract.functions.latestRoundData().call(block_identifier=block_number)
                answer = int(round_data[1])
                if answer > 0:
                    price = answer / (10 ** decimals)
                    self.logger.debug(f"[Chainlink] {feed_symbol} @ block {block_number}: ${price}")
                    return price
            except Exception as e:
                self.logger.debug(f"Direct feed failed for {feed_symbol} @ block {block_number}: {e}")

        # 2) AAVE Oracle (second priority - authoritative for AAVE assets)
        if symbol_upper in AAVE_ORACLE_TOKENS:
            price = self._get_aave_oracle_price_for_block(symbol_upper, block_number)
            if price:
                self.logger.debug(f"[AAVE Oracle] {symbol_upper} @ block {block_number}: ${price}")
                return price

        # 3) CAPO-protected LSD (PREFERRED - safe version with cap)
        if symbol_upper in LSD_CONTRACTS:
            price, used_capo = self._get_lsd_price_for_block(symbol_upper, block_number)
            if price and used_capo:
                self.logger.debug(f"[CAPO] {symbol_upper} @ block {block_number}: ${price}")
                return price
        
        # 4) LSD without CAPO (FALLBACK - when no CAPO configured or as safety)
        if symbol_upper in LSD_CONTRACTS:
            price, used_capo = self._get_lsd_price_for_block(symbol_upper, block_number)
            if price and not used_capo:
                self.logger.debug(f"[LSD] {symbol_upper} @ block {block_number}: ${price}")
                return price

        # 5) ETH/BTC Composition feeds (X/ETH × ETH/USD) - for non-LSD tokens like MKR, LDO
        if symbol_upper in ETH_BASED_FEEDS:
            price = self._get_eth_based_price_for_block(symbol_upper, block_number)
            if price:
                self.logger.debug(f"[Composition] {symbol_upper} @ block {block_number}: ${price}")
                return price

        return None

    def _get_aave_oracle_price_for_block(self, symbol: str, block_number: int) -> Optional[float]:
        """
        Get price from AAVE V3 Oracle for tokens without Chainlink feeds.
        
        AAVE maintains its own oracle prices for all listed assets.
        This is the authoritative price source that AAVE uses for liquidations.
        
        Returns: USD price at the specified block, or None on error
        """
        if symbol not in AAVE_ORACLE_TOKENS:
            return None
        
        asset_address = AAVE_ORACLE_TOKENS[symbol]
        
        try:
            oracle = self.w3.eth.contract(address=AAVE_V3_ORACLE, abi=AAVE_ORACLE_ABI)
            price_raw = oracle.functions.getAssetPrice(asset_address).call(block_identifier=block_number)
            
            if price_raw and price_raw > 0:
                price_usd = price_raw / AAVE_ORACLE_BASE_UNIT
                self.logger.debug(f"[AAVE Oracle] {symbol} @ block {block_number}: ${price_usd:.2f}")
                return price_usd
        except Exception as e:
            self.logger.debug(f"[AAVE Oracle] Failed for {symbol} @ block {block_number}: {e}")
        
        return None

    def _get_eth_based_price_for_block(self, symbol: str, block_number: int) -> Optional[float]:
        """
        Calculate USD price for ETH-based feeds (X/ETH).
        
        Formula: X_USD = (X/ETH ratio) × (ETH/USD price)
        
        Example: LDO/USD = LDO/ETH × ETH/USD
        """
        if symbol not in ETH_BASED_FEEDS:
            return None
        
        feed_addr = ETH_BASED_FEEDS[symbol]
        
        try:
            contract = self.w3.eth.contract(address=feed_addr, abi=AGGREGATOR_ABI)
            decimals = contract.functions.decimals().call()
            round_data = contract.functions.latestRoundData().call(block_identifier=block_number)
            
            eth_ratio = int(round_data[1]) / (10 ** decimals)
            
            # Get ETH/USD price at same block
            eth_price = self.get_price_for_block("ETH", block_number)
            
            if eth_price and eth_ratio > 0:
                return eth_ratio * eth_price
        except Exception as e:
            self.logger.debug(f"Failed to get ETH-based price for {symbol} @ block {block_number}: {e}")
        
        return None

    def _get_lsd_price_for_block(self, symbol: str, block_number: int) -> tuple[Optional[float], bool]:
        """
        Calculate LSD price via exchange rate for historical blocks.
        
        Returns: (price, used_capo) where used_capo indicates if CAPO was applied
        
        wstETH: stETH_price × stEthPerToken()
        rETH: ETH_price × getExchangeRate()
        """
        if symbol not in LSD_CONTRACTS:
            return None, False
        
        config = LSD_CONTRACTS[symbol]
        
        # Get exchange rate at block
        try:
            lsd_abi = [{
                "inputs": [],
                "name": config["method"],
                "outputs": [{"type": "uint256"}],
                "stateMutability": "view",
                "type": "function"
            }]
            lsd_contract = self.w3.eth.contract(address=config["contract"], abi=lsd_abi)
            # Support ERC4626-style convertToAssets(input_amount)
            if "input_amount" in config:
                exchange_rate_raw = lsd_contract.functions.convertToAssets(config["input_amount"]).call(block_identifier=block_number)
            else:
                exchange_rate_raw = lsd_contract.functions[config["method"]]().call(block_identifier=block_number)
            exchange_rate = exchange_rate_raw / (10 ** config["decimals"])
        except Exception as e:
            self.logger.debug(f"Failed to get exchange rate for {symbol} @ block {block_number}: {e}")
            return None, False

        # Get underlying price
        underlying = config["underlying"]

        # Special case: wstETH uses stETH/USD feed
        if symbol == "WSTETH" and underlying == "STETH":
            try:
                steth_contract = self.w3.eth.contract(address=STETH_USD_FEED, abi=AGGREGATOR_ABI)
                decimals = steth_contract.functions.decimals().call()
                round_data = steth_contract.functions.latestRoundData().call(block_identifier=block_number)
                underlying_price = int(round_data[1]) / (10 ** decimals)
            except Exception as e:
                self.logger.debug(f"Failed to get stETH price @ block {block_number}: {e}")
                return None, False
        else:
            # For rETH, get ETH price
            underlying_price = self.get_price_for_block(underlying, block_number)

        if not underlying_price:
            return None, False

        # compute raw LSD USD price
        raw_price = underlying_price * exchange_rate

        # Try to apply CAPO if token is configured
        try:
            # Option 3: Dynamisches Abrufen aus AAVE Contracts (blockgenau!)
            # Option 1: Statische Parameter aus JSON (Fallback)
            if self.use_capo_contracts:
                capo_params_dict = self._get_capo_params_from_chain(symbol, block_number)
            else:
                capo_params_dict = None
            
            # Fallback auf JSON wenn Contract-Abruf fehlschlägt oder deaktiviert ist
            if not capo_params_dict:
                import json, os
                repo_root = os.path.dirname(os.path.abspath(__file__))
                capo_fn = os.path.join(repo_root, 'data', 'capo_params.json')
                if os.path.exists(capo_fn):
                    with open(capo_fn, 'r', encoding='utf-8') as cf:
                        capo_params = json.load(cf)
                    key = symbol if symbol in capo_params else symbol.lower() if symbol.lower() in capo_params else symbol.upper() if symbol.upper() in capo_params else None
                    if key and key in capo_params:
                        capo_params_dict = capo_params[key]
            
            if not capo_params_dict:
                return raw_price, False
            
            p = capo_params_dict
            t = p.get('type')
            
            if t == 'ratio':
                snapshot = int(p['snapshotRatio'])
                snap_ts = int(p['snapshotTimestamp'])
                max_bps = int(p['maxYearlyRatioGrowthPercent'])
                ratio_dec = int(p.get('ratioDecimals', 18))

                try:
                    blk = self.w3.eth.get_block(block_number)
                    event_ts = int(getattr(blk, 'timestamp', blk['timestamp']))
                except Exception:
                    event_ts = None

                base_price = Decimal(underlying_price)
                current_ratio = (Decimal(raw_price) * (Decimal(10) ** Decimal(ratio_dec))) / (base_price if base_price != 0 else Decimal(1))

                capo_price = cap_price_from_ratio(base_price, current_ratio, Decimal(snapshot), snap_ts, max_bps, ratio_dec, event_ts)
                from decimal import Decimal as _D
                epsilon = _D('0.01')
                raw_d = _D(str(raw_price))
                capo_d = _D(str(capo_price))
                if raw_d - capo_d > epsilon:
                    source = "CONTRACT" if self.use_capo_contracts else "JSON"
                    self.logger.info(f"[CAPO {source}] Applied for {symbol} @ block {block_number}: raw={raw_price:.4f} capo={capo_price:.4f}")
                    return float(capo_price), True
                else:
                    return float(raw_price), True

            # stable caps: explicit price cap per token
            if t in ('stable', 'stable_cap'):
                price_cap = int(p.get('priceCap') or p.get('price_cap') or p.get('priceCap', 0))
                decimals = int(p.get('decimals', 8))
                try:
                    capo_price = cap_price_for_stable(raw_price, price_cap, decimals)
                    from decimal import Decimal as _D
                    epsilon = _D('0.01')
                    raw_d = _D(str(raw_price))
                    capo_d = _D(str(capo_price))
                    if raw_d - capo_d > epsilon:
                        self.logger.info(f"CAPO applied (stable cap) for {symbol} @ block {block_number}: raw={raw_price} capo={capo_price}")
                    return float(capo_price), True
                except Exception:
                    return float(raw_price), True

        except Exception as e:
            try:
                self.logger.debug(f"CAPO check failed for {symbol} @ block {block_number}: {e}")
            except Exception:
                pass

        return float(raw_price), False

    def get_price_for_block_COMPLEX_BACKUP(self, feed_symbol: str, block_number: int) -> Optional[float]:
        """
        BACKUP: Complex phase-aware resolver (kept for reference).
        FULLY ROBUST Chainlink historical price resolver.
        - Correctly handles OCR v2 feeds
        - Correctly handles aggregator phases
        - Traverses roundInPhase safely
        - Falls back across phase boundaries
        - Always finds the last round with updatedAt <= block.timestamp

        Returns the exact Chainlink price valid at that block.
        """

        # Implement a phase-aware bisection-based resolver that searches only
        # real rounds inside each phase aggregator. This greatly reduces RPC
        # calls and guarantees correctness for any block that Chainlink has data for.
        feed_addr = self._get_feed_addr(feed_symbol)
        if not feed_addr:
            return None

        # 1. Block timestamp
        try:
            block = self.w3.eth.get_block(block_number)
            target_ts = int(block.timestamp)
        except Exception as e:
            self.logger.warning(f"Failed to load block {block_number}: {e}")
            return None

        decimals = self._get_decimals(feed_addr)

        # 2. Get proxy contract and current phase
        contract = self._get_contract(feed_addr)
        if not contract:
            return None
        try:
            current_phase = contract.functions.phaseId().call()
        except Exception:
            # If proxy doesn't support phases, fall back to the legacy finder
            latest = self._call_latest(feed_addr)
            if not latest or latest["updatedAt"] == 0:
                return None
            if latest["updatedAt"] <= target_ts:
                return latest["answer"] / (10 ** decimals)
            # Fall back to slower _find_round_before on the proxy address
            rd = self._find_round_before(feed_addr, target_ts)
            if not rd:
                return None
            return rd["answer"] / (10 ** decimals)

        # 3. Iterate phases from newest to oldest (include phase 0)
        for phase in range(int(current_phase), -1, -1):
            # Fetch aggregator for this phase
            try:
                agg_addr = contract.functions.phaseAggregators(phase).call()
            except Exception:
                continue

            try:
                if int(agg_addr, 16) == 0:
                    continue
            except Exception:
                continue

            agg_contract = self.w3.eth.contract(address=agg_addr, abi=AGGREGATOR_ABI)
            # Quick-path: if this phase aggregator's latestRoundData is already
            # at or before the target timestamp, return it immediately to avoid
            # running the full bisection (huge speedup for many common cases).
            try:
                latest = self._safe_call(lambda: agg_contract.functions.latestRoundData().call(), feed_addr)
                latest_updated = int(latest[3])
                latest_answer = int(latest[1])
                if latest_updated != 0 and latest_answer > 0 and latest_updated <= target_ts:
                    return latest_answer / (10 ** decimals)
            except Exception:
                # ignore and continue to full bounds/search
                pass

            # 4. Determine valid round bounds for this phase
            lo, hi = self._get_phase_round_bounds(feed_addr, phase)
            if lo is None or hi is None or hi < lo:
                continue

            # 5. Binary search inside valid round range (bisection)
            left = lo
            right = hi
            best = None

            while left <= right:
                mid = (left + right) // 2
                round_id = self._encode_round_id(phase, mid)
                try:
                    rd = agg_contract.functions.getRoundData(round_id).call()
                except Exception:
                    # if the call fails, step left to shrink range
                    right = mid - 1
                    continue

                updated = int(rd[3])
                answer = int(rd[1])

                # Skip invalid or empty rounds
                if updated == 0 or answer <= 0:
                    right = mid - 1
                    continue

                if updated > target_ts:
                    # too new → search left half
                    right = mid - 1
                else:
                    # candidate — search right half to find the last <= target
                    best = rd
                    left = mid + 1

            if best:
                return int(best[1]) / (10 ** decimals)

        # No phase had a valid round ≤ target timestamp
        return None

    def _decode_round_id(self, round_id: int):
        """
        Decode Chainlink composite roundId into (phase, roundInPhase).
        roundId = (phaseId << 64) | roundInPhase
        """
        phase = round_id >> 64
        round_in_phase = round_id & ((1 << 64) - 1)
        return phase, round_in_phase

    def _encode_round_id(self, phase: int, round_in_phase: int) -> int:
        """
        Encode (phase, roundInPhase) back into a composite roundId.
        """
        return (phase << 64) | round_in_phase

    def _get_phase_round_bounds(self, feed_addr: str, phase: int):
        """
        Determine the valid (minRoundInPhase, maxRoundInPhase) for a given phase.
        This avoids blind backtracking and drastically reduces RPC calls.
        """
        contract = self._get_contract(feed_addr)

        # 1. Fetch phase aggregator
        try:
            agg_addr = contract.functions.phaseAggregators(phase).call()
        except Exception:
            return None, None

        try:
            if int(agg_addr, 16) == 0:
                return None, None
        except Exception:
            return None, None

        agg_contract = self.w3.eth.contract(address=agg_addr, abi=AGGREGATOR_ABI)

        # 2. Fetch latest round for that aggregator
        try:
            latest = agg_contract.functions.latestRoundData().call()
        except Exception:
            return None, None

        latest_round_id = int(latest[0])
        _, max_round = self._decode_round_id(latest_round_id)

        # minimum valid round number in any phase is 1
        return 1, max_round

    def _find_round_before(self, feed_address: str, target_ts: int):
        latest = self._call_latest(feed_address)
        if latest["updatedAt"] == 0:
            return None
        if latest["updatedAt"] <= target_ts:
            return latest
        latest_round = latest["roundId"]
        latest_ts = latest["updatedAt"]

        # Bounded exponential/backoff search from latest_round downward.
        # This avoids arithmetic on potentially-composite roundIds and prevents huge ids.
        lo_round = None
        lo_data = None
        hi_round = latest_round
        hi_data = latest

        step = 1
        attempts = 0
        max_attempts = 24
        curr = latest_round

        while curr > 1 and attempts < max_attempts:
            prev_round = max(1, curr - step)
            prev_data = self._get_round(feed_address, prev_round)
            attempts += 1
            if prev_data["updatedAt"] == 0:
                # no data for this round, try a larger step
                curr = prev_round
                step *= 2
                continue
            if prev_data["updatedAt"] <= target_ts:
                lo_round = prev_round
                lo_data = prev_data
                break
            # move further back
            curr = prev_round
            step *= 2

        if lo_data is None:
            return None

        # Binary search between lo_round and hi_round (bounded iterations)
        while lo_round < hi_round:
            if hi_round - lo_round <= 1:
                break
            mid = (lo_round + hi_round) // 2
            mid_data = self._get_round(feed_address, mid)
            if mid_data["updatedAt"] == 0:
                hi_round = mid
                continue
            if mid_data["updatedAt"] <= target_ts:
                lo_round = mid
                lo_data = mid_data
            else:
                hi_round = mid

        return lo_data


__all__ = [
    "CHAINLINK_FEEDS",
    "TOKEN_ALIASES",
    "ADDRESS_TO_SYMBOL",
    "ChainlinkPriceFetcher",
    "normalize_symbol",
]
