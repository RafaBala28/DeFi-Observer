# DeFi Observer 2.0

**Ethereum Mainnet** Aave V3 Liquidations Scanner with historical USD enrichment via Chainlink price feeds.

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-2.3+-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

## Quick Start

```powershell
git clone https://github.com/RafaBala28/DeFi_Observer.git
cd DeFi_Observer
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Dashboard: **http://localhost:5000**

## Core Features

### Aave V3 Liquidations Scanner (Ethereum Mainnet)

**Automatic scanning** of all Aave V3 liquidations on **Ethereum Mainnet** from block 16,000,000 (Jan 2023) to present:
- Extracts `LiquidationCall` events from Ethereum mainnet
- Enriches with **historical USD prices** via Chainlink oracles (at exact liquidation block)
- Writes to `data/liquidations_master.csv` with 20+ columns
- **4 public RPC providers** with automatic rotation: PublicNode, LlamaRPC, Cloudflare, Ankr (+ optional Alchemy/Infura)
- Gap detection and automatic backfill
- Live monitoring mode (scans new blocks every 60s)

### Chainlink Price Resolution (Historical)

Every liquidation is enriched with USD values at the **exact liquidation block**:

**Fallback Chain:**
1. **Aave V3 Oracle** - All Aave-listed assets (authoritative live protocol state)
2. **Chainlink Direct USD Feed** - Standard tokens (ETH, WBTC, USDC, etc.)
3. **CAPO Protection** - Aave's Correlated Assets Price Oracle (blockgenau from deployed contracts)
4. **Raw LSD Calculation** - Exchange rate × underlying price (fallback when no CAPO)
5. **Stablecoins** - $1.00 default

**CAPO (Correlated Assets Price Oracle):**
- **Generic cap mechanism** for LSDs, stablecoins, and PT tokens
- Limits exchange rate growth (e.g., 9.68% yearly max for wstETH)
- Formula: `max_ratio = snapshot + (growth × elapsed_time)`
- **Historically accurate**: Reads parameters blockgenau from deployed Aave contracts
- Deployed: Late 2024 on Ethereum mainnet (Aave Governance)
- **7 CAPO Adapters on Ethereum Mainnet**: wstETH (0xe1D9...), rETH (0x6929...), cbETH (0x8893...), weETH (0x8762...), rsETH (0x7292...), osETH (0x2b86...), sUSDe (0x42bc...)
- **Note**: Additional CAPO adapters exist on other chains (Arbitrum, Linea, Avalanche, etc.) but are not used on Ethereum Mainnet. Stablecoins (USDC, USDT, DAI, EURC) use direct Chainlink feeds on Mainnet.
- Source: https://github.com/bgd-labs/aave-capo
- Protects against flash loan attacks and rate manipulation
- **Automatic fallback**: If CAPO contract not deployed at block → uses Raw LSD

**LSD (Liquid Staking Derivatives):**
- Separate fallback when CAPO not available/configured
- Simple calculation: `exchange_rate × underlying_price`
- No rate cap applied (raw calculation)

**Log Prefixes:**
- `[AAVE Oracle]` - Aave oracle (Priority 1)
- `[Chainlink]` - Direct USD feed (Priority 2)
- `[CAPO]` - CAPO-protected price with rate cap (Priority 3)
- `[Raw LSD]` - LSD without CAPO protection (Priority 4)

## Installation & Setup

### Requirements
- Python 3.10+
- Windows / macOS / Linux
- (Optional) Alchemy/Infura API Keys for better RPC performance

### Installation (PowerShell)

```powershell
# 1) Clone repository
git clone https://github.com/RafaBala28/DeFi_Observer.git
cd DeFi_Observer

# 2) Create virtual environment
python -m venv .venv

# 3) Activate virtual environment
.\.venv\Scripts\Activate.ps1  # Windows PowerShell

# 4) Install dependencies
pip install -r requirements.txt

# 5) (Optional) Configure API Keys
copy .env.example .env
# Edit .env and add ALCHEMY_API_KEY / INFURA_API_KEY

# 6) Start app
python app.py
```

Open dashboard: **http://localhost:5000**

### macOS / Linux Installation

```bash
# 1-2) Clone & Virtual Environment
git clone https://github.com/RafaBala28/DeFi_Observer.git
cd DeFi_Observer
python -m venv .venv

# 3) Activate
source .venv/bin/activate

# 4-6) Dependencies & Start
pip install -r requirements.txt
cp .env.example .env  # Optional: Add API Keys
python app.py
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Web server port | `5000` |
| `HOST_IP` | LAN IP for banner (auto-detect if empty) | auto |
| `DISABLE_BACKGROUND_SERVICES` | Disables scanner/backfills | `0` |
| `ALCHEMY_API_KEY` | Alchemy RPC API Key (optional) | - |
| `INFURA_API_KEY` | Infura RPC API Key (optional) | - |



## Common Commands

### Scanner Maintenance (Standalone)

**Validate CSV** (checks prices + fills gaps):
```powershell
# PowerShell
& ".\.venv\Scripts\python.exe" ".\tools\aave_v3_liquidations_scanner.py" --validate

# Bash/macOS/Linux
python ./tools/aave_v3_liquidations_scanner.py --validate
```

**Note**: Can be RPC-intensive - API keys in `.env` recommended!

### Change Port

```powershell
$env:PORT = "8080"
python app.py
```

### Scanner Only (without Web UI)

```powershell
# With helper script (Windows)
.\scripts\start-defi-observer.ps1 -RunScanner

# Direct
python .\tools\aave_v3_liquidations_scanner.py
```



### CSV Output Structure

**File:** `data/liquidations_master.csv`

**20 columns organized in 4 groups:**

1. **Block/Time** - block, timestamp, datetime_utc
2. **Event Data** - collateralAsset, debtAsset, user, liquidator, collateralOut, debtToCover, receiveAToken
3. **Enriched** - collateralSymbol, debtSymbol, collateral_price_usd_at_block, debt_price_usd_at_block, collateral_value_usd, debt_value_usd
4. **Transaction** - tx, block_builder, gas_used, gas_price_gwei


## Liquidations CSV - Data Structure

### CSV Generation Pipeline

The scanner processes `LiquidationCall` events from Aave V3:

```
┌───────────────────────────────────────────────────────────────────────┐
│                     CSV Generation Pipeline                           │
├───────────────────────────────────────────────────────────────────────┤
│ 1. EVENT DETECTION                                                    │
│    └── eth_getLogs(topic=LiquidationCall, fromBlock, toBlock)        │
│                                                                       │
│ 2. BLOCK DATA ENRICHMENT                                              │
│    └── eth_getBlock(blockNumber) → timestamp, miner (block_builder)  │
│                                                                       │
│ 3. TOKEN SYMBOL RESOLUTION                                            │
│    └── TOKEN_SYMBOLS mapping (address → symbol)                      │
│    └── Fallback: ERC20.symbol() call                                 │
│                                                                       │
│ 4. CHAINLINK PRICE LOOKUP (Historical)                                │
│    └── Canonical Fallback Chain (see above)                          │
│    └── Returns: collateral_price_usd_at_block,                       │
│                 debt_price_usd_at_block                              │
│                                                                       │
│ 5. USD VALUE CALCULATION                                              │
│    └── collateral_value_usd = collateralOut × collateral_price       │
│    └── debt_value_usd = debtToCover × debt_price                     │
│                                                                       │
│ 6. GAS DATA                                                           │
│    └── eth_getTransactionReceipt(txHash) → gasUsed                   │
│    └── eth_getTransaction(txHash) → gasPrice                         │
│                                                                       │
│ 7. CSV APPEND                                                         │
│    └── Write row to data/liquidations_master.csv (immediate)         │
└───────────────────────────────────────────────────────────────────────┘
```

### CSV Columns (20 Columns in 4 Groups)

#### 1. Block/Time Data

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `block` | int | Ethereum block number | `19500000` |
| `timestamp` | int | Unix timestamp | `1710432000` |
| `datetime_utc` | string | UTC date readable | `2024-03-14 12:00:00` |

#### 2. LiquidationCall Event Data (Raw Smart Contract Data)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `collateralAsset` | address | Collateral token address | `0xC02aaA39b...` |
| `debtAsset` | address | Debt token address | `0xA0b86991c...` |
| `user` | address | Liquidated borrower | `0x1234...abcd` |
| `liquidator` | address | Liquidator address | `0xabcd...1234` |
| `collateralOut` | float | Seized collateral amount | `5.5` (WETH) |
| `debtToCover` | float | Repaid debt amount | `15000.0` (USDC) |
| `receiveAToken` | bool | Liquidator receives aTokens | `False` |

#### 3. Enriched Data (Symbols, Prices, USD Values)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `collateralSymbol` | string | Collateral token symbol | `WETH` |
| `debtSymbol` | string | Debt token symbol | `USDC` |
| `collateral_price_usd_at_block` | float | Chainlink price at event block | `3456.17` |
| `debt_price_usd_at_block` | float | Chainlink price at event block | `1.0` |
| `collateral_value_usd` | float | `collateralOut × collateral_price` | `19008.94` |
| `debt_value_usd` | float | `debtToCover × debt_price` | `15000.0` |

**Note**: When no price is found (no feed exists), price/value fields remain **empty** (not 0).

#### 4. Transaction Metadata

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `tx` | hash | Transaction hash | `0xe933b1...30ec3` |
| `block_builder` | address | Block miner/builder (MEV relevant) | `0x95222290DD...` |
| `gas_used` | int | Actual gas consumed | `598895` |
| `gas_price_gwei` | float | Gas price in Gwei | `29.87` |

### Data Sources per Column

| Column | Source | RPC Call |
|--------|--------|----------|
| `block`, `timestamp` | Block Header | `eth_getBlock` |
| `collateralAsset`, `debtAsset`, `user`, ... | Event Log | `eth_getLogs` |
| `collateralSymbol`, `debtSymbol` | Mapping or ERC20 | `symbol()` |
| `*_price_usd_at_block` | Canonical Fallback Chain | Chainlink/Aave/LSD/Comp |
| `*_value_usd` | Calculated | - |
| `tx` | Event Log | `eth_getLogs` |
| `block_builder` | Block Header | `eth_getBlock` |
| `gas_used` | TX Receipt | `eth_getTransactionReceipt` |
| `gas_price_gwei` | Transaction | `eth_getTransaction` |

## API Reference

Main endpoints:
- `GET /api/aave/liquidations/recent?limit=50` - Recent liquidations
- `GET /download` - Download full CSV
- `GET /api/eth/network` - Network stats
- `GET /debug/rpc` - RPC provider status

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a Pull Request

## License

MIT License - see [LICENSE](LICENSE) file

Summary:
- Use, copy, modify, merge, publish, distribute, sublicense
- Commercial and private use
- Copyright notice and license text must be included
- Software provided "as is" without warranty
- Authors not liable for damages

## Acknowledgments

- [Aave](https://aave.com/) - Lending protocol
- [Uniswap](https://uniswap.org/) - DEX protocol  
- [Chainlink](https://chain.link/) - Price oracles
- Public RPC providers: PublicNode, LlamaRPC, Cloudflare, Ankr

## Current Stats (December 2025)

An integrity run was performed after recent price resolution changes:

**Token Coverage:**
- Tested symbols: **46** unique collateral/debt symbols
- Resolution success: **46 / 46** (100%) via canonical fallback chain
- Source distribution (approx.):
  - Chainlink Direct: 26
  - Aave Oracle: 11
  - LSD Exchange-Rate: 5
  - ETH/BTC Composition: 3
  - Stable Fallback: 1

**Remarks:**
- Scanner enforces canonical fallback order
- For live enrichment: Add API keys (`ALCHEMY_API_KEY` / `INFURA_API_KEY`)

---

## AI/ML Notice

Parts of this project (documentation edits, helper scripts, code suggestions) were created or refined with assistance from AI/ML tools.

**Responsibility:**
- Repository owner and deployer are responsible for validation, testing, and security
- RPC/API keys, `.env` contents, secrets management, and access controls must be verified before production use
- Scanner records USD values and block ETH prices for reproducibility of historical values

**Auditability:**
- All price resolutions traceable through logs (`[Chainlink]`, `[AAVE Oracle]`, etc.)
- CAPO applications are logged
- CSV backups created during repairs

For a formal legal disclaimer extension, please let me know.

