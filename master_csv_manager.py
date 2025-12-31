"""Central management for the canonical liquidations master CSV.

Simplified version - the V3 scanner writes directly to CSV,
so we only need to ensure the file exists with proper headers.
"""
import os
import csv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")
MASTER_CSV_PATH = os.path.join(DATA_DIR, "liquidations_master.csv")

# Required CSV headers (MUST match aave_v3_liquidations_scanner.py CSV_FIELD_ORDER!)
# Canonical CSV column order used by the frontend download
# Gruppiert: 1) Block/Zeit, 2) LiquidationCall Event, 3) Angereichert, 4) TX-Meta
REQUIRED_HEADERS = [
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
    # ETH Preis pro Block (neu)
    'eth_price_usd_at_block',
]


def ensure_master_csv_exists() -> None:
    """Create the master CSV with headers if it does not exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(MASTER_CSV_PATH):
        with open(MASTER_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=REQUIRED_HEADERS)
            writer.writeheader()


def refresh_master_csv(auto_refill: bool = True) -> bool:
    """No-op: Scanner writes directly to CSV. Kept for API compatibility."""
    ensure_master_csv_exists()
    return True


__all__ = [
    "MASTER_CSV_PATH",
    "REQUIRED_HEADERS",
    "ensure_master_csv_exists",
    "refresh_master_csv",
]

