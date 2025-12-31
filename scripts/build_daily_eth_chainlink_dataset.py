"""
Build Daily ETH/USD Price Dataset from Chainlink for Aave V3 Liquidation Analysis

This script constructs a clean daily ETH/USD time series from the Chainlink price feed,
starting 7 days before the first observed Aave V3 liquidation event.

Dataset is designed for econometric analysis (daily returns, rolling volatility, regression).

Author: DeFi Observer 2.0
Date: 2025-12-25
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import pandas as pd
from web3 import Web3
from web3.exceptions import BlockNotFound

# Add parent directory to path for imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ============================================================================
# CONFIGURATION
# ============================================================================

# First observed Aave V3 liquidation on Ethereum mainnet
FIRST_AAVE_V3_LIQ_BLOCK = 16521648

# Chainlink ETH/USD AggregatorV3 contract address on Ethereum mainnet
CHAINLINK_ETH_USD_ADDRESS = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

# AggregatorV3Interface ABI (minimal required methods)
AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint80", "name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# Output file
OUTPUT_CSV = "eth_chainlink_daily_pre_aave_v3.csv"

# Status file for tracking updates (like liquidation scanner)
STATUS_FILE = "eth_price_dataset_status.json"

# Retry configuration for RPC calls
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds


# ============================================================================
# WEB3 SETUP
# ============================================================================

def get_web3() -> Web3:
    """Initialize Web3 connection using environment variable."""
    rpc_url = os.environ.get('ETH_RPC_URL')
    
    if not rpc_url:
        # Fallback: try to import from project's web3_utils
        try:
            from web3_utils import get_web3 as project_get_web3
            print("Using project's Web3 connection...")
            return project_get_web3()
        except ImportError:
            raise ValueError(
                "ETH_RPC_URL environment variable not set and project web3_utils not available.\n"
                "Please set ETH_RPC_URL to an Ethereum mainnet RPC endpoint."
            )
    
    print(f"Connecting to Ethereum mainnet: {rpc_url[:50]}...")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    
    if not w3.is_connected():
        raise ConnectionError("Failed to connect to Ethereum RPC endpoint")
    
    chain_id = w3.eth.chain_id
    if chain_id != 1:
        raise ValueError(f"Wrong network! Expected Ethereum mainnet (chain_id=1), got {chain_id}")
    
    print(f"Connected to Ethereum mainnet (chain_id={chain_id})")
    return w3


# ============================================================================
# HELPER FUNCTIONS WITH RETRY LOGIC
# ============================================================================

def retry_call(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Execute a function with retry logic for RPC calls."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Retry {attempt + 1}/{max_retries} after error: {str(e)[:100]}")
            time.sleep(delay * (attempt + 1))


def get_block_with_retry(w3: Web3, block_number: int) -> Dict:
    """Get block data with retry logic."""
    return retry_call(w3.eth.get_block, block_number)


def get_round_data_with_retry(contract, round_id: int) -> Tuple:
    """Get Chainlink round data with retry logic."""
    return retry_call(contract.functions.getRoundData(round_id).call)


# ============================================================================
# CHAINLINK PRICE FEED FUNCTIONS
# ============================================================================

def get_block_timestamp_utc(w3: Web3, block_number: int) -> datetime:
    """Get UTC timestamp for a given block number."""
    block = get_block_with_retry(w3, block_number)
    return datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)


def find_block_at_time(w3: Web3, target_timestamp: int, 
                       start_block: int, end_block: int) -> int:
    """
    Binary search to find the block closest to (but not after) target timestamp.
    
    Args:
        w3: Web3 instance
        target_timestamp: Unix timestamp to search for
        start_block: Lower bound block number
        end_block: Upper bound block number
    
    Returns:
        Block number closest to target_timestamp (but not after)
    """
    print(f"   Binary searching blocks {start_block:,} to {end_block:,}...", end='', flush=True)
    
    left, right = start_block, end_block
    result = start_block
    
    iterations = 0
    while left <= right:
        iterations += 1
        mid = (left + right) // 2
        
        try:
            block = get_block_with_retry(w3, mid)
            block_ts = block['timestamp']
            
            if block_ts <= target_timestamp:
                result = mid
                left = mid + 1
            else:
                right = mid - 1
                
        except BlockNotFound:
            right = mid - 1
        except Exception as e:
            print(f"\n   Error at block {mid}: {e}")
            right = mid - 1
    
    print(f" found block {result:,} ({iterations} iterations)")
    return result


def get_latest_chainlink_price_at_time(
    w3: Web3,
    contract,
    target_timestamp: int,
    decimals: int,
    search_start_block: int,
    search_end_block: int
) -> Optional[Dict]:
    """
    Find the latest Chainlink price update at or before target_timestamp.
    
    Strategy:
    1. Get the latest round as of target_timestamp
    2. Use binary search to find the approximate block at target_timestamp
    3. Query latestRoundData at that block
    4. Verify the round's updatedAt <= target_timestamp
    5. Get round data including block number
    
    Args:
        w3: Web3 instance
        contract: Chainlink aggregator contract
        target_timestamp: Unix timestamp (UTC)
        decimals: Price feed decimals
        search_start_block: Block to start search from
        search_end_block: Block to end search at
    
    Returns:
        Dict with price data or None if not found
    """
    try:
        # Find block at target timestamp
        target_block = find_block_at_time(w3, target_timestamp, 
                                         search_start_block, search_end_block)
        
        # Get latest round data at that block
        round_data = retry_call(
            contract.functions.latestRoundData().call,
            block_identifier=target_block
        )
        
        round_id, answer, started_at, updated_at, answered_in_round = round_data
        
        # Verify this round was updated before or at target time
        if updated_at > target_timestamp:
            # This round is too recent, need to look back
            print(f"   Round {round_id} updated at {updated_at}, after target {target_timestamp}")
            # Try to get previous rounds (simplified: just return None)
            return None
        
        # Get the block where this round was updated
        update_block = find_block_at_time(w3, updated_at, 
                                         search_start_block, target_block)
        update_block_data = get_block_with_retry(w3, update_block)
        
        # Convert price using decimals
        eth_price_usd = float(answer) / (10 ** decimals)
        
        return {
            'round_id': round_id,
            'chainlink_updatedAt': updated_at,
            'chainlink_updatedAt_utc': datetime.fromtimestamp(updated_at, tz=timezone.utc),
            'update_block_number': update_block,
            'update_block_time': update_block_data['timestamp'],
            'update_block_time_utc': datetime.fromtimestamp(update_block_data['timestamp'], tz=timezone.utc),
            'eth_price_usd': eth_price_usd,
            'answer_raw': answer
        }
        
    except Exception as e:
        print(f"\n   ❌ Error getting price at timestamp {target_timestamp}: {e}")
        return None


# ============================================================================
# STATUS TRACKING (same pattern as liquidation scanner)
# ============================================================================

def write_status(status: str, message: str = None, current_date: str = None, total_days: int = None):
    """Write status file for monitoring (compatible with liquidation scanner pattern)."""
    status_path = os.path.join(PROJECT_ROOT, 'data', STATUS_FILE)
    payload = {
        'status': status,  # 'running', 'completed', 'error', 'idle'
        'current_date': current_date,
        'total_days': total_days,
        'last_updated': int(time.time()),
        'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        'message': message
    }
    try:
        os.makedirs(os.path.dirname(status_path), exist_ok=True)
        with open(status_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"Failed to write status: {e}")


def get_last_date_from_csv(csv_path: str) -> Optional[datetime]:
    """Read CSV and return the most recent date (checkpoint for incremental updates)."""
    if not os.path.exists(csv_path):
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty or 'date_utc' not in df.columns:
            return None
        
        # Get the last date from CSV
        last_date_str = df['date_utc'].iloc[-1]
        last_date = datetime.strptime(last_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        return last_date
    except Exception as e:
        print(f"Error reading checkpoint from CSV: {e}")
        return None


# ============================================================================
# MAIN DATASET BUILDER
# ============================================================================

def build_daily_eth_dataset(w3: Web3, incremental: bool = True) -> pd.DataFrame:
    """
    Build daily ETH/USD price dataset from Chainlink.
    
    Returns:
        DataFrame with daily ETH prices
    """
    print("\n" + "="*80)
    print("BUILDING DAILY ETH/USD DATASET FROM CHAINLINK")
    print("="*80 + "\n")
    
    # Initialize Chainlink contract
    print(f"Initializing Chainlink ETH/USD aggregator at {CHAINLINK_ETH_USD_ADDRESS}...")
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(CHAINLINK_ETH_USD_ADDRESS),
        abi=AGGREGATOR_V3_ABI
    )
    
    # Get decimals
    decimals = retry_call(contract.functions.decimals().call)
    print(f"   Decimals: {decimals}")
    
    # Get timestamp of first liquidation block
    print(f"\nGetting timestamp of first Aave V3 liquidation block {FIRST_AAVE_V3_LIQ_BLOCK:,}...")
    first_liq_time_utc = get_block_timestamp_utc(w3, FIRST_AAVE_V3_LIQ_BLOCK)
    print(f"   Block timestamp: {first_liq_time_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Calculate date range: 7 days before first liquidation to TODAY
    start_date_utc = (first_liq_time_utc - timedelta(days=7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # End date is today (current date)
    end_date_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    
    # CHECKPOINT LOGIC: Check if we should do incremental update
    output_path = os.path.join(PROJECT_ROOT, 'data', OUTPUT_CSV)
    last_date_in_csv = None
    existing_df = None
    
    if incremental and os.path.exists(output_path):
        last_date_in_csv = get_last_date_from_csv(output_path)
        if last_date_in_csv:
            # Load existing data for appending
            existing_df = pd.read_csv(output_path)
            # Resume from day after last date
            start_date_utc = (last_date_in_csv + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            print(f"\nINCREMENTAL UPDATE MODE")
            print(f"   Last date in CSV: {last_date_in_csv.strftime('%Y-%m-%d')}")
            print(f"   Resuming from:    {start_date_utc.strftime('%Y-%m-%d')}")
            print(f"   Existing records: {len(existing_df)}")
    
    # Check if already up to date
    if start_date_utc > end_date_utc:
        print(f"\nAlready up to date! Last date: {last_date_in_csv.strftime('%Y-%m-%d')}")
        write_status('idle', message='Dataset is up to date', current_date=last_date_in_csv.strftime('%Y-%m-%d'))
        return existing_df
    
    print(f"\nDataset date range:")
    print(f"   Start: {start_date_utc.strftime('%Y-%m-%d')} {'(resuming)' if last_date_in_csv else '(7 days before first liquidation)'}")
    print(f"   End:   {end_date_utc.strftime('%Y-%m-%d')} (today)")
    print(f"   Total: {(end_date_utc - start_date_utc).days + 1} days")
    print(f"   Estimated time: ~{((end_date_utc - start_date_utc).days + 1) * 3} seconds")
    
    # Generate daily sampling times (23:59:59 UTC each day - end of day)
    date_range = pd.date_range(
        start=start_date_utc,
        end=end_date_utc,
        freq='D',
        tz=timezone.utc
    )
    
    sampling_times = [
        dt.replace(hour=23, minute=59, second=59, microsecond=0)
        for dt in date_range
    ]
    
    print(f"\nSampling times: 23:59:59 UTC each day (end of day)")
    print(f"   First sample: {sampling_times[0].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   Last sample:  {sampling_times[-1].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Determine search block range
    # We need blocks from ~8 days before first liquidation to current block
    search_start_block = FIRST_AAVE_V3_LIQ_BLOCK - (8 * 24 * 60 * 4)  # ~8 days worth of blocks (15s/block)
    search_end_block = w3.eth.block_number  # Current latest block
    
    print(f"\nBlock search range: {search_start_block:,} to {search_end_block:,}")
    print(f"   Latest block: {search_end_block:,}")
    
    # Collect price data for each day
    print(f"\nCollecting Chainlink prices...")
    print("-" * 80)
    
    # Write initial status
    total_days = len(sampling_times)
    write_status('running', 
                message=f'Scanning {total_days} days', 
                current_date=sampling_times[0].strftime('%Y-%m-%d'),
                total_days=total_days)
    
    records = []
    for i, sample_time in enumerate(sampling_times, 1):
        date_str = sample_time.strftime('%Y-%m-%d')
        
        # Update status every 5 days or on first/last
        if i % 5 == 1 or i == len(sampling_times):
            write_status('running', 
                        message=f'Processing {i}/{len(sampling_times)} days',
                        current_date=date_str,
                        total_days=len(sampling_times))
        
        # Print progress less frequently for large datasets
        if i % 10 == 1 or i == len(sampling_times):
            print(f"\n[{i}/{len(sampling_times)}] {date_str} 23:59:59 UTC")
        else:
            print(f"[{i}/{len(sampling_times)}] {date_str} 23:59:59 UTC", end=' ')
        
        target_ts = int(sample_time.timestamp())
        
        price_data = get_latest_chainlink_price_at_time(
            w3, contract, target_ts, decimals,
            search_start_block, search_end_block
        )
        
        if price_data:
            record = {
                'date_utc': date_str,
                'sample_time_utc': '23:59:59',
                'round_id': price_data['round_id'],
                'chainlink_updatedAt_utc': price_data['chainlink_updatedAt_utc'].strftime('%Y-%m-%d %H:%M:%S'),
                'update_block_number': price_data['update_block_number'],
                'update_block_time_utc': price_data['update_block_time_utc'].strftime('%Y-%m-%d %H:%M:%S'),
                'eth_price_usd': price_data['eth_price_usd']
            }
            records.append(record)
            # Print details only every 10th record or last one
            if i % 10 == 1 or i == len(sampling_times):
                print(f"   ETH Price: ${price_data['eth_price_usd']:,.2f} (Round {price_data['round_id']})")
                print(f"      Updated: {price_data['chainlink_updatedAt_utc'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
                print(f"      Block: {price_data['update_block_number']:,}")
            else:
                print(f"${price_data['eth_price_usd']:,.2f}")
        else:
            print(f"X" if i % 10 != 1 else f"   No price data found")
    
    print("\n" + "-" * 80)
    
    # Create DataFrame from new records
    new_df = pd.DataFrame(records)
    
    # Combine with existing data if doing incremental update
    if existing_df is not None and not existing_df.empty:
        df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"\nIncremental update complete: {len(new_df)} new observations")
        print(f"   Total records: {len(df)} (was {len(existing_df)})")
    else:
        df = new_df
        print(f"\nDataset complete: {len(df)} observations")
    
    return df


# ============================================================================
# MAIN
# ============================================================================

def main(incremental: bool = True):
    """Main entry point.
    
    Args:
        incremental: If True, resume from last date in CSV. If False, rebuild from scratch.
    """
    print("\n" + "="*80)
    print("DAILY ETH/USD CHAINLINK DATASET BUILDER")
    print("="*80)
    print(f"\nMode: {'INCREMENTAL UPDATE' if incremental else 'FULL REBUILD'}")
    print(f"\nTarget: Daily price series starting 7 days before first Aave V3 liquidation (block {FIRST_AAVE_V3_LIQ_BLOCK:,})")
    print(f"Output: {OUTPUT_CSV}")
    
    try:
        # Initialize Web3
        w3 = get_web3()
        
        # Build dataset (with incremental support)
        df = build_daily_eth_dataset(w3, incremental=incremental)
        
        if df.empty:
            print("\n❌ No data collected. Exiting.")
            return 1
        
        # Display results
        print("\n" + "="*80)
        print("DATASET PREVIEW")
        print("="*80 + "\n")
        
        print("First 3 rows:")
        print(df.head(3).to_string(index=False))
        
        print("\n" + "-" * 80 + "\n")
        
        print("Last 3 rows:")
        print(df.tail(3).to_string(index=False))
        
        # Summary statistics
        print("\n" + "="*80)
        print("SUMMARY STATISTICS")
        print("="*80 + "\n")
        
        print(f"Observations:    {len(df)}")
        print(f"Date range:      {df['date_utc'].iloc[0]} to {df['date_utc'].iloc[-1]}")
        print(f"ETH price range: ${df['eth_price_usd'].min():,.2f} to ${df['eth_price_usd'].max():,.2f}")
        print(f"Mean price:      ${df['eth_price_usd'].mean():,.2f}")
        print(f"Std dev:         ${df['eth_price_usd'].std():,.2f}")
        
        # Save to CSV
        output_path = os.path.join(PROJECT_ROOT, 'data', OUTPUT_CSV)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.to_csv(output_path, index=False)
        print(f"\nDataset saved to: {output_path}")
        
        # Verify file
        file_size = os.path.getsize(output_path)
        print(f"   File size: {file_size:,} bytes")
        
        # Write completion status
        write_status('completed',
                    message=f'Dataset updated successfully ({len(df)} records)',
                    current_date=df['date_utc'].iloc[-1],
                    total_days=len(df))
        
        print("\n" + "="*80)
        print("SUCCESS - Dataset ready for analysis")
        print("="*80 + "\n")
        
        return 0
        
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        # Write error status
        write_status('error', message=f'Error: {str(e)[:100]}')
        
        return 1


if __name__ == "__main__":
    # Support command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Build/update daily ETH price dataset')
    parser.add_argument('--full', action='store_true', 
                       help='Full rebuild instead of incremental update')
    args = parser.parse_args()
    
    sys.exit(main(incremental=not args.full))
