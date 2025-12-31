"""
ETH Price Storage - Memory-optimized persistent storage for ETH price data
Stores price history with automatic pruning and deduplication
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Storage configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PRICE_FILE = os.path.join(DATA_DIR, "eth_price_history.json")

# Memory limits
MAX_RETENTION_DAYS = 30  # Keep 30 days of price data
MAX_PRICE_POINTS = 50000  # Maximum number of price points (~2.5MB compressed)
PRUNE_THRESHOLD = 60000  # Trigger pruning at 60k points

# Compressed field names to save space
# Full format: {"timestamp": 123, "price": 3000.50, "source": "chainlink", "decimals": 8}
# Compressed: {"t": 123, "p": 3000.50, "s": "chainlink", "d": 8}
FIELD_MAP = {
    "timestamp": "t",
    "price": "p",
    "source": "s",
    "decimals": "d"
}

REVERSE_FIELD_MAP = {v: k for k, v in FIELD_MAP.items()}


def _compress_price_data(price_data: Dict) -> Dict:
    """Compress price data by shortening field names"""
    compressed = {}
    for key, value in price_data.items():
        compressed_key = FIELD_MAP.get(key, key)
        compressed[compressed_key] = value
    return compressed


def _decompress_price_data(compressed_data: Dict) -> Dict:
    """Decompress price data by restoring field names"""
    decompressed = {}
    for key, value in compressed_data.items():
        original_key = REVERSE_FIELD_MAP.get(key, key)
        decompressed[original_key] = value
    return decompressed


def _ensure_data_dir():
    """Ensure data directory exists"""
    os.makedirs(DATA_DIR, exist_ok=True)


def _load_price_history() -> Dict:
    """Load price history from disk"""
    if not os.path.exists(PRICE_FILE):
        return {
            "prices": [],
            "metadata": {
                "last_updated": None,
                "total_count": 0,
                "oldest_timestamp": None,
                "newest_timestamp": None
            }
        }
    
    try:
        with open(PRICE_FILE, 'r') as f:
            loaded_data = json.load(f)
        
        # Handle legacy format (list instead of dict)
        if isinstance(loaded_data, list):
            logger.warning("Converting legacy price history format...")
            # Convert old list format to new dict format
            return {
                "prices": [],  # Legacy data not compatible, start fresh
                "metadata": {
                    "last_updated": None,
                    "total_count": 0,
                    "oldest_timestamp": None,
                    "newest_timestamp": None
                }
            }
        
        # Ensure metadata exists
        if "metadata" not in loaded_data:
            loaded_data["metadata"] = {
                "last_updated": None,
                "total_count": len(loaded_data.get("prices", [])),
                "oldest_timestamp": None,
                "newest_timestamp": None
            }
        
        return loaded_data
        
    except Exception as e:
        logger.error(f"Error loading price history: {e}")
        return {
            "prices": [],
            "metadata": {
                "last_updated": None,
                "total_count": 0,
                "oldest_timestamp": None,
                "newest_timestamp": None
            }
        }


def _save_price_history(data: Dict):
    """Save price history to disk"""
    _ensure_data_dir()
    try:
        with open(PRICE_FILE, 'w') as f:
            json.dump(data, f, separators=(',', ':'))  # Compact JSON
        logger.debug(f"[ETH Price] Saved {len(data['prices'])} price points")
    except Exception as e:
        logger.error(f"Error saving price history: {e}")


def _prune_price_history(data: Dict) -> Dict:
    """Remove old price data to stay within memory limits"""
    prices = data["prices"]
    
    if len(prices) <= MAX_PRICE_POINTS:
        return data
    
    logger.info(f"Pruning price history: {len(prices)} -> {MAX_PRICE_POINTS}")
    
    # Sort by timestamp (newest first)
    prices.sort(key=lambda x: x.get("t", 0), reverse=True)
    
    # Keep only MAX_PRICE_POINTS newest entries
    pruned_prices = prices[:MAX_PRICE_POINTS]
    
    # Also remove entries older than MAX_RETENTION_DAYS
    cutoff_time = int((datetime.now() - timedelta(days=MAX_RETENTION_DAYS)).timestamp())
    pruned_prices = [p for p in pruned_prices if p.get("t", 0) >= cutoff_time]
    
    # Update metadata
    if pruned_prices:
        data["metadata"]["oldest_timestamp"] = min(p.get("t", 0) for p in pruned_prices)
        data["metadata"]["newest_timestamp"] = max(p.get("t", 0) for p in pruned_prices)
        data["metadata"]["total_count"] = len(pruned_prices)
    else:
        data["metadata"]["oldest_timestamp"] = None
        data["metadata"]["newest_timestamp"] = None
        data["metadata"]["total_count"] = 0
    
    data["prices"] = pruned_prices
    
    logger.info(f"Pruned to {len(pruned_prices)} price points")
    
    return data


def append_price(timestamp: int, price: float, source: str = "unknown", decimals: int = 18):
    """
    Append a single price point to storage
    
    Args:
        timestamp: Unix timestamp
        price: ETH price in USD
        source: Price source (chainlink, uniswap_v3, uniswap_v2, coingecko, etc.)
        decimals: Decimals used in price (for tracking precision)
    """
    data = _load_price_history()
    
    # Create compressed price entry
    price_entry = _compress_price_data({
        "timestamp": timestamp,
        "price": price,
        "source": source,
        "decimals": decimals
    })
    
    # Check for duplicates (same timestamp)
    existing_timestamps = {p.get("t") for p in data["prices"]}
    if timestamp in existing_timestamps:
        logger.debug(f"Price already exists for timestamp {timestamp}, skipping")
        return
    
    # Add new price
    data["prices"].append(price_entry)
    data["metadata"]["last_updated"] = int(datetime.now().timestamp())
    data["metadata"]["total_count"] = len(data["prices"])
    
    # Update timestamp range
    if data["metadata"]["oldest_timestamp"] is None:
        data["metadata"]["oldest_timestamp"] = timestamp
    else:
        data["metadata"]["oldest_timestamp"] = min(data["metadata"]["oldest_timestamp"], timestamp)
    
    if data["metadata"]["newest_timestamp"] is None:
        data["metadata"]["newest_timestamp"] = timestamp
    else:
        data["metadata"]["newest_timestamp"] = max(data["metadata"]["newest_timestamp"], timestamp)
    
    # Prune if needed
    if len(data["prices"]) >= PRUNE_THRESHOLD:
        data = _prune_price_history(data)
    
    _save_price_history(data)
    logger.debug(f"[ETH Price] Stored: ${price:.2f} from {source} at {timestamp}")


def append_prices(price_list: List[Dict]):
    """
    Append multiple price points in bulk
    
    Args:
        price_list: List of dicts with keys: timestamp, price, source, decimals
    """
    if not price_list:
        return
    
    data = _load_price_history()
    
    # Get existing timestamps for deduplication
    existing_timestamps = {p.get("t") for p in data["prices"]}
    
    # Add new prices (skip duplicates)
    new_count = 0
    for price_data in price_list:
        timestamp = price_data.get("timestamp")
        if timestamp not in existing_timestamps:
            compressed_entry = _compress_price_data(price_data)
            data["prices"].append(compressed_entry)
            existing_timestamps.add(timestamp)
            new_count += 1
    
    if new_count == 0:
        logger.debug("No new prices to add (all duplicates)")
        return
    
    # Update metadata
    data["metadata"]["last_updated"] = int(datetime.now().timestamp())
    data["metadata"]["total_count"] = len(data["prices"])
    
    # Update timestamp range
    all_timestamps = [p.get("t") for p in data["prices"] if p.get("t")]
    if all_timestamps:
        data["metadata"]["oldest_timestamp"] = min(all_timestamps)
        data["metadata"]["newest_timestamp"] = max(all_timestamps)
    
    # Prune if needed
    if len(data["prices"]) >= PRUNE_THRESHOLD:
        data = _prune_price_history(data)
    
    _save_price_history(data)
    logger.debug(f"[ETH Price] Stored {new_count} new price points (total: {len(data['prices'])})")


def get_prices(hours: Optional[int] = None, limit: Optional[int] = None) -> List[Dict]:
    """
    Get price history
    
    Args:
        hours: Only return prices from last N hours (None = all)
        limit: Maximum number of prices to return (None = all)
    
    Returns:
        List of decompressed price dicts, sorted by timestamp (oldest first)
    """
    data = _load_price_history()
    prices = data.get("prices", [])
    
    # Filter by time range
    if hours is not None:
        cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
        prices = [p for p in prices if p.get("t", 0) >= cutoff_time]
    
    # Sort by timestamp (oldest first)
    prices.sort(key=lambda x: x.get("t", 0))
    
    # Apply limit
    if limit is not None:
        prices = prices[:limit]
    
    # Decompress before returning
    return [_decompress_price_data(p) for p in prices]


def get_latest_price() -> Optional[Dict]:
    """Get the most recent price point"""
    data = _load_price_history()
    
    prices = data.get("prices", [])
    
    if not prices:
        return None
    
    # Find price with newest timestamp
    latest = max(prices, key=lambda x: x.get("t", 0))
    
    return _decompress_price_data(latest)


def get_stats() -> Dict:
    """Get storage statistics"""
    data = _load_price_history()
    
    file_size_kb = 0
    if os.path.exists(PRICE_FILE):
        file_size_kb = os.path.getsize(PRICE_FILE) / 1024
    
    metadata = data.get("metadata", {})
    
    return {
        "total_count": metadata.get("total_count", 0),
        "oldest_timestamp": metadata.get("oldest_timestamp"),
        "newest_timestamp": metadata.get("newest_timestamp"),
        "last_updated": metadata.get("last_updated"),
        "file_size_kb": round(file_size_kb, 2),
        "max_points": MAX_PRICE_POINTS,
        "retention_days": MAX_RETENTION_DAYS
    }


def clear_all():
    """Clear all stored prices (for testing/reset)"""
    if os.path.exists(PRICE_FILE):
        os.remove(PRICE_FILE)
        logger.info("Cleared all price history")
