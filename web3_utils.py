"""
DeFi Observer 2.0 - Web3 Connection Utilities
Centralized Web3 connection management with fallback RPC providers
"""
from web3 import Web3
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict, deque
import logging
import requests
import time

from config import get_chain_config, ACTIVE_CHAIN

logger = logging.getLogger(__name__)

# Enforce repository-wide active chain to Ethereum mainnet for mainnet-only deployments
assert ACTIVE_CHAIN == 'ethereum', "ACTIVE_CHAIN must be 'ethereum' for mainnet-only operation"

# Global RPC tracking (shared across all modules)
_rpc_call_success = defaultdict(int)
_rpc_call_errors = defaultdict(int)
_rpc_response_times = defaultdict(lambda: deque(maxlen=100))
_current_provider_url = None


def track_rpc_success(provider_url: str, response_time: float):
    """Track successful RPC call"""
    global _current_provider_url
    _rpc_call_success[provider_url] += 1
    _rpc_response_times[provider_url].append(response_time)
    _current_provider_url = provider_url  # Update active provider on every successful call


def track_rpc_error(provider_url: str):
    """Track failed RPC call"""
    _rpc_call_errors[provider_url] += 1


class TrackedWeb3:
    """Web3 wrapper that tracks all RPC calls for statistics"""
    def __init__(self, web3_instance: Web3, provider_url: str):
        self._web3 = web3_instance
        self._provider_url = provider_url
    
    def __getattr__(self, name):
        attr = getattr(self._web3, name)
        if name == 'eth':
            return TrackedEth(attr, self._provider_url)
        return attr
    
    def is_connected(self):
        return self._web3.is_connected()


class TrackedEth:
    """Wrapper for web3.eth that tracks all method calls"""
    def __init__(self, eth_module, provider_url: str):
        self._eth = eth_module
        self._provider_url = provider_url
    
    def __getattr__(self, name):
        attr = getattr(self._eth, name)
        # Track method calls (not properties like 'block_number')
        if callable(attr):
            def tracked_call(*args, **kwargs):
                start_time = time.time()
                try:
                    result = attr(*args, **kwargs)
                    response_time = time.time() - start_time
                    track_rpc_success(self._provider_url, response_time)
                    return result
                except Exception as e:
                    track_rpc_error(self._provider_url)
                    raise e
            return tracked_call
        else:
            # For properties like block_number, track the access
            try:
                start_time = time.time()
                result = attr
                response_time = time.time() - start_time
                track_rpc_success(self._provider_url, response_time)
                return result
            except Exception as e:
                track_rpc_error(self._provider_url)
                raise e


def get_rpc_stats() -> Dict:
    """Get global RPC statistics across all modules"""
    from config import get_chain_config
    
    chain_cfg = get_chain_config(ACTIVE_CHAIN)
    all_providers = chain_cfg.get("rpc", [])
    
    stats = []
    for url in all_providers:
        success = _rpc_call_success[url]
        errors = _rpc_call_errors[url]
        total = success + errors
        
        success_rate = (success / total * 100) if total > 0 else 0
        avg_response_time = sum(_rpc_response_times[url]) / len(_rpc_response_times[url]) if _rpc_response_times[url] else 0
        
        stats.append({
            'url': url,
            'provider': url.split('/')[2] if '/' in url else url[:30],
            'success': success,
            'errors': errors,
            'total': total,
            'success_rate': success_rate,
            'avg_response_time': avg_response_time
        })
    
    # Sort by total requests (descending), then by success rate (descending)
    stats.sort(key=lambda x: (-x['total'], -x['success_rate'], x['avg_response_time']))
    
    return {
        'stats': stats,
        'total_requests': sum(_rpc_call_success.values()) + sum(_rpc_call_errors.values()),
        'total_success': sum(_rpc_call_success.values()),
        'total_errors': sum(_rpc_call_errors.values()),
        'active_provider': _current_provider_url
    }


@dataclass
class ProviderState:
    """Track health metrics for a single RPC provider."""

    url: str
    error_count: int = 0
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None

    def mark_success(self):
        self.last_success = datetime.utcnow()
        self.last_error = None

    def mark_failure(self, err: str):
        self.error_count += 1
        self.last_error = err


class ProviderManager:
    """Round-robin RPC manager with health tracking and retries."""

    def __init__(self, chain_name: Optional[str] = None):
        self.chain_name = chain_name or ACTIVE_CHAIN
        chain_cfg = get_chain_config(self.chain_name)
        rpc_urls = chain_cfg.get("rpc", [])
        self.providers: List[ProviderState] = [ProviderState(url) for url in rpc_urls]
        self._last_index: int = -1
        # Expected chain id for this manager (used to validate RPC endpoints)
        self.expected_chain_id = chain_cfg.get("chain_id")
        self._sticky: Optional[Tuple[int, Web3]] = None

    def _provider_order(self) -> List[int]:
        if not self.providers:
            return []
        indices = list(range(len(self.providers)))
        start = (self._last_index + 1) % len(indices)
        rotated = indices[start:] + indices[:start]
        # Prefer providers with the fewest errors while keeping rotation order stable
        return sorted(rotated, key=lambda idx: (self.providers[idx].error_count, rotated.index(idx)))

    def _log_status(self):
        status = [
            f"{p.url} (errors={p.error_count}, last_success={p.last_success}, last_error={p.last_error})"
            for p in self.providers
        ]
        logger.info("Provider status [%s]: %s", self.chain_name, "; ".join(status))

    def get_web3(self, base_timeout: int = 10, force_new: bool = False, sticky: bool = False) -> Optional[Web3]:
        global _current_provider_url
        
        if sticky and not force_new and self._sticky and self._sticky[1].is_connected():
            return self._sticky[1]

        if not self.providers:
            logger.error("No RPC providers configured for chain %s", self.chain_name)
            return None

        attempt = 0
        for idx in self._provider_order():
            attempt += 1
            timeout = base_timeout * attempt
            provider = self.providers[idx]
            logger.info(
                "Connecting to provider %s (chain=%s, timeout=%ss, errors=%s)",
                provider.url,
                self.chain_name,
                timeout,
                provider.error_count,
            )
            try:
                start_time = time.time()
                w3 = Web3(Web3.HTTPProvider(provider.url, request_kwargs={"timeout": timeout}))
                if w3.is_connected():
                    # Verify provider is serving the expected chain id (avoid cross-chain providers)
                    try:
                        prov_chain = getattr(w3.eth, 'chain_id', None)
                    except Exception:
                        prov_chain = None
                    if self.expected_chain_id and prov_chain != self.expected_chain_id:
                        provider.mark_failure(f"wrong chain (reported {prov_chain})")
                        track_rpc_error(provider.url)
                        logger.warning("Provider %s reports chain %s, expected %s -> skipping", provider.url, prov_chain, self.expected_chain_id)
                        continue
                    
                    response_time = time.time() - start_time
                    provider.mark_success()
                    track_rpc_success(provider.url, response_time)
                    _current_provider_url = provider.url
                    self._last_index = idx
                    # Wrap in TrackedWeb3 for automatic RPC call tracking
                    tracked_w3 = TrackedWeb3(w3, provider.url)
                    if sticky:
                        self._sticky = (idx, tracked_w3)
                    self._log_status()
                    return tracked_w3
                provider.mark_failure("connection check failed")
                track_rpc_error(provider.url)
            except requests.exceptions.RequestException as exc:
                provider.mark_failure(str(exc))
                track_rpc_error(provider.url)
                logger.warning("Network error on provider %s: %s", provider.url, exc)
            except Exception as exc:
                provider.mark_failure(str(exc))
                track_rpc_error(provider.url)
                logger.debug("Provider %s failed with %s", provider.url, exc)

        logger.error("All RPC providers failed for chain %s", self.chain_name)
        self._log_status()
        return None


_provider_managers: Dict[str, ProviderManager] = {}


def get_web3(
    timeout: int = 10,
    force_new: bool = False,
    chain_name: Optional[str] = None,
    sticky: bool = False,
) -> Optional[Web3]:
    """
    Get a Web3 instance using round-robin provider selection.

    Args:
        timeout: Base request timeout in seconds (increases per retry)
        force_new: Ignore sticky cache and force new connection
        chain_name: Chain identifier defined in config.CHAINS
        sticky: Reuse last healthy provider for subsequent calls
    """

    chain_key = chain_name or ACTIVE_CHAIN
    manager = _provider_managers.setdefault(chain_key, ProviderManager(chain_key))
    return manager.get_web3(base_timeout=timeout, force_new=force_new, sticky=sticky)

def get_logs_chunked(
    w3: Web3,
    address: str,
    topics: List,
    from_block: int,
    to_block: int,
    initial_chunk: int = 1000,
    min_chunk: int = 64
) -> List:
    """
    Fetch event logs with automatic chunk size reduction on errors
    
    Handles "query exceeds max results" errors by reducing chunk size
    Tracks RPC performance for all calls
    
    Args:
        w3: Web3 instance
        address: Contract address
        topics: Event topics filter
        from_block: Starting block number
        to_block: Ending block number
        initial_chunk: Initial chunk size
        min_chunk: Minimum chunk size before giving up
    
    Returns:
        List of event logs
    """
    global _current_provider_url
    logs = []
    chunk = initial_chunk
    to_blk = to_block
    
    while to_blk >= from_block:
        frm = max(from_block, to_blk - chunk + 1)
        
        try:
            start_time = time.time()
            part = w3.eth.get_logs({
                "fromBlock": frm,
                "toBlock": to_blk,
                "address": address,
                "topics": topics
            })
            
            # Track successful call
            if _current_provider_url:
                response_time = time.time() - start_time
                track_rpc_success(_current_provider_url, response_time)
            
            if part:
                logs.extend(part)
            
            to_blk = frm - 1
            
        except Exception as e:
            # Track failed call
            if _current_provider_url:
                track_rpc_error(_current_provider_url)
            
            msg = str(e).lower()
            
            # Reduce chunk size if too many results
            if "range" in msg or "exceeds" in msg or "too large" in msg:
                chunk = max(min_chunk, chunk // 2)
                logger.debug(f"Reduced chunk size to {chunk}")
            else:
                # Other error - skip this range
                logger.warning(f"Error fetching logs [{frm}-{to_blk}]: {e}")
                to_blk = frm - 1
    
    return logs
