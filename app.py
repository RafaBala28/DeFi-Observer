from flask import Flask, render_template, jsonify, request, Response, send_file
from uniswap_data import get_uniswap_data
from uniswap_v3_data import get_uniswap_v3_pools
from uniswap_extended import get_uniswap_extended
from aave_risk_monitor import get_aave_risk_snapshot
from eth_network_stats import get_eth_network_stats
from wallet_positions import get_wallet_positions, analyze_v3_position, analyze_wallet_positions
from eth_price_tracker import get_tracker
import time
import metrics_store
import eth_price_store
import csv
from functools import lru_cache, wraps
import logging
import threading
import traceback
import requests
import pandas as pd
import os
import socket
import platform
import json
import sys
from datetime import timedelta

# Import shared utilities
from web3_utils import get_web3
from chainlink_price_utils import ChainlinkPriceFetcher, normalize_symbol
from web3 import Web3
from master_csv_manager import ensure_master_csv_exists, refresh_master_csv, MASTER_CSV_PATH
from config import ACTIVE_CHAIN

# Terminal colors (module-level for reuse)
MAGENTA = '\x1b[35m'
BLUE = '\x1b[34m'
GREEN = '\x1b[32m'
RESET = '\x1b[0m'

app = Flask(__name__)
app.config['COMPRESS_REGISTER'] = False  # Manual compression control

# Track server start time for uptime calculation
SERVER_START_TIME = time.time()


@app.route('/debug/rpc')
def debug_rpc():
    """Return current RPC provider error counters from the scanner module.
    This is useful for live inspection without attaching to the scanner process.
    """
    try:
        import importlib, sys
        tools_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'tools')
        if tools_path not in sys.path:
            sys.path.insert(0, tools_path)
        # Import the scanner module and read provider_errors
        scanner = importlib.import_module('tools.aave_v3_liquidations_scanner')
        # Return a simple dict copy so Flask/jsonify can serialize it
        return jsonify({'provider_errors': scanner.provider_errors})
    except Exception as e:
        logger.exception('Failed to read provider_errors')
        return jsonify({'error': str(e)}), 500

# Logging Setup - Einheitliches, farbiges Console-Format
class _ColorFormatter(logging.Formatter):
    """Simple color formatter for console logs."""
    COLORS = {
        'DEBUG': '\x1b[90m',   # dim gray
        'INFO': '\x1b[37m',    # white (not blue)
        'WARNING': '\x1b[33m', # yellow
        'ERROR': '\x1b[31m',   # red
        'CRITICAL': '\x1b[41m' # red background
    }
    RESET = '\x1b[0m'

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, '')
        formatted = super().format(record)
        return f"{color}{formatted}{self.RESET}"


def setup_logging(level=logging.INFO):
    root = logging.getLogger()
    if root.handlers:
        return  # already configured

    fmt = '%(asctime)s %(levelname)-7s [%(name)s] %(message)s'
    handler = logging.StreamHandler()
    handler.setFormatter(_ColorFormatter(fmt, datefmt='%H:%M:%S'))

    root.setLevel(level)
    root.addHandler(handler)

    # Module specific defaults to reduce noise
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('web3_utils').setLevel(logging.WARNING)
    logging.getLogger('aave_scanner').setLevel(logging.INFO)


setup_logging()
logger = logging.getLogger(__name__)

# --- Banner updater: print banner + current ETH price at the top of terminal ---
def _get_latest_eth_price():
    return _get_latest_eth_price_impl(force_refresh=False)


def _get_latest_eth_price_impl(force_refresh=False):
    try:
        # If caller requested a forced refresh prefer the tracker
        if force_refresh:
            tracker = globals().get('eth_tracker')
            if tracker:
                try:
                    price, source = tracker.get_current_price(force_refresh=True)
                    return price, source
                except Exception:
                    pass

        # Try to read from eth_price_store first (fast, cached)
        prices = eth_price_store.get_prices(hours=1)
        if prices:
            latest = prices[-1]
            return latest.get('price'), latest.get('source')
    except Exception:
        pass
    # Fallback: attempt a quick fetch via tracker if available
    try:
        tracker = globals().get('eth_tracker')
        if tracker:
            price, source = tracker.get_current_price(force_refresh=False)
            return price, source
    except Exception:
        pass
    return None, None


def _get_scan_status():
    """Read scanner status from data/scan_status.json if available."""
    try:
        status_fn = os.path.join('data', 'scan_status.json')
        if os.path.exists(status_fn):
            with open(status_fn, 'r', encoding='utf-8') as sf:
                return json.load(sf)
    except Exception:
        pass
    return None


def _print_banner_with_price(full=False):
    """Update the small banner area at the top of the terminal.
    - If full=True: print the full ASCII art banner (used once at startup).
    - If full=False: perform a non-destructive top-area overwrite using
      cursor save/restore so previous logs are preserved.
    """
    MAGENTA = '\x1b[35m'
    BLUE = '\x1b[34m'
    GREEN = '\x1b[32m'
    RESET = '\x1b[0m'

    # When full is requested, print the full logo once (no cursor magic)
    if full:
        print(MAGENTA + "  ____        _____ _    ___  _                                " + RESET)
        print(MAGENTA + r" |  _ \  ___ |  ___(_)  / _ \| |__  ___  ___ _ ____   _____ _ __ " + RESET)
        print(MAGENTA + r" | | | |/ _ \| |_  | | | | | | '_ \/ __|/ _ \ '__\ \ / / _ \ '__|" + RESET)
        print(MAGENTA + r" | |_| |  __/|  _| | | | |_| | |_) \__ \  __/ |   \ V /  __/ |   " + RESET)
        print(MAGENTA + r" |____/ \___||_|   |_|  \___/|_.__/|___/\___|_|    \_/ \___|_|   " + RESET)
        print("")
        print(BLUE + "  Version 2.0 - AAVE V3 Mainnet Liquidation Monitor" + RESET)
        return
    # For non-full updates we intentionally avoid re-printing the full ASCII
    # art to prevent terminal duplication issues. Instead a lightweight
    # periodic status logger is used (see _start_banner_updater()).
    price, source = _get_latest_eth_price()
    scan_status = _get_scan_status()
    try:
        st = scan_status.get('status', 'unknown') if scan_status else 'unknown'
        events = scan_status.get('events_found', 0) if scan_status else 0
        from_b = scan_status.get('from_block') if scan_status else None
        to_b = scan_status.get('to_block') if scan_status else None
    except Exception:
        st, events, from_b, to_b = 'unknown', 0, None, None

    # Compose a compact one-line status for fallback printing (rarely used)
    if price:
        one_line = f"Version 2.0 | ETH ${price:,.2f} (src:{source}) | Last scan: status={st} events={events} range={from_b}-{to_b}"
    else:
        one_line = f"Version 2.0 | ETH:(unavailable) | Last scan: status={st} events={events} range={from_b}-{to_b}"

    # Print a single non-intrusive line so terminals that don't support cursor
    # control still show a compact status. This avoids reprinting the logo.
    print(one_line)


def _start_banner_updater(interval_seconds=30):
    def run():
        try:
            # Periodically update the banner area (either inline or via compact log)
            # Cursor-based inline updates are enabled by default to keep the
            # logo static and refresh only the ETH/scan lines.
            # Default: disable cursor-mode on Windows/PowerShell where ANSI
            # cursor positioning can behave inconsistently. Allow override
            # via env `TERMINAL_BANNER_CURSOR=1`.
            if os.environ.get('TERMINAL_BANNER_CURSOR') is not None:
                cursor_mode = os.environ.get('TERMINAL_BANNER_CURSOR', '1') not in ('0', 'false', 'no')
            else:
                cursor_mode = not platform.system().lower().startswith('win')
            while True:
                try:
                    # Use cached/latest stored price for banner to avoid
                    # triggering repeated Chainlink fetches and duplicate logs.
                    price, source = _get_latest_eth_price()
                    scan_status = _get_scan_status() or {}
                    st = scan_status.get('status', 'unknown')
                    events = scan_status.get('events_found', 0)
                    from_b = scan_status.get('from_block')
                    to_b = scan_status.get('to_block')

                    if cursor_mode:
                        # Inline update: overwrite only the ETH + scan lines at the
                        # top of the terminal to keep logs intact below.
                        try:
                            # Save cursor, move to fixed banner lines, clear them and write
                            # Line numbers chosen to match printed logo area from __main__
                            # (row 8 = ETH line, row 9 = scan status)
                            print('\x1b[s', end='')
                            # ETH line
                            print('\x1b[8;1H', end='')
                            print('\x1b[2K', end='')
                            if price:
                                print(GREEN + f"  ETH: ${price:,.2f}  (source: {source})" + RESET)
                            else:
                                print(GREEN + "  ETH: (unavailable)" + RESET)
                            # Scan status line
                            print('\x1b[9;1H', end='')
                            print('\x1b[2K', end='')
                            if scan_status:
                                try:
                                    last_up = scan_status.get('last_updated', 0)
                                    updated_str = ''
                                    if last_up:
                                        try:
                                            updated_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_up))
                                        except Exception:
                                            updated_str = str(last_up)
                                    print(BLUE + f"  Last scan: status={st} events_total={events} range={from_b}-{to_b} updated={updated_str}" + RESET)
                                except Exception:
                                    print(BLUE + f"  Last scan: status={st} events_total={events}" + RESET)
                            else:
                                print(BLUE + "  Last scan: status=unknown" + RESET)
                            # Restore cursor
                            print('\x1b[u', end='')
                        except Exception:
                            # On terminals without ANSI support, fallback to compact log
                            cursor_mode = False
                    if not cursor_mode:
                        if price:
                            logger.info(f"[Banner] ETH ${price:,.2f} (src:{source}) | scan={st} events={events} range={from_b}-{to_b}")
                        else:
                            logger.info(f"[Banner] ETH:(unavailable) | scan={st} events={events} range={from_b}-{to_b}")
                except Exception:
                    logger.debug("[Banner] status update failed", exc_info=False)
                time.sleep(interval_seconds)
        except Exception:
            pass

    t = threading.Thread(target=run, daemon=True)
    t.start()

# Initialisiere Web3 using shared utility
DEFAULT_CHAIN = ACTIVE_CHAIN
w3 = get_web3(timeout=10, chain_name=DEFAULT_CHAIN, sticky=True)
if not w3:
    logger.error("Failed to initialize Web3 connection")

# Stelle sicher, dass die Master-CSV bereits vorhanden ist
ensure_master_csv_exists()

# Ensure scan_status.json is derived from canonical master CSV at startup
try:
    from tools.sync_scan_status_from_csv import main as sync_scan_status_from_csv
    try:
        sync_scan_status_from_csv()
        logger.debug("[Startup] scan_status.json synced from master CSV")
    except Exception as e:
        logger.warning(f"[Startup] Failed to sync scan_status from CSV: {e}")
except Exception:
    logger.debug("[Startup] sync helper not available", exc_info=False)

# ============================================================================
# CSV-basierte Liquidations-Funktionen (ersetzt liquidations_store.py)
# ============================================================================
def get_liquidations_from_csv(hours=None, limit=None):
    """
    Lese Liquidationen direkt aus der Master-CSV.
    
    Args:
        hours: Nur Liquidationen der letzten X Stunden (None = alle)
        limit: Maximale Anzahl zurückgeben (None = alle)
    
    Returns:
        Liste von Liquidation-Dicts
    """
    if not os.path.exists(MASTER_CSV_PATH):
        return []
    
    # Helper for parsing timestamps (handles both unix and datetime strings)
    def parse_ts(val):
        if not val:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        val_str = str(val).strip()
        if val_str.replace('.', '').isdigit():
            return int(float(val_str))
        # Try datetime format
        try:
            from datetime import datetime
            return int(datetime.strptime(val_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
        except:
            return 0
    
    try:
        with open(MASTER_CSV_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        logger.warning(f"CSV Lesefehler: {e}")
        return []
    
    # Konvertiere zu standardisiertem Format
    liquidations = []
    cutoff_time = int(time.time()) - (hours * 3600) if hours else 0
    
    for row in rows:
        try:
            ts = parse_ts(row.get('timestamp', 0))
            if hours and ts < cutoff_time:
                continue
            
            liquidations.append({
                'block': int(row.get('block', 0)),
                'timestamp': ts,
                'time': ts,  # Alias für Kompatibilität
                'tx': row.get('tx', ''),
                'hash': row.get('tx', ''),  # Alias
                'collateralAsset': row.get('collateralAsset', ''),
                'debtAsset': row.get('debtAsset', ''),
                'collateralSymbol': row.get('collateralSymbol', ''),
                'debtSymbol': row.get('debtSymbol', ''),
                'collateralOut': row.get('collateralOut', '0'),
                'debtToCover': row.get('debtToCover', '0'),
                'user': row.get('user', ''),
                'liquidator': row.get('liquidator', ''),
                'collateral_price_usd': float(row.get('collateral_price_usd_at_block', 0) or 0),
                'debt_price_usd': float(row.get('debt_price_usd_at_block', 0) or 0),
                'collateralAmountUSD': float(row.get('collateral_value_usd', 0) or 0),
                'debtAmountUSD': float(row.get('debt_value_usd', 0) or 0),
            })
        except Exception:
            continue
    
    # Sortiere nach Block (neueste zuerst)
    liquidations.sort(key=lambda x: x['block'], reverse=True)
    
    if limit:
        liquidations = liquidations[:limit]
    
    return liquidations

def fetch_recent_liquidations_from_csv(limit=10, since_timestamp=None):
    """
    Hole aktuelle Liquidationen aus CSV.
    Ersetzt aave_liquidations.fetch_recent_liquidations()
    """
    all_liqs = get_liquidations_from_csv(hours=None, limit=None)

    # Dedupe by tx to ensure frontend never shows duplicate transactions
    seen = set()
    deduped = []
    for l in all_liqs:
        tx = (l.get('tx') or '').lower()
        if not tx:
            deduped.append(l)
            continue
        if tx in seen:
            continue
        seen.add(tx)
        deduped.append(l)
    all_liqs = deduped

    if since_timestamp:
        all_liqs = [l for l in all_liqs if l.get('timestamp', 0) >= since_timestamp]

    if limit and limit > 0:
        all_liqs = all_liqs[:limit]
    
    return {
        "items": all_liqs,
        "source": "csv",
        "errors": [],
        "total_stored": len(all_liqs)
    }

# ❌ DEAKTIVIERT - refresh_master_csv() überschreibt CSV beim Start!
# Scanner schreibt direkt in CSV, kein Refresh nötig

# ❌ DEAKTIVIERT - Timestamp Backfill nicht mehr nötig
# Scanner schreibt Timestamps direkt in CSV

def start_periodic_liquidations_update():
    """Runs periodic AAVE v3 liquidations scan every 60 seconds using the mainnet-only scanner.
    
    Uses daemon=False to ensure scanner continues running even during graceful shutdown.
    The scanner persists state to CSV after each scan, so restarts are safe.
    """
    import sys
    import os
    ROOT = os.path.abspath(os.path.dirname(__file__))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    
    # Import scanner module
    sys.path.insert(0, os.path.join(ROOT, 'tools'))
    from tools.aave_v3_liquidations_scanner import main as scan_aave_v3
    
    def run():
        # First run: immediately scan to fill any gaps
        try:
            logger.info("[Liquidations] Starting initial blockchain scan...")
            # Allow skipping the initial (potentially long) scan via env var
            skip_initial = os.environ.get('SKIP_INITIAL_SCAN') in ('1', 'true', 'True')
            if skip_initial:
                logger.info("[Liquidations] SKIP_INITIAL_SCAN set - skipping initial scan on startup")
            else:
                # Ensure we refresh ETH price before the scan so event enrichment
                # can rely on a fresh Chainlink value.
                try:
                    tracker = globals().get('eth_tracker')
                    if tracker:
                        p, s = tracker.get_current_price(force_refresh=True)
                        logger.debug(f"[Liquidations] Interval ETH: ${p:,.2f} ({s})")
                except Exception:
                    logger.debug("[Liquidations] ETH price pre-scan refresh failed", exc_info=False)

                scan_aave_v3(to_block="latest")
                logger.info("[Liquidations] Initial scan completed")
        except Exception as e:
            logger.error(f"[Liquidations] Initial scan failed: {e}\n{traceback.format_exc()}")
            logger.warning("[Liquidations] Scanner will retry in next periodic cycle...")
        
        # Then run periodically every 60 seconds - INFINITE LOOP
        scan_number = 1
        while True:
            try:
                time.sleep(60)
                logger.info(f"[Liquidations] Periodic scan #{scan_number} started")
                
                # Refresh ETH price at each interval before scanning
                try:
                    tracker = globals().get('eth_tracker')
                    if tracker:
                        p, s = tracker.get_current_price(force_refresh=True)
                        logger.debug(f"[Liquidations] Interval ETH: ${p:,.2f} ({s})")
                except Exception:
                    logger.debug("[Liquidations] ETH price interval refresh failed", exc_info=False)

                scan_aave_v3(to_block="latest")
                logger.info(f"[Liquidations] Periodic scan #{scan_number} completed successfully")
                scan_number += 1
                
                # ❌ DEAKTIVIERT - refresh_master_csv() überschreibt CSV!
                # try:
                #     refresh_master_csv()
                # except Exception as exc:
                #     logger.warning("Failed to refresh master CSV after scan: %s", exc)
            except KeyboardInterrupt:
                logger.info("[Liquidations] Scanner stopped by user interrupt")
                break
            except Exception as e:
                logger.error(f"[Liquidations] Error in periodic scan #{scan_number}: {e}\n{traceback.format_exc()}")
                logger.warning("[Liquidations] Scanner will retry after sleep interval...")
                scan_number += 1
                # Continue loop - don't break on errors
    
    # CRITICAL: daemon=False ensures scanner survives app restarts
    # The scanner thread will keep running even if Flask is reloading
    thread = threading.Thread(target=run, daemon=False, name="LiquidationsScanner")
    thread.start()
    logger.info("[Liquidations] Scanner thread started (NON-DAEMON mode - continuous operation)")

def start_eth_price_dataset_updater():
    """Runs daily ETH price dataset update at 00:05 UTC.
    
    Updates the daily ETH/USD price dataset incrementally.
    Same pattern as liquidation scanner.
    """
    import sys
    import os
    from datetime import datetime, timezone, time as dt_time
    
    ROOT = os.path.abspath(os.path.dirname(__file__))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    
    def run():
        from scripts.build_daily_eth_chainlink_dataset import main as update_eth_dataset
        
        # Initial update on startup (skip if dataset is current)
        try:
            logger.info("[ETH Dataset] Checking for updates...")
            update_eth_dataset(incremental=True)
            logger.info("[ETH Dataset] Startup check completed")
        except Exception as e:
            logger.error(f"[ETH Dataset] Startup update failed: {e}")
            logger.warning("[ETH Dataset] Will retry at next scheduled time...")
        
        # Then run daily at 00:05 UTC
        while True:
            try:
                now_utc = datetime.now(timezone.utc)
                # Calculate next 00:05 UTC
                next_run = now_utc.replace(hour=0, minute=5, second=0, microsecond=0)
                if now_utc >= next_run:
                    # Already past today's 00:05, schedule for tomorrow
                    next_run += timedelta(days=1)
                
                sleep_seconds = (next_run - now_utc).total_seconds()
                logger.info(f"[ETH Dataset] Next update at {next_run.strftime('%Y-%m-%d %H:%M:%S UTC')} (in {sleep_seconds/3600:.1f}h)")
                
                time.sleep(sleep_seconds)
                
                # Run update
                logger.info("[ETH Dataset] Starting daily update...")
                update_eth_dataset(incremental=True)
                logger.info("[ETH Dataset] Daily update completed")
                
            except KeyboardInterrupt:
                logger.info("[ETH Dataset] Updater stopped by user interrupt")
                break
            except Exception as e:
                logger.error(f"[ETH Dataset] Error in daily update: {e}")
                logger.warning("[ETH Dataset] Will retry in 1 hour...")
                time.sleep(3600)  # Retry in 1 hour on error
    
    thread = threading.Thread(target=run, daemon=True, name="ETHPriceDatasetUpdater")
    thread.start()
    logger.info("[ETH Dataset] Updater thread started (daily updates at 00:05 UTC)")

# Global tracker variable
eth_tracker = None
_liquidations_started = False

def _init_background_services():
    """Initialize background services - called from main block after logo"""
    global eth_tracker, _liquidations_started
    
    if _liquidations_started:
        return
    _liquidations_started = True
    
    if w3 and w3.is_connected():
        eth_tracker = get_tracker(w3)
        if eth_tracker:
            # Start periodic liquidations scan
            start_periodic_liquidations_update()
            
            # Start daily ETH price dataset updater
            start_eth_price_dataset_updater()
            
            # Backfill ETH prices for chart (7 days in background)
            def _backfill_eth_prices_bg():
                try:
                    import time as _time
                    _time.sleep(3)  # Wait for server to start
                    existing = eth_price_store.get_prices(hours=168)
                    if len(existing) < 100:
                        logger.debug("[USD Prices] Loading 7-day ETH price history...")
                        eth_tracker.backfill_history_from_chainlink(hours=168)
                except Exception as e:
                    logger.warning(f"[USD Prices] ETH backfill failed: {e}")
            
            tb_eth = threading.Thread(target=_backfill_eth_prices_bg, daemon=True)
            tb_eth.start()
    else:
        logger.warning("No Web3 provider - ETH Price Tracker not available")

# Cache-Hilfsfunktion: Rundet Timestamp auf 30 Sekunden
def _cache_key_30s():
    return int(time.time() // 30) * 30

# Cache-Hilfsfunktion: Rundet Timestamp auf 60 Sekunden (für Chart-Daten)
def _cache_key_60s():
    return int(time.time() // 60) * 60

# Decorator für Chart-Response mit Caching-Headers
def cache_chart_response(max_age=60):
    """Fügt Cache-Control und ETag Headers für Chart-Endpoints hinzu"""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            response = f(*args, **kwargs)
            if isinstance(response, tuple):
                resp, status = response
            else:
                resp = response
                status = 200
            
            # Füge Cache-Headers hinzu
            if hasattr(resp, 'headers'):
                resp.headers['Cache-Control'] = f'public, max-age={max_age}'
                resp.headers['Vary'] = 'Accept-Encoding'
            
            if isinstance(response, tuple):
                return resp, status
            return resp
        return wrapped
    return decorator

# Gecachte Daten-Funktionen (30s TTL)
def _selected_chain():
    return request.args.get("chain") or DEFAULT_CHAIN

@lru_cache(maxsize=32)
def _cached_uniswap(cache_key):
    return get_uniswap_data()

# 🔧 AAVE CACHE: 5 Minuten TTL statt 30s für bessere Performance
def _cache_key_5min():
    """Cache-Key der alle 5 Minuten wechselt"""
    return int(time.time() // 300)

@lru_cache(maxsize=8)
def _cached_aave(chain_name, cache_key):
    """Cache Aave Reserves für 5 Minuten"""
    from aave_data import get_aave_data
    return get_aave_data(chain_name=chain_name)

@lru_cache(maxsize=32)
def _cached_uniswap_v3(chain_name, cache_key):
    return get_uniswap_v3_pools(chain_name=chain_name)

@lru_cache(maxsize=32)
def _cached_eth_network(cache_key):
    return get_eth_network_stats()

@lru_cache(maxsize=128)
def _cached_liquidations(blocks, limit, forced_only, cache_key):
    return fetch_recent_liquidations(blocks_back=blocks, limit=limit, forced_only=bool(forced_only))

@app.route('/')
def index():
    """Hauptseite mit Dashboard"""
    # Cache-Busting für statische Assets (Frontend erhält frische app.js)
    response = render_template('index.html', build_ts=int(time.time()))
    return response

@app.after_request
def add_no_cache_headers(response):
    """Disable caching for development"""
    if request.path.startswith('/static/'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

@app.route('/api/uniswap')
def api_uniswap():
    """API Endpoint für Uniswap Daten"""
    data = _cached_uniswap(_cache_key_30s())
    # In History speichern, falls valide
    try:
        if isinstance(data, dict) and not data.get("error"):
            point = {
                "t": time.time(),
                "tvl_usd": float(data.get("tvl_usd", 0) or 0),
                "eth_price": float(data.get("eth_price", 0) or 0),
                "eth_reserve": float(data.get("eth_reserve", 0) or 0),
                "usdc_reserve": float(data.get("usdc_reserve", 0) or 0),
            }
            metrics_store.append_uniswap_point(point)
    except Exception as _:
        pass
    return jsonify(data)


@app.route('/data/scan_status.json')
def data_scan_status():
    """Serve the scan status JSON created by the scanner tools for the frontend."""
    import os
    path = os.path.join(os.getcwd(), 'data', 'scan_status.json')
    if os.path.exists(path):
        try:
            return send_file(path, mimetype='application/json')
        except Exception:
            return jsonify({'error': 'failed_to_send'}), 500
    return jsonify({'error': 'not_found'}), 404

@app.route('/api/aave')
def api_aave():
    """API Endpoint für Aave Daten"""
    chain_name = _selected_chain()
    data = _cached_aave(chain_name, _cache_key_30s())
    # In History speichern, falls valide
    try:
        if isinstance(data, dict) and not data.get("error") and isinstance(data.get("assets"), list):
            assets_slim = []
            for a in data.get("assets", []):
                try:
                    assets_slim.append({
                        "symbol": a.get("name"),
                        "deposit_apy": float(a.get("deposit_apy", 0) or 0),
                        "borrow_apy": float(a.get("borrow_apy", 0) or 0),
                        "utilization": float(a.get("utilization_rate", 0) or 0),
                        "liquidity_usd": float(a.get("liquidity_usd", 0) or 0),
                        "borrowed_usd": float(a.get("borrowed_usd", 0) or 0),
                        "price_usd": float(a.get("price_usd", 0) or 0),
                    })
                except Exception:
                    continue
            snapshot = {"t": time.time(), "assets": assets_slim}
            metrics_store.append_aave_snapshot(snapshot)
    except Exception as _:
        pass
    return jsonify(data)

@app.route('/api/uniswap_v3')
def api_uniswap_v3():
    """API Endpoint für Uniswap V3 Pools"""
    chain_name = _selected_chain()
    data = _cached_uniswap_v3(chain_name, _cache_key_30s())
    return jsonify(data)

@app.route('/api/uniswap/extended')
def api_uniswap_extended():
    """Erweiterte Uniswap V3 Metriken (24h Volumen, Ticks, On-Chain Price)"""
    data = get_uniswap_extended()
    return jsonify(data)

@app.route('/api/aave/risk')
def api_aave_risk():
    """Aave V3 Risk Monitor (LTV, Liquidation Threshold/Bonus, Utilization)"""
    data = get_aave_risk_snapshot()
    return jsonify(data)

@app.route('/api/eth/network')
def api_eth_network():
    """Ethereum Netzwerk Metriken (avg block time, gas, base fee)"""
    data = _cached_eth_network(_cache_key_30s())
    return jsonify(data)

@app.route('/api/aave/liquidations/recent')
def api_aave_liquidations_recent():
    """Letzte Liquidationen direkt aus CSV lesen."""
    import csv
    from datetime import datetime, timezone
    
    try:
        limit_param = request.args.get('limit', '100')
        limit = int(limit_param) if limit_param else 100
        if limit == 0:
            limit = None
    except Exception:
        limit = 100

    try:
        hours_param = request.args.get('hours')
        hours = int(hours_param) if hours_param else None
    except Exception:
        hours = None

    csv_path = MASTER_CSV_PATH
    items = []
    
    try:
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
                reader = csv.DictReader(f)  # Standard comma delimiter
                all_rows = list(reader)
                
                # Helper to safely parse timestamp (handle both unix and datetime strings)
                def safe_ts(row):
                    ts = row.get('timestamp', '0')
                    try:
                        return int(ts)
                    except:
                        # Maybe timestamp and datetime_utc are swapped
                        dt = row.get('datetime_utc', '0')
                        try:
                            return int(dt)
                        except:
                            return 0
                
                # Filtere nach Stunden falls angegeben
                if hours:
                    cutoff_time = int(datetime.now(timezone.utc).timestamp()) - (hours * 3600)
                    filtered_rows = [r for r in all_rows if safe_ts(r) >= cutoff_time]
                else:
                    filtered_rows = all_rows
                
                # Sortiere nach Timestamp absteigend (neueste zuerst)
                sorted_rows = sorted(filtered_rows, key=lambda x: safe_ts(x), reverse=True)
                
                # Limitiere Anzahl
                limited_rows = sorted_rows[:limit] if limit else sorted_rows
                
                # Konvertiere zu Frontend-Format
                for row in limited_rows:
                    # Sicher konvertieren und fehlende Werte abfangen
                    def _f(v):
                        try:
                            return float(v)
                        except Exception:
                            return 0.0
                    
                    def _i(v):
                        try:
                            return int(v)
                        except Exception:
                            return 0
                    
                    items.append({
                        'block': _i(row.get('block', 0)),
                        'time': safe_ts(row),
                        'tx': row.get('tx', ''),
                        'user': row.get('user', ''),
                        'liquidator': row.get('liquidator', ''),
                        'collateralAsset': row.get('collateralAsset', ''),
                        'debtAsset': row.get('debtAsset', ''),
                        'collateralSymbol': row.get('collateralSymbol', ''),
                        'debtSymbol': row.get('debtSymbol', ''),
                        'collateralOut': _f(row.get('collateralOut', 0)),
                        'debtToCover': _f(row.get('debtToCover', 0)),
                        'receiveAToken': str(row.get('receiveAToken', 'False')).lower() in ('true', '1', 'yes'),
                        # Preis-/USD-Felder
                        'collateral_price_usd_at_block': _f(row.get('collateral_price_usd_at_block', 0)),
                        'debt_price_usd_at_block': _f(row.get('debt_price_usd_at_block', 0)),
                        'collateral_value_usd': _f(row.get('collateral_value_usd', 0)),
                        'debt_value_usd': _f(row.get('debt_value_usd', 0)),
                        # Gas & Block Builder
                        'block_builder': row.get('block_builder', ''),
                        'gas_used': _i(row.get('gas_used', 0)),
                        'gas_price_gwei': _f(row.get('gas_price_gwei', 0))
                    })
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")

    stats = {
        'total_count': len(items),
        'last_block': items[0]['block'] if items else 0
    }

    return jsonify({
        "items": items,
        "count": len(items),
        "scan_info": {"triggered": False},
        "stats": stats,
        "source": "csv",
        "timestamp": int(time.time())
    })

@app.route('/api/aave/liquidations/scan', methods=['GET', 'POST'])
def api_aave_liquidations_scan():
    """Manueller Scan-Trigger - verwendet den robusten V3 Scanner"""
    try:
        from tools.aave_v3_liquidations_scanner import main as scan_aave_v3
        scan_aave_v3(to_block="latest")
        
        # Zähle aktuelle Events in CSV
        liqs = get_liquidations_from_csv(hours=24)
        return jsonify({
            'success': True,
            'message': 'Scan completed',
            'events_24h': len(liqs),
            'source': 'aave_v3_scanner'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/aave/liquidations/backfill_prices', methods=['POST', 'GET'])
def api_aave_liquidations_backfill():
    """Admin endpoint: Nicht mehr nötig - Scanner holt Preise automatisch."""
    return jsonify({
        'success': True, 
        'message': 'Backfill no longer needed - Scanner fetches Chainlink prices automatically',
        'info': 'Der V3 Scanner enriched alle Events direkt mit Chainlink-Preisen'
    })

@app.route('/api/aave/liquidations/export')
def api_aave_liquidations_export():
    """
    Exportiere Liquidationen als CSV direkt aus liquidations_master.csv
    """
    import csv
    from io import StringIO
    from datetime import datetime
    
    csv_path = os.path.join('data', 'liquidations_master.csv')
    
    if not os.path.exists(csv_path):
        return jsonify({"error": "No liquidations data available"}), 404
    
    try:
        # Lese CSV direkt mit csv module (nicht pandas - zu langsam/kaputt)
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            items = list(reader)
        
        logger.info(f"[EXPORT] Loaded {len(items)} items from CSV")
        
        # Optional: Limit anwenden
        try:
            limit_param = request.args.get('limit', '')
            limit = int(limit_param) if limit_param else 0
            if limit > 0:
                items = items[-limit:]  # Neueste zuerst
        except:
            pass
        
        # Timestamp für Dateiname
        timestamp_param = request.args.get('timestamp', '')
        if timestamp_param:
            filename_timestamp = timestamp_param
        else:
            filename_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        filename = f"aave_v3_liquidations_{filename_timestamp}.csv"
        
        # CSV generieren
        output = StringIO()
        if len(items) > 0:
            writer = csv.DictWriter(output, fieldnames=items[0].keys())
            writer.writeheader()
            writer.writerows(items)
        
        # Response mit CSV
        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        return response
        
    except Exception as e:
        logger.error(f"[EXPORT] Failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/rpc_stats')
def api_rpc_stats():
    """Get RPC provider performance statistics"""
    try:
        # Use centralized stats from web3_utils (tracks ALL modules)
        import web3_utils
        stats = web3_utils.get_rpc_stats()
        
        if not stats or stats['total_requests'] == 0:
            return jsonify({
                "status": "no_data",
                "message": "No RPC statistics available yet. System needs to make RPC calls first.",
                "stats": []
            })
        
        # Calculate uptime
        uptime_seconds = int(time.time() - SERVER_START_TIME)
        
        return jsonify({
            "status": "success",
            "timestamp": time.time(),
            "uptime_seconds": uptime_seconds,
            "total_requests": stats['total_requests'],
            "total_success": stats['total_success'],
            "total_errors": stats['total_errors'],
            "providers": stats['stats'],
            "active_provider": stats['active_provider']
        })
    except Exception as e:
        logger.error(f"[RPC Stats] Failed: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/csv_status')
def api_csv_status():
    """Serve the CSV build status JSON so the frontend can poll progress."""
    import os, json, time
    try:
        # MASTER_CSV_PATH imported earlier points to the master CSV file
        status_path = os.path.join(os.path.dirname(MASTER_CSV_PATH) or '.', 'liquidations_master_status.json')
        if not os.path.exists(status_path):
            # return an empty status structure
            base = {
                'total_rows': 0,
                'filled_both': 0,
                'filled_collateral': 0,
                'filled_debt': 0,
                'percent_both': 0.0,
                'percent_collateral': 0.0,
                'percent_debt': 0.0,
                'last_updated': int(time.time())
            }
            # add lightweight master CSV metadata so the FE can decide to reload
            try:
                if os.path.exists(MASTER_CSV_PATH):
                    st = os.stat(MASTER_CSV_PATH)
                    base['master_size_bytes'] = st.st_size
                    base['master_last_modified'] = int(st.st_mtime)
                    # count lines without loading whole file
                    cnt = 0
                    with open(MASTER_CSV_PATH, 'r', encoding='utf-8') as mf:
                        for _ in mf:
                            cnt += 1
                    base['master_rows'] = max(0, cnt - 1)  # subtract header
                    # check header presence (very small read)
                    with open(MASTER_CSV_PATH, 'r', encoding='utf-8') as mf:
                        first = mf.readline().strip()
                    base['header_present'] = bool(first and 'block' in first and 'timestamp' in first)
            except Exception:
                # don't fail status reporting for metadata read errors
                pass
            return jsonify(base)
        with open(status_path, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        # enrich status with master CSV metadata for FE sync decisions
        try:
            if os.path.exists(MASTER_CSV_PATH):
                st = os.stat(MASTER_CSV_PATH)
                data.setdefault('master_size_bytes', st.st_size)
                data.setdefault('master_last_modified', int(st.st_mtime))
                cnt = 0
                with open(MASTER_CSV_PATH, 'r', encoding='utf-8') as mf:
                    for _ in mf:
                        cnt += 1
                data.setdefault('master_rows', max(0, cnt - 1))
                with open(MASTER_CSV_PATH, 'r', encoding='utf-8') as mf:
                    first = mf.readline().strip()
                data.setdefault('header_present', bool(first and 'block' in first and 'timestamp' in first))
        except Exception:
            logger.exception('Failed to enrich csv status with master metadata')
        return jsonify(data)
    except Exception as e:
        logger.exception('Failed to read csv status: %s', e)
        return jsonify({'error': 'failed_to_read_status', 'message': str(e)}), 500

@app.route('/api/aave/liquidations/export_stats')
def api_aave_liquidations_export_stats():
    """
    Statistiken über Export-Qualität
    
    Zeigt Token-Mappings, Datenqualität und Zusammenfassung
    """
    from datetime import datetime, timezone
    
    # Token Address Mapping (gleich wie im Export)
    TOKEN_ADDRESS_MAP = {
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH',
        '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'WBTC',
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
        '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT',
        '0x514910771af9ca656af840dff83e8264ecf986ca': 'LINK',
        '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0': 'wstETH',
        '0xae78736cd615f374d3085123a210448e74fc6393': 'rETH',
        '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9': 'AAVE',
        '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984': 'UNI',
        '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
        '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 'cbBTC',
        '0xbe9895146f7af43049ca1c1ae358b0541ea49704': 'cbETH',
        '0xddc3d26baa9d2d979f5e2e42515478bf18f354d5': 'USDS',
        '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2': 'MKR',
        '0x83f20f44975d03b1b09e64809b757c47f942beea': 'sDAI',
        '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f': 'GHO',
        '0x6c3ea9036406852006290770bedfcaba0e23a0e8': 'PYUSD',
        '0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee': 'weETH',
        '0x4c9edd5852cd905f086c759e8383e09bff1e68b3': 'USDe',
        '0x9d39a5de30e57443bff2a8307a4256c8797a3497': 'sUSDe'
    }
    
    def map_token_symbol(address, current_symbol=''):
        """Mappe Token-Adresse zu Symbol"""
        if not address:
            return current_symbol or ''
        
        # Falls bereits Symbol vorhanden und gültig
        if current_symbol and not current_symbol.startswith('0x') and len(current_symbol) < 20:
            return current_symbol
        
        # Versuche Mapping
        address_lower = address.lower()
        if address_lower in TOKEN_ADDRESS_MAP:
            return TOKEN_ADDRESS_MAP[address_lower]
        
        # Falls Adresse: kürze sie
        if address.startswith('0x') and len(address) == 42:
            return f"{address[:6]}...{address[-4:]}"
        
        return address
    
    def shorten_address(addr):
        """Kürze Ethereum-Adresse"""
        if addr and addr.startswith('0x') and len(addr) == 42:
            return f"{addr[:6]}...{addr[-4:]}"
        return addr
    
    # Parse Parameter
    try:
        hours_param = request.args.get('hours', '')
        hours = int(hours_param) if hours_param else None
    except:
        hours = None
    
    try:
        limit_param = request.args.get('limit', '')
        limit = int(limit_param) if limit_param else 0
    except:
        limit = 0
    
    export_format = request.args.get('format', 'enhanced')

    # Enrichment: when false skip external RPC/Chainlink/block lookups to make export fast
    enrich_param = request.args.get('enrich', '0')
    enrich = str(enrich_param).lower() in ('1', 'true', 'yes')
    
    # Timestamp für Dateiname
    timestamp_param = request.args.get('timestamp', '')
    if timestamp_param:
        filename_timestamp = timestamp_param
    else:
        filename_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Hole Liquidationen direkt aus CSV (nicht aus Store)
    csv_path = os.path.join('data', 'liquidations_master.csv')
    
    if not os.path.exists(csv_path):
        return jsonify({"error": "No liquidations data available"}), 404
    
    try:
        df = pd.read_csv(csv_path)
        items = df.to_dict('records')
        logger.info(f"[EXPORT] Loaded {len(items)} items from CSV")
    except Exception as e:
        logger.error(f"[EXPORT] Failed to read CSV: {e}")
        return jsonify({"error": "Failed to read liquidations data"}), 500

    # Backfill/enrichment only when requested (enrich=True). Otherwise use stored fields as-is
    if enrich:
        # Backfill fehlender Timestamps — hole Blockzeit, falls time fehlt
        try:
            local_w3 = w3 if w3 and w3.is_connected() else get_web3(timeout=6, force_new=True, sticky=True)
        except Exception:
            local_w3 = None
        block_ts_cache = {}
        beacon_block_cache = {}
        validator_cache = {}
        # Sammle fehlende Blöcke
        missing_blocks = set()
        for item in items:
            if item.get('time'):
                continue
            block_num = item.get('block') or item.get('b')
            try:
                block_int = int(block_num)
            except Exception:
                continue
            missing_blocks.add(block_int)

        # Versuche Web3 zuerst
        if local_w3 and missing_blocks:
            for blk in list(missing_blocks):
                try:
                    blk_obj = local_w3.eth.get_block(blk)
                    block_ts_cache[blk] = blk_obj.get('timestamp')
                except Exception:
                    block_ts_cache[blk] = None

        # Fallback: Blockscout API, falls Web3 fehlgeschlagen oder nicht verbunden
        blockscout_url = "https://eth.blockscout.com/api"
        for blk in missing_blocks:
            if block_ts_cache.get(blk):
                continue
            try:
                resp = requests.get(
                    blockscout_url,
                    params={"module": "block", "action": "getblockreward", "blockno": blk},
                    timeout=6
                )
                if resp.status_code == 200:
                    data = resp.json().get("result", {})
                    ts = data.get("timeStamp")
                    if ts:
                        block_ts_cache[blk] = int(ts)
            except Exception:
                block_ts_cache[blk] = None
            time.sleep(0.02)  # sanftes Rate-Limit

        def get_validator_info(proposer_index):
            if proposer_index is None:
                return {}
            if proposer_index in validator_cache:
                return validator_cache[proposer_index]
            info = {'validator_address': None, 'validator_pubkey': None}
            try:
                resp = requests.get(f"https://beaconcha.in/api/v1/validator/{proposer_index}", timeout=6)
                if resp.status_code == 200:
                    data = resp.json().get('data', {})
                    info['validator_pubkey'] = data.get('pubkey')
                    creds = data.get('withdrawalcredentials') or ''
                    if isinstance(creds, str) and creds.startswith('0x') and len(creds) == 66:
                        address_hex = '0x' + creds[-40:]
                        try:
                            info['validator_address'] = Web3.to_checksum_address(address_hex)
                        except Exception:
                            info['validator_address'] = address_hex.lower()
            except Exception:
                pass
            validator_cache[proposer_index] = info
            return info

        def get_beacon_block_info(block_int):
            if block_int is None:
                return {}
            if block_int in beacon_block_cache:
                return beacon_block_cache[block_int]
            info = {'builder': None, 'proposer_index': None, 'validator_address': None}
            try:
                resp = requests.get(f"https://beaconcha.in/api/v1/execution/block/{block_int}", timeout=6)
                if resp.status_code == 200:
                    data = resp.json().get('data', [])
                    if isinstance(data, list) and data:
                        entry = data[0]
                        relay = entry.get('relay') or {}
                        builder = relay.get('producerFeeRecipient') or entry.get('feeRecipient')
                        proposer_index = entry.get('posConsensus', {}).get('proposerIndex')
                        info.update({
                            'builder': builder or entry.get('feeRecipient'),
                            'proposer_index': proposer_index,
                            'relay_tag': relay.get('tag'),
                            'builder_pubkey': relay.get('builderPubkey'),
                            'fee_recipient': entry.get('feeRecipient')
                        })
                        if proposer_index is not None:
                            val_info = get_validator_info(proposer_index)
                            info['validator_address'] = val_info.get('validator_address')
                            info['validator_pubkey'] = val_info.get('validator_pubkey')
            except Exception:
                pass
            beacon_block_cache[block_int] = info
            return info

        price_fetcher = None
        price_cache = {}
        if local_w3:
            try:
                price_fetcher = ChainlinkPriceFetcher(local_w3)
            except Exception:
                price_fetcher = None

        def get_chainlink_price(symbol, ts):
            if not price_fetcher or not symbol or not ts:
                return None
            key = (symbol, ts)
            if key in price_cache:
                return price_cache[key]
            try:
                price = price_fetcher.get_price_at_timestamp(symbol, ts)
            except Exception:
                price = None
            price_cache[key] = price
            return price
    else:
        # No enrichment: stubs to avoid external calls
        def get_beacon_block_info(_):
            return {}

        def get_chainlink_price(_, __):
            return None

    # Statistiken
    stats = {
        'total_rows': len(items),
        'mapped_collateral': 0,
        'mapped_debt': 0,
        'unmapped_addresses': set(),
        'token_symbols_found': set()
    }
    
    # Verarbeite und sortiere Daten
    processed_items = []
    for item in items:
        timestamp = item.get('time', 0)
        
        # ISO 8601 Datum und DateTime
        if timestamp:
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            date_iso = dt.strftime('%Y-%m-%d')
            datetime_iso = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            date_iso = ''
            datetime_iso = ''
        
        # Token-Mapping
        collateral_addr = item.get('collateralAsset', '')
        collateral_symbol_orig = item.get('collateralSymbol', '')
        collateral_symbol = map_token_symbol(collateral_addr, collateral_symbol_orig)
        
        debt_addr = item.get('debtAsset', '')
        debt_symbol_orig = item.get('debtSymbol', '')
        debt_symbol = map_token_symbol(debt_addr, debt_symbol_orig)

        collateral_feed_symbol = normalize_symbol(collateral_symbol_orig, collateral_addr)
        debt_feed_symbol = normalize_symbol(debt_symbol_orig, debt_addr)
        collateral_price = get_chainlink_price(collateral_feed_symbol, timestamp)
        debt_price = get_chainlink_price(debt_feed_symbol, timestamp)
        block_value = item.get('block') or item.get('b')
        try:
            block_int = int(block_value)
        except Exception:
            block_int = None
        beacon_info = get_beacon_block_info(block_int)
        
        # Statistiken
        if collateral_symbol != collateral_symbol_orig:
            stats['mapped_collateral'] += 1
        if debt_symbol != debt_symbol_orig:
            stats['mapped_debt'] += 1
        
        if collateral_symbol and not collateral_symbol.startswith('0x'):
            stats['token_symbols_found'].add(collateral_symbol)
        elif collateral_addr.startswith('0x'):
            stats['unmapped_addresses'].add(collateral_addr)
        
        if debt_symbol and not debt_symbol.startswith('0x'):
            stats['token_symbols_found'].add(debt_symbol)
        elif debt_addr.startswith('0x'):
            stats['unmapped_addresses'].add(debt_addr)
        
        # Formatiere Amounts
        collateral_amount = item.get('collateralOut', 0)
        debt_amount = item.get('debtToCover', 0)
        
        # Runde auf sinnvolle Dezimalstellen
        if isinstance(collateral_amount, (int, float)):
            collateral_amount = f"{collateral_amount:.8f}".rstrip('0').rstrip('.')
        if isinstance(debt_amount, (int, float)):
            debt_amount = f"{debt_amount:.8f}".rstrip('0').rstrip('.')
        
        processed_items.append({
            'date': date_iso,
            'datetime': datetime_iso,
            'timestamp': timestamp,
            'block': item.get('block', ''),
            'tx': item.get('tx', ''),
            'user': item.get('user', ''),
            'liquidator': item.get('liquidator', ''),
            'collateral_symbol': collateral_symbol,
            'collateral_asset': shorten_address(collateral_addr) if export_format == 'enhanced' else collateral_addr,
            'collateral_amount': collateral_amount,
            'collateral_price_usd_at_block': collateral_price,
            'debt_symbol': debt_symbol,
            'debt_asset': shorten_address(debt_addr) if export_format == 'enhanced' else debt_addr,
            'debt_amount': debt_amount,
            'debt_price_usd_at_block': debt_price,
            'block_builder': beacon_info.get('builder') or beacon_info.get('fee_recipient'),
            'block_validator': beacon_info.get('validator_address')
        })
    
    # Sortiere nach Datum und Block
    processed_items.sort(key=lambda x: (x['timestamp'] or 0, x['block'] or 0))
    
    # Erstelle CSV
    output = StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    
    # Header mit Metadaten (als Kommentare) - KORREKT formatiert
    output.write('# Aave V3 Liquidations Export\n')
    output.write(f'# Generated: {datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}\n')
    output.write(f'# Total Rows: {stats["total_rows"]}\n')
    output.write(f'# Token Mappings Applied: {stats["mapped_collateral"] + stats["mapped_debt"]}\n')
    output.write(f'# Unique Tokens: {len(stats["token_symbols_found"])}\n')
    output.write('#\n')
    
    # Spalten-Header (optimierte Reihenfolge) - MIT SEMIKOLON
    writer.writerow([
        'Date',
        'DateTime',
        'Timestamp',
        'Block',
        'Transaction Hash',
        'User',
        'Liquidator',
        'Collateral Symbol',
        'Collateral Asset',
        'Collateral Amount',
        'collateral_price_usd_at_block',
        'Debt Symbol',
        'Debt Asset',
        'Debt Covered',
        'debt_price_usd_at_block',
        'block_builder',
        'block_validator'
    ])
    
    # Daten
    for item in processed_items:
        writer.writerow([
            item['date'],
            item['datetime'],
            item['timestamp'],
            item['block'],
            item['tx'],
            item['user'],
            item['liquidator'],
            item['collateral_symbol'],
            item['collateral_asset'],
            item['collateral_amount'],
            item.get('collateral_price_usd_at_block'),
            item['debt_symbol'],
            item['debt_asset'],
            item['debt_amount'],
            item.get('debt_price_usd_at_block'),
            item.get('block_builder'),
            item.get('block_validator')
        ])
    
    # Response
    csv_data = output.getvalue()
    output.close()

@app.route('/api/history/uniswap')
def api_history_uniswap():
    """Zeitreihe für Uniswap, Parameter: window=1h|24h|7d|30m"""
    window = request.args.get('window', default='24h')
    series = metrics_store.get_uniswap_series(window)
    return jsonify({"series": series})

@app.route('/api/history/aave')
def api_history_aave():
    """Zeitreihe für Aave, Parameter: asset=WETH|USDC|..., window=1h|24h"""
    asset = request.args.get('asset', default='WETH')
    window = request.args.get('window', default='24h')
    series = metrics_store.get_aave_series(asset, window)
    return jsonify({"asset": asset, "series": series})

@app.route('/api/eth_price')
def api_eth_price():
    """Optimierter ETH-Preis Endpoint mit Multi-Source Fallback"""
    tracker = get_tracker()
    if not tracker:
        return jsonify({"error": "Tracker not initialized"}), 500
    
    force = request.args.get('force', '0') == '1'
    price, source = tracker.get_current_price(force_refresh=force)
    
    stats = tracker.get_statistics(hours=24)
    health = tracker.get_health_status()
    
    return jsonify({
        "price": price,
        "source": source,
        "stats": stats,
        "health": health,
        "timestamp": int(time.time())
    })

@app.route('/api/eth_price/history')
def api_eth_price_history():
    """ETH-Preis Historie für Charts"""
    tracker = get_tracker()
    if not tracker:
        return jsonify({"error": "Tracker not initialized"}), 500
    
    try:
        hours = int(request.args.get('hours', 24))
    except Exception:
        hours = 24
    
    history = tracker.get_price_history(hours)
    
    return jsonify({
        "history": history,
        "count": len(history)
    })

@app.route('/api/eth_price/backfill')
def api_eth_price_backfill():
    """
    Fülle Preis-Historie mit Chainlink-Daten auf
    
    Nutze dies beim ersten Start um sofort 24h+ Historie zu haben!
    Parameter: hours (default: 24, max: 168 für 1 Woche)
    """
    tracker = get_tracker()
    if not tracker:
        return jsonify({"error": "Tracker not initialized"}), 500
    
    try:
        hours = int(request.args.get('hours', 24))
        # Limit auf 1 Woche
        if hours > 168:
            hours = 168
    except Exception:
        hours = 24
    
    success = tracker.backfill_history_from_chainlink(hours)
    
    if success:
        history = tracker.get_price_history(hours)
        return jsonify({
            "success": True,
            "message": f"Historie mit {len(history)} Chainlink-Datenpunkten gefüllt",
            "data_points": len(history),
            "hours": hours
        })
    else:
        return jsonify({
            "success": False,
            "error": "Backfill fehlgeschlagen"
        }), 500

@app.route('/api/eth_price/stats')
def api_eth_price_stats():
    """
    ETH-Preis Storage Statistiken
    
    Zeigt Speicherverbrauch und Datenumfang des Price Store
    """
    try:
        stats = eth_price_store.get_stats()
        latest = eth_price_store.get_latest_price()
        
        return jsonify({
            "success": True,
            "stats": stats,
            "latest_price": latest,
            "timestamp": int(time.time())
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/eth_price/full_history')
@cache_chart_response(max_age=300)
def api_eth_price_full_history():
    """
    Kompletter ETH-Preis-Verlauf seit Aave V3 Launch (16.03.2023)
    
    Für langfristige Charts mit Liquidations-Overlay.
    
    Parameter:
    - interval: Aggregationsintervall in Stunden (default: 24 = täglich)
    - backfill: 'true' um fehlende Daten von Chainlink zu laden
    """
    from datetime import datetime
    from collections import defaultdict
    import time as time_module
    
    try:
        interval_hours = int(request.args.get('interval', 24))
    except Exception:
        interval_hours = 24
    
    backfill = request.args.get('backfill', 'false').lower() == 'true'
    
    # Aave V3 Launch: 16. März 2023
    AAVE_V3_LAUNCH = 1678982400
    now = int(time_module.time())
    total_hours = (now - AAVE_V3_LAUNCH) // 3600
    
    # Hole alle gespeicherten Preise
    all_prices = eth_price_store.get_prices(hours=total_hours)
    
    # Falls wenig Daten UND Backfill gewünscht: Hole von Chainlink
    if backfill and len(all_prices) < 500 and eth_tracker:
        try:
            # Chainlink Backfill (max ~1 Jahr wegen API Limits)
            hours_to_fetch = min(total_hours, 8760)  # Max 365 Tage
            success = eth_tracker.backfill_history_from_chainlink(hours=hours_to_fetch)
            if success:
                all_prices = eth_price_store.get_prices(hours=total_hours)
        except Exception as e:
            logger.warning(f"Chainlink Backfill Fehler: {e}")
    
    # Filtere nur Daten ab Aave V3 Launch
    filtered_prices = [p for p in all_prices if p.get('timestamp', 0) >= AAVE_V3_LAUNCH]
    
    # Aggregiere nach Intervall
    interval_seconds = interval_hours * 3600
    aggregated = defaultdict(list)
    
    for p in filtered_prices:
        ts = p.get('timestamp', 0)
        bucket = (ts // interval_seconds) * interval_seconds
        aggregated[bucket].append(p.get('price', 0))
    
    # Berechne Durchschnitt, Min, Max pro Bucket
    price_series = []
    for bucket_ts in sorted(aggregated.keys()):
        prices = aggregated[bucket_ts]
        price_series.append({
            "timestamp": bucket_ts,
            "date": datetime.fromtimestamp(bucket_ts).strftime('%Y-%m-%d %H:%M'),
            "price": round(sum(prices) / len(prices), 2),
            "min": round(min(prices), 2),
            "max": round(max(prices), 2),
            "samples": len(prices)
        })
    
    return jsonify({
        "success": True,
        "price_series": price_series,
        "stats": {
            "total_raw_samples": len(all_prices),
            "filtered_samples": len(filtered_prices),
            "aggregated_points": len(price_series),
            "interval_hours": interval_hours,
            "start_date": datetime.fromtimestamp(AAVE_V3_LAUNCH).strftime('%Y-%m-%d'),
            "end_date": datetime.fromtimestamp(now).strftime('%Y-%m-%d'),
            "days_covered": (now - AAVE_V3_LAUNCH) // 86400,
            "data_coverage_percent": round((len(filtered_prices) / max(1, total_hours // 24)) * 100, 1)
        }
    })

@app.route('/api/history/eth_price_liquidations')
@cache_chart_response(max_age=60)
def api_history_eth_price_liquidations():
    """Historische ETH-Preise und Liquidationen für Chart mit intelligenter Aggregation
    
    Parameter:
    - timeWindow: '1h', '6h', '24h', '7d', '30d' (überschreibt hours)
    - hours: Anzahl Stunden (falls timeWindow nicht gesetzt)
    """
    import time as time_module
    from collections import defaultdict
    
    # Parse timeWindow Parameter (neue Dropdown-Option)
    time_window = request.args.get('timeWindow', '').lower()
    
    # Mapping: timeWindow -> (hours, aggregation_seconds)
    WINDOW_CONFIG = {
        '1h': (1, 60),          # 1 Stunde → 1 Minute Aggregation
        '6h': (6, 300),         # 6 Stunden → 5 Minuten Aggregation
        '24h': (24, 3600),      # 24 Stunden → 1 Stunde Aggregation
        '7d': (168, 21600),     # 7 Tage → 6 Stunden Aggregation
        '30d': (720, 86400)     # 30 Tage → 1 Tag Aggregation
    }
    
    if time_window in WINDOW_CONFIG:
        hours, bar_interval = WINDOW_CONFIG[time_window]
        aggregation_type = time_window
    else:
        # Fallback: Alte hours-basierte Logik
        try:
            hours = int(request.args.get('hours', 24))
        except Exception:
            hours = 24
        
        # Bestimme Aggregationsintervall basierend auf Zeitspanne
        if hours <= 1:
            bar_interval = 60  # 1 Minute
            aggregation_type = "1-Minute"
        elif hours <= 6:
            bar_interval = 300  # 5 Minuten
            aggregation_type = "5-Minuten"
        elif hours <= 24:
            bar_interval = 3600  # 1 Stunde
            aggregation_type = "Stunden"
        elif hours <= 168:
            bar_interval = 21600  # 6 Stunden
            aggregation_type = "6-Stunden"
        else:
            bar_interval = 86400  # 1 Tag
            aggregation_type = "Tage"
    
    # Hole ETH-Preis-Verlauf aus Storage (schneller als Blockchain-Scan)
    eth_prices = eth_price_store.get_prices(hours=hours)
    
    # Falls nicht genug Daten vorhanden: Triggere Backfill SYNCHRON
    # Mindest-Datenpunkte: 1h=5, 6h=10, 24h=15, 7d=50, 30d=100
    min_expected_by_hours = {1: 3, 6: 8, 24: 15, 168: 50, 720: 100}
    min_expected_points = min_expected_by_hours.get(hours, max(5, hours // 10))
    
    if len(eth_prices) < min_expected_points and eth_tracker:
        try:
            logger.info(f"Backfill: {hours}h Preis-Daten (haben {len(eth_prices)}, brauchen {min_expected_points})")
            eth_tracker.backfill_history_from_chainlink(hours=min(hours, 720))  # Max 30 Tage Backfill
            eth_prices = eth_price_store.get_prices(hours=hours)
        except Exception as e:
            logger.warning(f"Backfill fehlgeschlagen: {e}")
    
    # Aggregiere ETH-Preise nach Intervall (Durchschnitt pro Bar)
    price_buckets = defaultdict(list)
    for p in eth_prices:
        bucket_ts = (p["timestamp"] // bar_interval) * bar_interval
        price_buckets[bucket_ts].append(p["price"])
    
    eth_price_series = []
    for ts, prices in sorted(price_buckets.items()):
        avg_price = sum(prices) / len(prices)
        eth_price_series.append({
            "t": ts,
            "eth_price": round(avg_price, 2)
        })
    
    # Hole Liquidationen direkt aus CSV (schnell und robust)
    all_liquidations = get_liquidations_from_csv(hours=hours)
    
    # Aggregiere Liquidationen nach Intervall (Summe pro Bar)
    liq_buckets = defaultdict(int)
    for liq in all_liquidations:
        # Nutze "time" Feld (nicht "timestamp")
        liq_time = liq.get("time", 0)
        if liq_time == 0:
            continue
        bucket_ts = (liq_time // bar_interval) * bar_interval
        liq_buckets[bucket_ts] += 1
    
    # Konvertiere zu Chart-Format mit detaillierten Stats
    liq_series = [{"x": ts * 1000, "y": count} for ts, count in sorted(liq_buckets.items())]
    
    # Berechne Korrelationsstatistiken (ETH Preis-Drops vs Liquidations)
    total_liquidations = sum(liq_buckets.values())
    max_liq_count = max(liq_buckets.values()) if liq_buckets else 0
    
    # Finde größte Preisschwankungen
    price_changes = []
    eth_prices_sorted = sorted(price_buckets.items())
    for i in range(1, len(eth_prices_sorted)):
        prev_ts, prev_prices = eth_prices_sorted[i-1]
        curr_ts, curr_prices = eth_prices_sorted[i]
        prev_avg = sum(prev_prices) / len(prev_prices)
        curr_avg = sum(curr_prices) / len(curr_prices)
        change_pct = ((curr_avg - prev_avg) / prev_avg * 100) if prev_avg > 0 else 0
        price_changes.append((curr_ts, change_pct))
    
    max_price_drop = min(price_changes, key=lambda x: x[1])[1] if price_changes else 0
    
    return jsonify({
        "eth_price_series": eth_price_series,
        "liquidation_series": liq_series,
        "total_liquidations": total_liquidations,
        "aggregation": aggregation_type,
        "hours": hours,
        "timeWindow": time_window if time_window in WINDOW_CONFIG else None,
        "stats": {
            "total_liquidations": total_liquidations,
            "max_liquidations_per_bucket": max_liq_count,
            "avg_liquidations_per_bucket": round(total_liquidations / len(liq_buckets), 2) if liq_buckets else 0,
            "max_price_drop_pct": round(max_price_drop, 2),
            "eth_price_points": len(eth_price_series),
            "bar_interval_seconds": bar_interval,
            "total_price_points": len(eth_prices),
            "aggregated_price_points": len(eth_price_series),
            "aggregated_bars": len(liq_series)
        }
    })

@app.route('/api/liquidations/aggregated')
@cache_chart_response(max_age=60)
def api_liquidations_aggregated():
    """
    Aggregierte Liquidations-Daten für optimierte Chart-Anzeige
    
    Gruppiert Liquidationen nach Zeitintervall mit Totals und Statistiken.
    
    Parameter:
    - timeWindow: '1h', '6h', '24h', '7d', '30d'
    - hours: Alternative zu timeWindow (Stunden rückwärts)
    """
    import time as time_module
    from collections import defaultdict
    
    # Parse timeWindow Parameter
    time_window = request.args.get('timeWindow', '').lower()
    
    # Mapping: timeWindow -> (hours, aggregation_seconds)
    WINDOW_CONFIG = {
        '1h': (1, 60),          # 1 Minute Buckets
        '6h': (6, 300),         # 5 Minuten Buckets
        '24h': (24, 3600),      # 1 Stunde Buckets
        '7d': (168, 21600),     # 6 Stunden Buckets
        '30d': (720, 86400)     # 1 Tag Buckets
    }
    
    if time_window in WINDOW_CONFIG:
        hours, bucket_interval = WINDOW_CONFIG[time_window]
    else:
        try:
            hours = int(request.args.get('hours', 24))
        except:
            hours = 24
        bucket_interval = 3600 if hours <= 24 else 21600 if hours <= 168 else 86400
    
    # Hole alle Liquidationen aus CSV
    all_liquidations = get_liquidations_from_csv(hours=hours, limit=None)
    
    # Aggregiere nach Buckets
    buckets = defaultdict(lambda: {
        'count': 0,
        'total_collateral_usd': 0,
        'total_debt_usd': 0,
        'liquidations': []
    })
    
    for liq in all_liquidations:
        liq_time = liq.get('time', 0)
        if liq_time == 0:
            continue
        
        bucket_ts = (liq_time // bucket_interval) * bucket_interval
        bucket = buckets[bucket_ts]
        
        bucket['count'] += 1
        bucket['total_collateral_usd'] += liq.get('collateralAmountUSD', 0)
        bucket['total_debt_usd'] += liq.get('debtAmountUSD', 0)
        
        # Speichere kompakte Liquidation (optional für Details)
        bucket['liquidations'].append({
            'hash': liq.get('hash', ''),
            'collateralAsset': liq.get('collateralSymbol', ''),
            'debtAsset': liq.get('debtSymbol', ''),
            'user': liq.get('user', '')[:10] + '...'
        })
    
    # Konvertiere zu Array
    aggregated_data = []
    for ts in sorted(buckets.keys()):
        bucket = buckets[ts]
        aggregated_data.append({
            'timestamp': ts,
            'count': bucket['count'],
            'total_collateral_usd': round(bucket['total_collateral_usd'], 2),
            'total_debt_usd': round(bucket['total_debt_usd'], 2),
            'avg_collateral_usd': round(bucket['total_collateral_usd'] / bucket['count'], 2) if bucket['count'] > 0 else 0,
            'avg_debt_usd': round(bucket['total_debt_usd'] / bucket['count'], 2) if bucket['count'] > 0 else 0,
            'sample_liquidations': bucket['liquidations'][:3]  # Top 3 für Details
        })
    
    # Gesamt-Statistiken
    total_count = sum(b['count'] for b in buckets.values())
    total_collateral = sum(b['total_collateral_usd'] for b in buckets.values())
    total_debt = sum(b['total_debt_usd'] for b in buckets.values())
    
    return jsonify({
        'aggregated_data': aggregated_data,
        'timeWindow': time_window if time_window in WINDOW_CONFIG else None,
        'hours': hours,
        'bucket_interval_seconds': bucket_interval,
        'stats': {
            'total_liquidations': total_count,
            'total_buckets': len(buckets),
            'avg_per_bucket': round(total_count / len(buckets), 2) if buckets else 0,
            'total_collateral_usd': round(total_collateral, 2),
            'total_debt_usd': round(total_debt, 2),
            'max_bucket_count': max((b['count'] for b in buckets.values()), default=0)
        }
    })

@app.route('/api/dashboard/summary')
def api_dashboard_summary():
    """Batch-API: Alle Dashboard-Daten in einem Request"""
    cache_key_30s = _cache_key_30s()
    cache_key_5min = _cache_key_5min()  # 🔧 5-Minuten-Cache für Aave
    chain_name = _selected_chain()
    try:
        # Uniswap V2 ist aktuell nur auf Ethereum konfiguriert
        if chain_name == 'ethereum':
            uni_v2 = _cached_uniswap(cache_key_30s)
        else:
            uni_v2 = {"error": "Uniswap V2 ist nur auf Ethereum verfügbar"}

        return jsonify({
            "uniswap_v2": uni_v2,
            "uniswap_v3": _cached_uniswap_v3(chain_name, cache_key_30s),
            "aave": _cached_aave(chain_name, cache_key_5min),  # 🔧 5-Minuten-Cache!
            "eth_network": _cached_eth_network(cache_key_30s),
            "chain": chain_name,
            "timestamp": time.time()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/wallet/positions')
def api_wallet_positions():
    """Aggregierte DeFi-Positionen für eine Wallet-Adresse (Ethereum Mainnet only)."""
    # Only available on Ethereum Mainnet
    if ACTIVE_CHAIN != 'ethereum':
        return jsonify({
            "error": "Wallet position analysis is only available on Ethereum Mainnet",
            "current_chain": ACTIVE_CHAIN
        }), 400
    
    address = (request.args.get('address') or '').strip()
    if not address.startswith('0x') or len(address) != 42:
        return jsonify({"error": "invalid address"}), 400
    try:
        data = get_wallet_positions(address)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/uniswap/position/<int:position_id>')
def api_uniswap_position(position_id):
    """
    Analyze a specific Uniswap V3 Position NFT
    
    Returns complete position analysis:
    - Token pair and fee tier
    - Liquidity range (ticks and prices)
    - Current price vs range
    - Position value in USD
    - Unclaimed fees
    - In-range status
    - Liquidity share of pool
    """
    try:
        result = analyze_v3_position(w3, position_id)
        
        if result.get("success"):
            return jsonify(result)
        else:
            return jsonify(result), 404
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "position_id": position_id
        }), 500

@app.route('/api/uniswap/wallet/<wallet_address>/positions')
def api_uniswap_wallet_positions(wallet_address):
    """
    Analyze all Uniswap V3 positions for a wallet
    
    Returns:
    - Summary (total value, count, active/inactive)
    - Detailed analysis for each position
    - Aggregated statistics
    
    Example: /api/uniswap/wallet/0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb/positions
    """
    try:
        # Validate address
        if not wallet_address.startswith('0x') or len(wallet_address) != 42:
            return jsonify({
                "success": False,
                "error": "Invalid Ethereum address"
            }), 400
        
        result = analyze_wallet_positions(wallet_address)
        
        if result.get("success"):
            return jsonify(result)
        else:
            return jsonify(result), 404
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "wallet": wallet_address
        }), 500


@app.route('/download')
def download_master_csv():
    """Liefert die konsolidierte Liquidations-Master-CSV."""
    try:
        refresh_master_csv()
    except Exception as exc:
        logger.warning("Download refresh failed: %s", exc)
    ensure_master_csv_exists()
    return send_file(
        MASTER_CSV_PATH,
        mimetype='text/csv',
        as_attachment=True,
        download_name='liquidations_master.csv',
    )

if __name__ == '__main__':
    # Professional startup banner - Logo FIRST before any background services
    # Attempt to fetch a fresh ETH price synchronously so the startup banner
    # shows a live green price line. This does not start background services.
    try:
        _tmp_tracker = get_tracker(w3)
        p_start, s_start = (None, None)
        if _tmp_tracker:
            # Try a few times (short retries) to get a fresh price at startup.
            attempts = 3
            for _ in range(attempts):
                try:
                    p_start, s_start = _tmp_tracker.get_current_price(force_refresh=True)
                    if p_start:
                        break
                except Exception:
                    p_start, s_start = (None, None)
                time.sleep(0.5)
            # Store singleton tracker for later use by background services
            globals()['eth_tracker'] = _tmp_tracker
        else:
            p_start, s_start = (None, None)
    except Exception:
        p_start, s_start = (None, None)

    print("\n" + "="*70)
    # Colored banner with BOLD styling for professional look
    MAGENTA = '\x1b[35m'
    BLUE = '\x1b[34m'
    GREEN = '\x1b[32m'
    CYAN = '\x1b[36m'
    YELLOW = '\x1b[33m'
    RESET = '\x1b[0m'
    BOLD = '\x1b[1m'
    DIM = '\x1b[2m'
    
    print(BOLD + MAGENTA + "\n  ____        _____ _    ___  _                                " + RESET)
    print(BOLD + MAGENTA + r" |  _ \  ___ |  ___(_)  / _ \| |__  ___  ___ _ ____   _____ _ __ " + RESET)
    print(BOLD + MAGENTA + r" | | | |/ _ \| |_  | | | | | | '_ \/ __|/ _ \ '__\ \ / / _ \ '__|" + RESET)
    print(BOLD + MAGENTA + r" | |_| |  __/|  _| | | | |_| | |_) \__ \  __/ |   \ V /  __/ |   " + RESET)
    print(BOLD + MAGENTA + r" |____/ \___||_|   |_|  \___/|_.__/|___/\___|_|    \_/ \___|_|   " + RESET)
    print("")
    print("  " + BOLD + BLUE + "Version 2.0" + RESET + DIM + " - AAVE V3 Mainnet Liquidation Monitor" + RESET)
    print("")
    # Print current ETH price with colors
    try:
        if 'p_start' in globals() and p_start:
            print("  " + BOLD + GREEN + "ETH Price:" + RESET + GREEN + f" ${p_start:,.2f}" + RESET + DIM + f" ({s_start})" + RESET)
        else:
            # Fallback: try cached price store
            p, s = _get_latest_eth_price()
            if p:
                print("  " + BOLD + GREEN + "ETH Price:" + RESET + GREEN + f" ${p:,.2f}" + RESET + DIM + f" ({s})" + RESET)
            else:
                print("  " + YELLOW + "ETH Price: (fetching...)" + RESET)
    except Exception:
        print("  " + YELLOW + "ETH Price: (fetching...)" + RESET)
    print("  " + "-"*66)
    
    # Print status overview
    print("")
    print("  " + BOLD + CYAN + "System Status:" + RESET)
    
    # Check scan status
    scan_status = _get_scan_status()
    if scan_status:
        events = scan_status.get('events_found', 0)
        status = scan_status.get('status', 'unknown')
        from_b = scan_status.get('from_block', 0)
        to_b = scan_status.get('to_block', 0)
        print("  " + f"   Liquidations: " + BOLD + f"{events:,}" + RESET + DIM + f" events (blocks {from_b:,} → {to_b:,})" + RESET)
        if status == 'completed':
            print("  " + GREEN + "   Scan: Up to date" + RESET)
        elif status == 'running':
            print("  " + YELLOW + "   Scan: Running..." + RESET)
        else:
            print("  " + DIM + f"   Scan: {status}" + RESET)
    else:
        print("  " + YELLOW + "   Liquidations: Initializing..." + RESET)
    
    print("")
    print("  " + BOLD + CYAN + "Network Access:" + RESET)
    # Print local and network addresses. Attempt to auto-detect a usable
    # LAN-facing IP by opening a UDP socket to a public IP (no packets sent).
    def _detect_network_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.5)
            # This does not send data but yields the outbound interface IP
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            if ip and not ip.startswith("127.") and ip != "0.0.0.0":
                return ip
        except Exception:
            pass
        return None

    port = int(os.environ.get('PORT', 5000))
    host_ip = _detect_network_ip() or os.environ.get('HOST_IP')

    # Print network addresses
    print("  " + DIM + f"   Local:   " + RESET + f"http://127.0.0.1:{port}")
    if host_ip:
        print("  " + DIM + "   Network: " + RESET + GREEN + f"http://{host_ip}:{port}" + RESET)
    print("")
    print("  " + "="*66)
    print("")
    
    # Now start background services AFTER logo
    # Allow disabling background services (scanner, price backfills) for CI/tests
    bg_disabled = False
    if os.environ.get('DISABLE_BACKGROUND_SERVICES', '').lower() in ('1', 'true', 'yes'):
        bg_disabled = True
        logger.warning("⚠️  Background services disabled via DISABLE_BACKGROUND_SERVICES")
    else:
        _init_background_services()

    # Final readiness/info log before the blocking app.run()
    if bg_disabled:
        logger.warning("⚠️  [App] Background services disabled")
    else:
        logger.info("[App] Ready - Background services active")

    # Start the banner updater (clears terminal and shows ETH price). Make interval configurable via env.
    try:
        interval = int(os.environ.get('TERMINAL_BANNER_UPDATE_SECS', '30'))
    except Exception:
        interval = 30
    _start_banner_updater(interval_seconds=interval)
    
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False)
