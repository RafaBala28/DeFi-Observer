"""
ETH Price Tracker mit intelligenter Fallback-Kette
Quellen: Chainlink → Uniswap V3 → Uniswap V2 → Cache → CoinGecko
"""

from web3 import Web3
import time
import json
import os
import requests
from typing import Optional, Tuple, List, Dict, Any
import eth_price_store
import logging

logger = logging.getLogger(__name__)

class ETHPriceTracker:
    def __init__(self, w3: Web3):
        self.w3 = w3
        self.cache_file = "data/eth_price_cache.json"
        self.history_file = "data/eth_price_history.json"
        
        # 1. Chainlink ETH/USD Price Feed (BESTE Quelle)
        self.chainlink_feed = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
        self.chainlink_abi = [
            {
                "inputs": [],
                "name": "latestRoundData",
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"}
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [{"name": "_roundId", "type": "uint80"}],
                "name": "getRoundData",
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"}
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        # 2. Uniswap V3 ETH/USDC Pool (0.05% fee - höchste Liquidität: $300M+)
        self.univ3_pool = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        self.univ3_abi = [{
            "inputs": [],
            "name": "slot0",
            "outputs": [
                {"name": "sqrtPriceX96", "type": "uint160"},
                {"name": "tick", "type": "int24"},
                {"name": "observationIndex", "type": "uint16"},
                {"name": "observationCardinality", "type": "uint16"},
                {"name": "observationCardinalityNext", "type": "uint16"},
                {"name": "feeProtocol", "type": "uint8"},
                {"name": "unlocked", "type": "bool"}
            ],
            "stateMutability": "view",
            "type": "function"
        }]
        
        # 3. CoinGecko API (nur als letzter Fallback wegen Rate Limits)
        self.coingecko_url = "https://api.coingecko.com/api/v3/simple/price"
        self.coingecko_last_call = 0
        self.coingecko_min_interval = 60  # Max 1 call pro Minute
        
        # Cache
        self.price_cache = self._load_cache()
        self.last_update = 0
        self.cache_duration = 30  # 30s Cache
        
        # Statistiken
        self.stats = {
            "chainlink_calls": 0,
            "chainlink_success": 0,
            "univ3_calls": 0,
            "univ3_success": 0,
            "univ2_calls": 0,
            "univ2_success": 0,
            "coingecko_calls": 0,
            "coingecko_success": 0,
            "cache_hits": 0
        }
    
    def _load_cache(self) -> Dict[str, Any]:
        """Lade Cache von Disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    # Prüfe ob Cache noch gültig (max 5 Minuten alt)
                    if time.time() - data.get("timestamp", 0) < 300:
                        return data
            except:
                pass
        return {"price": 0, "timestamp": 0, "source": "none"}
    
    def _save_cache(self, price: float, source: str, quality: str = "high") -> None:
        """Speichere Cache auf Disk"""
        cache = {
            "price": price,
            "timestamp": int(time.time()),
            "source": source,
            "quality": quality
        }
        os.makedirs("data", exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(cache, f)
        self.price_cache = cache
    
    def _save_to_history(self, price: float, source: str) -> None:
        """Speichere in Historie für Charts"""
        timestamp = int(time.time())
        
        # PRIMÄR: Speichere im neuen eth_price_store (optimiert, 30 Tage)
        try:
            eth_price_store.append_price(
                timestamp=timestamp,
                price=round(price, 2),
                source=source,
                decimals=8 if source == "chainlink" else 18
            )
        except Exception as e:
            logger.warning("Fehler beim Speichern in eth_price_store: %s", e)
        
        # LEGACY JSON wird nicht mehr genutzt (verwende eth_price_store stattdessen)
        # Entferne alte Datei falls vorhanden
        if os.path.exists(self.history_file):
            try:
                os.remove(self.history_file)
            except:
                pass
    
    def get_price_from_chainlink(self) -> Tuple[Optional[float], Optional[str]]:
        """
        Holt Preis von Chainlink Oracle
        ⭐⭐⭐⭐⭐ Beste Quelle: Genauigkeit, Multi-Oracle, Manipulation-resistent
        """
        self.stats["chainlink_calls"] += 1
        
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.chainlink_feed),
                abi=self.chainlink_abi
            )
            
            round_data = contract.functions.latestRoundData().call()
            price = round_data[1] / 1e8  # Chainlink nutzt 8 Dezimalstellen
            updated_at = round_data[3]
            
            # Prüfe ob Daten aktuell (nicht älter als 2 Stunden)
            if time.time() - updated_at > 7200:
                logger.warning("Chainlink Daten veraltet (%dmin alt)", int((time.time()-updated_at)/60))
                return None, None
            
            # Sanity Check: ETH sollte zwischen $100 und $10,000 liegen
            if not (100 < price < 10000):
                logger.warning("Chainlink Preis unrealistisch: $%s", price)
                return None, None
            
            self.stats["chainlink_success"] += 1
            return price, "chainlink"
            
        except Exception as e:
            logger.error("Chainlink Fehler: %s", e)
            return None, None
    
    def get_chainlink_historical_data(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Holt historische Preis-Daten von Chainlink
        
        Chainlink aktualisiert ca. alle 1-2 Stunden oder bei >0.5% Preisänderung.
        Wir können zurück durch die Runden gehen um Historie zu bekommen.
        
        Args:
            hours: Anzahl Stunden zurück (max 168 = 1 Woche)
        
        Returns:
            Liste von {timestamp, price, roundId}
        """
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.chainlink_feed),
                abi=self.chainlink_abi
            )
            
            # Hole aktuelle Runde
            latest_round = contract.functions.latestRoundData().call()
            current_round_id = latest_round[0]
            
            # Sammle historische Daten
            historical_data = []
            target_timestamp = int(time.time()) - (hours * 3600)
            
            # Chainlink Phase-ID ist in den oberen Bits kodiert
            # Wir gehen rückwärts durch die Runden
            round_id = current_round_id
            attempts = 0
            max_attempts = 200  # Safety limit
            
            logger.debug("[ETH Price] Fetching Chainlink history: %sh back...", hours)
            
            while attempts < max_attempts:
                try:
                    round_data = contract.functions.getRoundData(round_id).call()
                    
                    timestamp = round_data[3]  # updatedAt
                    price = round_data[1] / 1e8
                    
                    # Stoppe wenn wir alt genug sind
                    if timestamp < target_timestamp:
                        break
                    
                    # Sanity Check
                    if 100 < price < 10000:
                        historical_data.append({
                            "timestamp": timestamp,
                            "price": round(price, 2),
                            "roundId": round_id,
                            "source": "chainlink"
                        })
                    
                    # Gehe zur vorherigen Runde
                    round_id -= 1
                    attempts += 1
                    
                except Exception as e:
                    # Runde existiert nicht mehr oder Fehler
                    logger.warning("Chainlink Round %s nicht verfügbar: %s", round_id, e)
                    break
            
            # Sortiere chronologisch (älteste zuerst)
            historical_data.reverse()
            
            logger.info("[ETH Price] Loaded %d Chainlink data points", len(historical_data))
            
            return historical_data
            
        except Exception as e:
            logger.error("[USD Prices] Chainlink history error: %s", e)
            return []
    
    def get_price_from_uniswap_v3(self) -> Tuple[Optional[float], Optional[str]]:
        """
        Holt Preis von Uniswap V3
        ⭐⭐⭐⭐ Sehr gut: Real-time, hohe Liquidität, TWAP-fähig
        """
        self.stats["univ3_calls"] += 1
        
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.univ3_pool),
                abi=self.univ3_abi
            )
            
            slot0 = contract.functions.slot0().call()
            sqrtPriceX96 = slot0[0]
            
            # Konvertiere sqrtPriceX96 zu Preis
            # Pool ist USDC/WETH, token0=USDC, token1=WETH
            price = (sqrtPriceX96 / (2 ** 96)) ** 2
            
            # USDC=6 decimals, ETH=18 decimals → Faktor: 10^12
            eth_price = price * (10 ** 12)
            
            # Pool kann invertiert sein - prüfen
            if eth_price < 100:  # Falls < $100, dann ist Preis invertiert
                eth_price = 1 / price / (10 ** 12)
            
            # Sanity Check
            if not (100 < eth_price < 10000):
                logger.warning("Uniswap V3 Preis unrealistisch: $%s", eth_price)
                return None, None
            
            self.stats["univ3_success"] += 1
            return eth_price, "uniswap_v3"
            
        except Exception as e:
            logger.error("Uniswap V3 Fehler: %s", e)
            return None, None
    
    def get_price_from_uniswap_v2(self) -> Tuple[Optional[float], Optional[str]]:
        """
        Holt Preis von Uniswap V2
        ⭐⭐⭐ OK: Einfach, aber anfälliger für Manipulation
        """
        self.stats["univ2_calls"] += 1
        
        try:
            from uniswap_data import get_pool_data
            data = get_pool_data()
            
            if data and data.get("eth_price", 0) > 0:
                price = data["eth_price"]
                
                # Sanity Check
                if not (100 < price < 10000):
                    return None, None
                
                self.stats["univ2_success"] += 1
                return price, "uniswap_v2"
        except Exception as e:
            logger.error("Uniswap V2 Fehler: %s", e)
        
        return None, None
    
    def get_price_from_coingecko(self) -> Tuple[Optional[float], Optional[str]]:
        """
        Holt Preis von CoinGecko API
        ⭐⭐⭐ Fallback: Extern, Rate Limits, aber zuverlässig
        NUR nutzen wenn On-Chain Quellen fehlschlagen!
        """
        now = time.time()
        
        # Rate Limiting: Max 1 Call pro Minute
        if now - self.coingecko_last_call < self.coingecko_min_interval:
            remaining = int(self.coingecko_min_interval - (now - self.coingecko_last_call))
            logger.warning("⏳ CoinGecko Rate Limit: warte %ds", remaining)
            return None, None
        
        self.stats["coingecko_calls"] += 1
        
        try:
            params = {
                "ids": "ethereum",
                "vs_currencies": "usd",
                "precision": 2
            }
            
            response = requests.get(
                self.coingecko_url,
                params=params,
                timeout=5
            )
            
            self.coingecko_last_call = now
            
            if response.status_code == 200:
                data = response.json()
                price = data.get("ethereum", {}).get("usd", 0)
                
                if price > 0:
                    # Sanity Check
                    if not (100 < price < 10000):
                        return None, None
                    
                    self.stats["coingecko_success"] += 1
                    return price, "coingecko"
            
                elif response.status_code == 429:
                    logger.warning("CoinGecko Rate Limit erreicht")
            
            return None, None
            
        except Exception as e:
            logger.error("CoinGecko Fehler: %s", e)
            return None, None
    
    def get_current_price(self, force_refresh: bool = False) -> Tuple[float, str]:
        """
        Holt aktuellen ETH-Preis mit intelligenter Fallback-Kette
        
        OPTIMALE Reihenfolge (nach Genauigkeit & Zuverlässigkeit):
        
        1. 🥇 Chainlink Oracle (±0.01%, Multi-Oracle, dezentral)
        2. 🥈 Uniswap V3 (±0.1%, real-time, hohe Liquidität)
        3. 🥉 Uniswap V2 (±0.5%, real-time, einfach)
        4. Cache (falls < 5min alt)
        5. 🌐 CoinGecko API (nur als LETZTER Fallback wegen Rate Limits)
        
        Returns:
            (price: float, source: str)
        """
        now = int(time.time())
        
        # Cache-Check (falls noch gültig)
        if not force_refresh and (now - self.last_update) < self.cache_duration:
            if self.price_cache.get("price", 0) > 0:
                self.stats["cache_hits"] += 1
                return self.price_cache["price"], f"{self.price_cache['source']}_cached"
        
        # 1. PRIMÄR: Chainlink (beste Genauigkeit, aber manchmal veraltet)
        price, source = self.get_price_from_chainlink()
        if price and price > 0:
            self._save_cache(price, source, quality="high")
            self._save_to_history(price, source)
            self.last_update = now
            return price, source
        
        # 2. FALLBACK 1: Uniswap V3 (real-time, hohe Liquidität)
        price, source = self.get_price_from_uniswap_v3()
        if price and price > 0:
            self._save_cache(price, source, quality="high")
            self._save_to_history(price, source)
            self.last_update = now
            return price, source
        
        # 3. FALLBACK 2: Uniswap V2 (einfach, aber OK)
        price, source = self.get_price_from_uniswap_v2()
        if price and price > 0:
            self._save_cache(price, source, quality="medium")
            self._save_to_history(price, source)
            self.last_update = now
            return price, source
        
        # 4. FALLBACK 3: Alter Cache (falls vorhanden und < 5min)
        cached_price = self.price_cache.get("price", 0)
        cache_age = now - self.price_cache.get("timestamp", 0)
        if cached_price > 0 and cache_age < 300:  # 5 Minuten
            logger.warning("Nutze Cache (%dmin alt): $%s", int(cache_age/60), f"{cached_price:,.2f}")
            return cached_price, f"{self.price_cache['source']}_cached_old"
        
        # 5. LETZTER FALLBACK: CoinGecko (nur wenn alle On-Chain Quellen down)
        logger.warning("Alle On-Chain Quellen fehlgeschlagen - versuche CoinGecko...")
        price, source = self.get_price_from_coingecko()
        if price and price > 0:
            self._save_cache(price, source, quality="low")
            self._save_to_history(price, source)
            self.last_update = now
            return price, source
        
        # NOTFALL: Gebe letzten bekannten Preis zurück
        if cached_price > 0:
            logger.error("ALLE Quellen fehlgeschlagen - nutze letzten Cache: $%s", f"{cached_price:,.2f}")
            return cached_price, "cached_emergency"
        
        # Komplett fehlgeschlagen
        logger.critical("KRITISCH: Kein Preis verfügbar!")
        return 0, "unavailable"
    
    def backfill_history_from_chainlink(self, hours: int = 24) -> bool:
        """
        Füllt lokale Historie mit Chainlink-Daten auf
        
        Nutze dies beim ersten Start um sofort historische Daten zu haben!
        
        Args:
            hours: Anzahl Stunden zurück zu laden
        
        Returns:
            True wenn erfolgreich
        """
        logger.debug("[ETH Price] Loading %sh Chainlink history...", hours)
        
        chainlink_data = self.get_chainlink_historical_data(hours)
        
        if not chainlink_data:
            logger.warning("[USD Prices] Backfill failed")
            return False
        
        # Merge mit existierender Historie
        existing_history = self.get_price_history(hours * 2)  # Lade etwas mehr
        
        # Erstelle Timestamp-Set für schnelle Lookup
        existing_timestamps = {h["timestamp"] for h in existing_history}
        
        # Füge nur neue Datenpunkte hinzu
        new_points = 0
        new_store_points = []
        for point in chainlink_data:
            if point["timestamp"] not in existing_timestamps:
                # Collect new points for bulk import. Avoid calling
                # _save_to_history() here (it appends individually and logs per-point).
                new_store_points.append({
                    "timestamp": point["timestamp"],
                    "price": point["price"],
                    "source": point["source"],
                    "decimals": 8  # Chainlink uses 8 decimals
                })
                new_points += 1
        
        # Bulk-Import in eth_price_store (effizienter)
        if new_store_points:
            try:
                eth_price_store.append_prices(new_store_points)
            except Exception as e:
                logger.warning("Fehler beim Bulk-Import in eth_price_store: %s", e)
        
        logger.info("[ETH Price] Backfill complete: %d new data points", new_points)
        return True
    
    def get_price_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Holt Preis-Historie für Charts"""
        # PRIMÄR: Nutze eth_price_store (optimiert, mehr Daten)
        try:
            store_history = eth_price_store.get_prices(hours=hours)
            if store_history:
                return store_history
        except Exception as e:
            logger.warning("Fehler beim Laden aus eth_price_store: %s", e)
        
        # FALLBACK: Nutze alte JSON-Datei
        if not os.path.exists(self.history_file):
            return []
        
        try:
            with open(self.history_file, 'r') as f:
                history = json.load(f)
            
            cutoff = int(time.time()) - (hours * 3600)
            history = [h for h in history if h["timestamp"] > cutoff]
            
            return history
        except:
            return []
    
    def get_statistics(self, hours: int = 24) -> Optional[Dict[str, Any]]:
        """Berechne Statistiken (24h high/low/change)"""
        history = self.get_price_history(hours)
        
        if not history:
            return None
        
        prices = [h["price"] for h in history]
        
        current = prices[-1] if prices else 0
        high_24h = max(prices) if prices else 0
        low_24h = min(prices) if prices else 0
        
        if len(prices) > 1:
            change_24h = current - prices[0]
            change_pct = (change_24h / prices[0] * 100) if prices[0] > 0 else 0
        else:
            change_24h = 0
            change_pct = 0
        
        # Quellen-Verteilung
        sources = {}
        for h in history:
            src = h.get("source", "unknown")
            sources[src] = sources.get(src, 0) + 1
        
        return {
            "current": current,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "change_24h": change_24h,
            "change_pct": change_pct,
            "data_points": len(history),
            "sources": sources,
            "tracker_stats": self.stats
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Gibt Gesundheitsstatus der Preis-Quellen zurück"""
        total_calls = sum([
            self.stats["chainlink_calls"],
            self.stats["univ3_calls"],
            self.stats["univ2_calls"],
            self.stats["coingecko_calls"]
        ])
        
        if total_calls == 0:
            return {"status": "not_started"}
        
        success_rates = {}
        
        if self.stats["chainlink_calls"] > 0:
            success_rates["chainlink"] = (
                self.stats["chainlink_success"] / self.stats["chainlink_calls"] * 100
            )
        
        if self.stats["univ3_calls"] > 0:
            success_rates["uniswap_v3"] = (
                self.stats["univ3_success"] / self.stats["univ3_calls"] * 100
            )
        
        if self.stats["univ2_calls"] > 0:
            success_rates["uniswap_v2"] = (
                self.stats["univ2_success"] / self.stats["univ2_calls"] * 100
            )
        
        if self.stats["coingecko_calls"] > 0:
            success_rates["coingecko"] = (
                self.stats["coingecko_success"] / self.stats["coingecko_calls"] * 100
            )
        
        # Bestimme beste Quelle
        best_source = max(success_rates.items(), key=lambda x: x[1]) if success_rates else ("none", 0)
        
        return {
            "status": "healthy" if best_source[1] > 80 else "degraded" if best_source[1] > 50 else "critical",
            "best_source": best_source[0],
            "best_source_success_rate": round(best_source[1], 1),
            "success_rates": success_rates,
            "cache_hit_rate": round(self.stats["cache_hits"] / total_calls * 100, 1) if total_calls > 0 else 0,
            "total_calls": total_calls
        }


# Globale Instanz
_tracker = None

def get_tracker(w3=None):
    """Singleton für ETH Price Tracker"""
    global _tracker
    if _tracker is None and w3:
        _tracker = ETHPriceTracker(w3)
    return _tracker
