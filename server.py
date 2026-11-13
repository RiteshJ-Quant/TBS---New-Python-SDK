"""
Kotak Neo & Indian Markets Web Server (server.py)
Provides REST endpoints for Kotak Neo broker authentication, live NIFTY 50 / SENSEX index price streaming,
and TBS (Trading Strategy Builder & Execution System).
"""

import os
import sys
import builtins
import logging

# Mute Werkzeug HTTP GET request log spam so terminal output remains clean for strategy events
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Force unbuffered real-time stdout terminal log printing with UTF-8 / safe fallback
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    except Exception:
        pass
if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    except Exception:
        pass

_orig_print = builtins.print
def print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    try:
        _orig_print(*args, **kwargs)
    except UnicodeEncodeError:
        safe_args = [str(a).encode('ascii', errors='replace').decode('ascii') for a in args]
        _orig_print(*safe_args, **kwargs)
builtins.print = print

import json
import re
import time
import math
import asyncio
import uuid
import datetime
import calendar
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List
from flask import Flask, request, jsonify, send_from_directory, Response
import pandas as pd

import csv

# Import NeoAPI client and WebSocket SFeed Protocol
try:
    from neo_api_client import NeoAPI
    from neo_api_client.websocket.feed import SFeedWebSocket, WsToken, SFeedScrip, SFeedScripLite, SFeedIndex
except ImportError:
    NeoAPI = None
    SFeedWebSocket = None
    WsToken = None

# decouple/env reader
try:
    from decouple import config
except ImportError:
    def config(key: str, default: str = "") -> str:
        return os.environ.get(key, default)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DIR = os.path.join(BASE_DIR, "master_scrips")
app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")

# Active NeoAPI client session
SESSION_DATA: Dict[str, Any] = {
    "client": None,
    "user": None
}

# Live WebSocket Option Quotes Store & Token Maps
OPTION_LIVE_PRICES: Dict[str, float] = {}             # TradingSymbol or Token -> Live LTP
OPTION_LIVE_DEPTH: Dict[str, dict] = {}               # TradingSymbol or Token -> {"bid": float, "ask": float, "ltp": float, "updated": float}
SYMBOL_TO_TOKEN_MAP: Dict[str, tuple] = {}            # TradingSymbol -> (Token, Segment)
TOKEN_TO_SYMBOL_MAP: Dict[str, str] = {}              # Token -> TradingSymbol
PENDING_WS_SUBSCRIPTIONS: set = set()                 # Set of (Segment, Token) to subscribe
SUBSCRIBED_WS_TOKENS: set = set()                     # Set of already subscribed (Segment, Token)
MASTER_CONTRACTS_MAP: Dict[tuple, tuple] = {}         # (symbol, exp_code, strike, opt_type) -> (trd_sym, tok, seg)
INDEX_EXPIRIES_MAP: Dict[str, set] = {}               # symbol -> set of expiry datetimes
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}


def load_scrip_token_map(force: bool = False):
    """Loads Master Scrip CSV files into fast memory lookup dictionaries for WebSocket streaming & contract resolution."""
    global SYMBOL_TO_TOKEN_MAP, TOKEN_TO_SYMBOL_MAP, MASTER_CONTRACTS_MAP, INDEX_EXPIRIES_MAP
    if not force and SYMBOL_TO_TOKEN_MAP and MASTER_CONTRACTS_MAP:
        return
    if force:
        SYMBOL_TO_TOKEN_MAP.clear()
        TOKEN_TO_SYMBOL_MAP.clear()
        MASTER_CONTRACTS_MAP.clear()
        INDEX_EXPIRIES_MAP.clear()
    for filename, seg in [("nse_fo.csv", "nse_fo"), ("bse_fo.csv", "bse_fo"), ("nse_cm.csv", "nse_cm"), ("bse_cm.csv", "bse_cm")]:
        path = os.path.join(MASTER_DIR, filename)
        if not os.path.exists(path):
            path = os.path.join(BASE_DIR, filename)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        reader.fieldnames = [c.strip() for c in reader.fieldnames]
                    for row in reader:
                        tok = str(row.get("pSymbol", "")).strip()
                        trd_sym = str(row.get("pTrdSymbol", "")).strip()
                        if tok and trd_sym:
                            SYMBOL_TO_TOKEN_MAP[trd_sym] = (tok, seg)
                            TOKEN_TO_SYMBOL_MAP[tok] = trd_sym

                        sym = str(row.get("pSymbolName", "")).strip().upper()
                        if sym in INDEX_SYMBOLS and seg in ["nse_fo", "bse_fo"]:
                            opt_type = str(row.get("pOptionType", "")).strip().upper()
                            if opt_type in ["CE", "PE"]:
                                ref = str(row.get("pScripRefKey", "")).strip()
                                m = re.search(r'([0-9]{2}[A-Z]{3}[0-9]{2})', ref)
                                if m:
                                    exp_code = m.group(1).upper()
                                    try:
                                        stk_raw = float(row.get("dStrikePrice;", 0) or 0)
                                        stk_val = int(round(stk_raw / 100.0)) if stk_raw > 100000 else int(round(stk_raw))
                                    except (ValueError, TypeError):
                                        stk_val = 0
                                    if stk_val > 0 and tok and trd_sym:
                                        MASTER_CONTRACTS_MAP[(sym, exp_code, stk_val, opt_type)] = (trd_sym, tok, seg)
                                        try:
                                            dt = datetime.datetime.strptime(exp_code, "%d%b%y")
                                            if sym not in INDEX_EXPIRIES_MAP:
                                                INDEX_EXPIRIES_MAP[sym] = set()
                                            INDEX_EXPIRIES_MAP[sym].add(dt)
                                        except Exception:
                                            pass
            except Exception as e:
                print(f"[-] Scrip Map Load Notice ({filename}): {e}")
    print(f"[+] [MASTER SCRIPS LOADED] Registered {len(SYMBOL_TO_TOKEN_MAP)} Tokens, {len(MASTER_CONTRACTS_MAP)} Active F&O Contracts!")


def register_option_for_live_ws(trading_symbol: str, token: str = None, seg: str = None):
    """Registers an option contract trading symbol for dynamic live WebSocket streaming."""
    if not trading_symbol:
        return
    sym = trading_symbol.strip()
    if sym in SYMBOL_TO_TOKEN_MAP:
        tok, s_seg = SYMBOL_TO_TOKEN_MAP[sym]
        PENDING_WS_SUBSCRIPTIONS.add((s_seg, tok))
    elif token and seg:
        PENDING_WS_SUBSCRIPTIONS.add((seg, token))
        SYMBOL_TO_TOKEN_MAP[sym] = (token, seg)
        TOKEN_TO_SYMBOL_MAP[token] = sym


def get_live_market_depth(trading_symbol: str) -> dict:
    """
    Returns {'bid': float, 'ask': float, 'ltp': float} for an option contract or symbol.
    Checks:
    1. OPTION_LIVE_DEPTH (real-time level 4 WebSocket touchline bids & asks)
    2. Fallback to OPTION_LIVE_PRICES for LTP
    """
    if not trading_symbol:
        return {"bid": 0.0, "ask": 0.0, "ltp": 0.0}

    sym = str(trading_symbol).strip()
    depth = OPTION_LIVE_DEPTH.get(sym)
    if not depth and sym in SYMBOL_TO_TOKEN_MAP:
        tok = str(SYMBOL_TO_TOKEN_MAP[sym][0])
        depth = OPTION_LIVE_DEPTH.get(tok)

    if depth:
        return {
            "bid": float(depth.get("bid", 0.0) or 0.0),
            "ask": float(depth.get("ask", 0.0) or 0.0),
            "ltp": float(depth.get("ltp", 0.0) or OPTION_LIVE_PRICES.get(sym, 0.0) or 0.0)
        }

    ltp = float(OPTION_LIVE_PRICES.get(sym, 0.0) or 0.0)
    return {"bid": 0.0, "ask": 0.0, "ltp": ltp}


def sync_master_scrips_in_background(api_client, force: bool = False):
    """Background worker to download latest master CSVs from broker and reload tokens into memory."""
    def _worker():
        try:
            from websocket import download_master_scrips, are_master_scrips_downloaded_today
            if not force and are_master_scrips_downloaded_today():
                today_str = datetime.date.today().strftime("%d-%b-%Y")
                print(f"[+] Master Scrips are already up-to-date for today ({today_str}). Skipping re-download.")
                if not MASTER_CONTRACTS_MAP:
                    load_scrip_token_map()
                return

            print("[*] Downloading latest Master Scrip CSV files in background...")
            downloaded = download_master_scrips(api_client, force=force)
            if downloaded:
                load_scrip_token_map(force=True)
                log_system_event(f"Fresh Master Scrips synchronized ({len(MASTER_CONTRACTS_MAP)} active contracts)!", "SUCCESS")
        except Exception as e:
            print(f"[-] Scrip sync background worker error: {e}")

    threading.Thread(target=_worker, daemon=True).start()


# Load master scrips on module import
load_scrip_token_map()

# TBS Strategy Storage (In-Memory Data Store)
STRATEGIES_STORE: List[Dict[str, Any]] = []

# Real-Time System Execution Logs Store
SYSTEM_LOGS: List[Dict[str, Any]] = []

def log_system_event(msg: str, level: str = "INFO"):
    """Appends an execution event to the in-memory SYSTEM_LOGS store and flushes to stdout."""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    entry = {"timestamp": timestamp, "message": str(msg), "level": level.upper()}
    SYSTEM_LOGS.append(entry)
    if len(SYSTEM_LOGS) > 400:
        SYSTEM_LOGS.pop(0)
    print(f"[{timestamp}] [{level.upper()}] {msg}", flush=True)

# Profile & OMS Settings Data Store
PROFILE_SETTINGS: Dict[str, Any] = {
    "omsLimitOffsetType": "Points (₹)",
    "omsLimitOffsetValue": 0.5,
    "triggerLimitDiff": 1.0,
    "displayTimeMode": "24-Hour Format (e.g. 21:09:15)",
    "displayQtyMode": "Show in Lots (Base)"
}

# Index Lot Sizes Mapping & Helper
INDEX_LOT_SIZES: Dict[str, int] = {
    "NIFTY": 65,
    "BANKNIFTY": 30,
    "FINNIFTY": 60,
    "MIDCPNIFTY": 120,
    "SENSEX": 20,
    "BANKEX": 30
}

def get_index_lot_size(symbol: str) -> int:
    """Returns exact lot size for standard index symbols."""
    symbol_upper = symbol.upper().replace(" 50", "").strip()
    if symbol_upper in INDEX_LOT_SIZES:
        return INDEX_LOT_SIZES[symbol_upper]
    
    csv_path = os.path.join(BASE_DIR, "master_scrips", "bse_fo.csv" if symbol_upper in ["SENSEX", "BANKEX"] else "nse_fo.csv")
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            sub = df[df["pSymbolName"] == symbol_upper]
            if not sub.empty and "lLotSize" in sub.columns:
                return int(sub["lLotSize"].iloc[0])
        except Exception:
            pass
    return 65

# Global memory cache for market data (Spot Cash Indices only)
MARKET_CACHE: Dict[str, Any] = {
    "NIFTY": {
        "symbol": "NIFTY 50",
        "displayName": "NIFTY 50",
        "type": "INDEX",
        "ticker": "^NSEI",
        "price": 0.0,
        "change": 0.0,
        "pChange": 0.0,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "prevClose": 0.0,
        "history": [],
        "lastUpdated": time.time()
    },
    "SENSEX": {
        "symbol": "SENSEX",
        "displayName": "SENSEX",
        "type": "INDEX",
        "ticker": "^BSESN",
        "price": 0.0,
        "change": 0.0,
        "pChange": 0.0,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "prevClose": 0.0,
        "history": [],
        "lastUpdated": time.time()
    }
}

def load_initial_live_market_quotes():
    """Fetches real live market quote snapshot on startup from market chart API."""
    import httpx
    for sym, ticker in [("NIFTY", "^NSEI"), ("SENSEX", "^BSESN")]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d"
            headers = {"User-Agent": "Mozilla/5.0"}
            r = httpx.get(url, headers=headers, timeout=3.5)
            if r.status_code == 200:
                meta = r.json()["chart"]["result"][0]["meta"]
                p = float(meta.get("regularMarketPrice", 0.0))
                prev = float(meta.get("chartPreviousClose", meta.get("previousClose", p)))
                if p > 0:
                    chg = round(p - prev, 2)
                    pct = round((chg / prev) * 100, 2) if prev > 0 else 0.0
                    MARKET_CACHE[sym].update({
                        "price": round(p, 2),
                        "change": chg,
                        "pChange": pct,
                        "prevClose": round(prev, 2),
                        "lastUpdated": time.time()
                    })
        except Exception:
            pass

try:
    load_initial_live_market_quotes()
except Exception:
    pass


def calculate_dte(expiry_str: str) -> int:
    """Calculates integer days to expiry (DTE) from an expiry date string."""
    if not expiry_str:
        return 0
    today = datetime.date.today()
    cleaned = str(expiry_str).strip()
    dt = None
    for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d%b%Y", "%d-%B-%Y"]:
        try:
            dt = datetime.datetime.strptime(cleaned, fmt).date()
            break
        except Exception:
            continue
    if dt:
        return max(0, (dt - today).days)
    return 0


# In-memory short-term cache for live broker expiries (5 min TTL)
LIVE_INDEX_EXPIRIES_CACHE: Dict[str, Dict[str, Any]] = {}

def get_index_expiries(index_symbol: str) -> List[Dict[str, Any]]:
    """
    Returns all actual tradeable expiry dates for the selected index or contract
    extracted from live Kotak Neo API (client.expiries) or in-memory Master Scrips lookup.
    """
    symbol_upper = index_symbol.upper().replace(" 50", "").strip()
    today = datetime.date.today()
    now_ts = time.time()

    # 1. Check in-memory 5-minute cache
    cached = LIVE_INDEX_EXPIRIES_CACHE.get(symbol_upper)
    if cached and (now_ts - cached.get("timestamp", 0) < 300):
        return cached.get("data", [])

    # 2. Check live Kotak Neo broker API via dedicated client.expiries()
    client = SESSION_DATA.get("client")
    if client is not None:
        try:
            exchange_seg = "bse_fo" if symbol_upper in ["SENSEX", "BANKEX"] else "nse_fo"
            res = client.expiries(exchange=exchange_seg, underlying=symbol_upper)
            if isinstance(res, dict) and "expiries" in res and res["expiries"]:
                valid_expiries = []
                for exp_str in res["expiries"]:
                    try:
                        exp_dt = datetime.datetime.strptime(exp_str.strip(), "%Y-%m-%d")
                        if exp_dt.date() >= today:
                            valid_expiries.append(exp_dt)
                            # Register in INDEX_EXPIRIES_MAP so contract lookup recognizes it
                            if symbol_upper not in INDEX_EXPIRIES_MAP:
                                INDEX_EXPIRIES_MAP[symbol_upper] = set()
                            INDEX_EXPIRIES_MAP[symbol_upper].add(exp_dt)
                    except Exception:
                        continue

                if valid_expiries:
                    valid_expiries.sort()
                    formatted = []
                    for dt in valid_expiries:
                        dte = (dt.date() - today).days
                        date_str = dt.strftime("%d-%b-%Y").upper()
                        formatted.append({
                            "label": f"{date_str} ({dte} DTE)",
                            "date": date_str,
                            "dte": dte,
                            "dte_label": f"{dte} DTE"
                        })
                    LIVE_INDEX_EXPIRIES_CACHE[symbol_upper] = {"timestamp": now_ts, "data": formatted}
                    return formatted
        except Exception as e:
            print(f"[-] Kotak client.expiries notice for {symbol_upper}: {e}")

    # 3. Check in-memory INDEX_EXPIRIES_MAP with gap detection
    dates_set = set(INDEX_EXPIRIES_MAP.get(symbol_upper, set()))
    future_dates = sorted([dt for dt in dates_set if dt.date() >= today])

    # If the nearest future date is within 7 days, use the master scrip dates
    if future_dates and (future_dates[0].date() - today).days <= 7:
        result = []
        for dt in future_dates:
            dte = (dt.date() - today).days
            date_str = dt.strftime("%d-%b-%Y").upper()
            result.append({
                "label": f"{date_str} ({dte} DTE)",
                "date": date_str,
                "dte": dte,
                "dte_label": f"{dte} DTE"
            })
        return result

    # 4. Fallback: Dynamic calendar-based weekly expiry generation
    weekday_map = {"MIDCPNIFTY": 0, "FINNIFTY": 1, "BANKNIFTY": 2, "NIFTY": 3, "SENSEX": 4, "BANKEX": 0}
    target_dow = weekday_map.get(symbol_upper, 3)

    days_ahead = (target_dow - today.weekday()) % 7
    if days_ahead == 0 and datetime.datetime.now().hour >= 15:
        days_ahead += 7

    curr_week = today + datetime.timedelta(days=days_ahead)
    next_week = curr_week + datetime.timedelta(weeks=1)

    curr_dte = (curr_week - today).days
    next_dte = (next_week - today).days
    curr_str = curr_week.strftime("%d-%b-%Y").upper()
    next_str = next_week.strftime("%d-%b-%Y").upper()

    fallback_result = [
        {"label": f"{curr_str} ({curr_dte} DTE)", "date": curr_str, "dte": curr_dte, "dte_label": f"{curr_dte} DTE"},
        {"label": f"{next_str} ({next_dte} DTE)", "date": next_str, "dte": next_dte, "dte_label": f"{next_dte} DTE"}
    ]
    # Append any remaining future dates from master scrip
    for dt in future_dates:
        date_str = dt.strftime("%d-%b-%Y").upper()
        if date_str not in [curr_str, next_str]:
            dte = (dt.date() - today).days
            fallback_result.append({
                "label": f"{date_str} ({dte} DTE)",
                "date": date_str,
                "dte": dte,
                "dte_label": f"{dte} DTE"
            })

    return fallback_result





def start_ws_index_listener(client):
    """Subscribes Kotak Neo SFeedWebSocket to live index tokens and dynamic option contracts for real-time market updates."""
    if client is None or SFeedWebSocket is None or WsToken is None:
        return

    async def ws_loop():
        while True:
            try:
                # Kotak Neo supports index subscription by official index name as well as token ID
                index_tokens = [
                    WsToken("nse_cm", "Nifty 50"),
                    WsToken("nse_cm", "26000"),
                    WsToken("bse_cm", "SENSEX"),
                    WsToken("bse_cm", "1")
                ]
                for t in index_tokens:
                    SUBSCRIBED_WS_TOKENS.add((t.exchange_segment, t.instrument_token))

                async with client.create_websocket() as ws:
                    # 1. Subscribe via subscribe_index (triggers subscribeIndices event)
                    try:
                        await ws.subscribe_index(index_tokens)
                        print("[+] [WS WEBSOCKET FEED] Subscribed Index tokens (Nifty 50, SENSEX) via subscribe_index!", flush=True)
                    except Exception as ie:
                        print(f"[-] [WS] subscribe_index notice: {ie}", flush=True)

                    # 2. Also subscribe via subscribe_scrips to cover all broker feed routing configurations
                    try:
                        await ws.subscribe_scrips(index_tokens)
                        print("[+] [WS WEBSOCKET FEED] Subscribed Index tokens (Nifty 50, SENSEX) via subscribe_scrips!", flush=True)
                    except Exception as se:
                        pass

                    async def dynamic_subscriber():
                        while True:
                            try:
                                await asyncio.sleep(0.5)
                                pending = [t for t in list(PENDING_WS_SUBSCRIPTIONS) if t not in SUBSCRIBED_WS_TOKENS]
                                if pending:
                                    new_tokens = [WsToken(seg, tok) for seg, tok in pending]
                                    await ws.subscribe_scrips(new_tokens)
                                    for p in pending:
                                        SUBSCRIBED_WS_TOKENS.add(p)
                                    print(f"[+] [DYNAMIC WS SUB] Subscribed {len(pending)} new option contract tokens to WebSocket feed!", flush=True)
                            except Exception:
                                pass

                    asyncio.create_task(dynamic_subscriber())

                    async for message in ws:
                        tok = ""
                        name = ""
                        ltp = 0.0
                        chg = 0.0
                        chg_pct = 0.0
                        open_p = 0.0
                        high_p = 0.0
                        low_p = 0.0
                        close_p = 0.0
                        best_bid = 0.0
                        best_ask = 0.0

                        if SFeedIndex is not None and isinstance(message, SFeedIndex):
                            tok = str(message.instrument_token or "")
                            name = str(message.name or "")
                            ltp = float(message.last_traded_price or 0.0)
                            chg = float(message.change or 0.0)
                            chg_pct = float(message.net_change_percent or 0.0)
                            open_p = float(message.open_price or ltp)
                            high_p = float(message.high_price or ltp)
                            low_p = float(message.low_price or ltp)
                            close_p = float(message.close_price or ltp)
                        elif isinstance(message, (SFeedScrip, SFeedScripLite)):
                            tok = str(message.instrument_token or "")
                            name = str(getattr(message, "trading_symbol", "") or "")
                            ltp = float(message.last_traded_price or 0.0)
                            chg = float(getattr(message, "net_change", 0.0) or getattr(message, "change", 0.0) or 0.0)
                            chg_pct = float(getattr(message, "net_change_percent", 0.0) or 0.0)
                            open_p = float(getattr(message, "open_price", ltp) or ltp)
                            high_p = float(getattr(message, "high_price", ltp) or ltp)
                            low_p = float(getattr(message, "low_price", ltp) or ltp)
                            close_p = float(getattr(message, "close_price", ltp) or ltp)
                            if hasattr(message, "buy") and message.buy:
                                try:
                                    best_bid = float(getattr(message.buy[0], "price", 0.0) or 0.0)
                                except Exception:
                                    pass
                            if hasattr(message, "sell") and message.sell:
                                try:
                                    best_ask = float(getattr(message.sell[0], "price", 0.0) or 0.0)
                                except Exception:
                                    pass
                        elif isinstance(message, dict):
                            tok = str(message.get("tk") or message.get("instrument_token") or "")
                            name = str(message.get("trading_symbol") or message.get("tsym") or message.get("name") or "")
                            ltp = float(message.get("ltp") or message.get("last_traded_price") or message.get("iv") or 0.0)
                            chg = float(message.get("change") or message.get("nc") or message.get("net_change") or 0.0)
                            chg_pct = float(message.get("net_change_percent") or message.get("per") or 0.0)
                            open_p = float(message.get("open") or message.get("open_price") or ltp)
                            high_p = float(message.get("high") or message.get("high_price") or ltp)
                            low_p = float(message.get("low") or message.get("low_price") or ltp)
                            close_p = float(message.get("close") or message.get("close_price") or ltp)
                            buy_list = message.get("buy") or message.get("depth", {}).get("buy") or []
                            if buy_list and isinstance(buy_list, list) and len(buy_list) > 0:
                                first_b = buy_list[0]
                                best_bid = float(first_b.get("price", 0.0) if isinstance(first_b, dict) else getattr(first_b, "price", 0.0))
                            elif message.get("bp") or message.get("bPrice") or message.get("buy_price"):
                                best_bid = float(message.get("bp") or message.get("bPrice") or message.get("buy_price") or 0.0)

                            sell_list = message.get("sell") or message.get("depth", {}).get("sell") or []
                            if sell_list and isinstance(sell_list, list) and len(sell_list) > 0:
                                first_s = sell_list[0]
                                best_ask = float(first_s.get("price", 0.0) if isinstance(first_s, dict) else getattr(first_s, "price", 0.0))
                            elif message.get("sp") or message.get("sPrice") or message.get("sell_price"):
                                best_ask = float(message.get("sp") or message.get("sPrice") or message.get("sell_price") or 0.0)
                        else:
                            tok = str(getattr(message, "instrument_token", "") or getattr(message, "token", "") or "")
                            name = str(getattr(message, "trading_symbol", "") or getattr(message, "name", "") or "")
                            ltp = float(getattr(message, "last_traded_price", 0.0) or getattr(message, "ltp", 0.0) or 0.0)
                            chg = float(getattr(message, "change", 0.0) or getattr(message, "net_change", 0.0) or 0.0)
                            chg_pct = float(getattr(message, "net_change_percent", 0.0) or 0.0)
                            open_p = float(getattr(message, "open_price", ltp) or ltp)
                            high_p = float(getattr(message, "high_price", ltp) or ltp)
                            low_p = float(getattr(message, "low_price", ltp) or ltp)
                            close_p = float(getattr(message, "close_price", ltp) or ltp)
                            buy_attr = getattr(message, "buy", None)
                            if buy_attr and len(buy_attr) > 0:
                                best_bid = float(getattr(buy_attr[0], "price", 0.0) or 0.0)
                            elif hasattr(message, "buy_price") or hasattr(message, "bp"):
                                best_bid = float(getattr(message, "buy_price", 0.0) or getattr(message, "bp", 0.0) or 0.0)
                            sell_attr = getattr(message, "sell", None)
                            if sell_attr and len(sell_attr) > 0:
                                best_ask = float(getattr(sell_attr[0], "price", 0.0) or 0.0)
                            elif hasattr(message, "sell_price") or hasattr(message, "sp"):
                                best_ask = float(getattr(message, "sell_price", 0.0) or getattr(message, "sp", 0.0) or 0.0)

                        if ltp > 0:
                            tok_str = str(tok).strip()
                            name_upper = str(name).strip().upper()

                            # 1. NIFTY 50 SPOT INDEX (Token: 26000 or Name: Nifty 50)
                            if tok_str == "26000" or ("NIFTY" in name_upper and "BANK" not in name_upper and "FIN" not in name_upper and "MID" not in name_upper and "FUT" not in name_upper and "CE" not in name_upper and "PE" not in name_upper):
                                MARKET_CACHE["NIFTY"].update({
                                    "price": round(ltp, 2),
                                    "change": round(chg, 2),
                                    "pChange": round(chg_pct, 2),
                                    "open": round(open_p, 2),
                                    "high": round(high_p, 2),
                                    "low": round(low_p, 2),
                                    "prevClose": round(close_p, 2),
                                    "lastUpdated": time.time()
                                })

                            # 2. SENSEX SPOT INDEX (Token: 1 or Name: SENSEX)
                            elif tok_str == "1" or ("SENSEX" in name_upper and "50" not in name_upper and "FUT" not in name_upper and "CE" not in name_upper and "PE" not in name_upper):
                                MARKET_CACHE["SENSEX"].update({
                                    "price": round(ltp, 2),
                                    "change": round(chg, 2),
                                    "pChange": round(chg_pct, 2),
                                    "open": round(open_p, 2),
                                    "high": round(high_p, 2),
                                    "low": round(low_p, 2),
                                    "prevClose": round(close_p, 2),
                                    "lastUpdated": time.time()
                                })

                            # Periodic 60s Market Heartbeat in Terminal (instead of spamming every millisecond)
                            _now_ts = time.time()
                            if _now_ts - MARKET_CACHE["NIFTY"].get("_last_hb_ts", 0) >= 60:
                                MARKET_CACHE["NIFTY"]["_last_hb_ts"] = _now_ts
                                n_p = MARKET_CACHE["NIFTY"]["price"]
                                n_c = MARKET_CACHE["NIFTY"]["change"]
                                s_p = MARKET_CACHE["SENSEX"]["price"]
                                print(f"[MARKET FEED] NIFTY 50: ₹{n_p:,.2f} ({n_c:+.2f}) | SENSEX: ₹{s_p:,.2f}", flush=True)

                            # Update Option Live Prices & Level 4 Depth Cache
                            depth_data = {
                                "ltp": round(ltp, 2),
                                "bid": round(best_bid, 2),
                                "ask": round(best_ask, 2),
                                "updated": time.time()
                            }
                            if tok_str:
                                OPTION_LIVE_PRICES[tok_str] = round(ltp, 2)
                                OPTION_LIVE_DEPTH[tok_str] = depth_data
                            if name:
                                OPTION_LIVE_PRICES[name] = round(ltp, 2)
                                OPTION_LIVE_DEPTH[name] = depth_data
                            sym_mapped = TOKEN_TO_SYMBOL_MAP.get(tok_str)
                            if sym_mapped:
                                OPTION_LIVE_PRICES[sym_mapped] = round(ltp, 2)
                                OPTION_LIVE_DEPTH[sym_mapped] = depth_data

                            # Also map strike number if present in symbol name
                            for s_name in [sym_mapped, name]:
                                if s_name:
                                    s_upper = s_name.upper()
                                    if "CE" in s_upper or "PE" in s_upper:
                                        m = re.search(r'(\d+)(CE|PE)$', s_upper)
                                        if m:
                                            try:
                                                stk_val = str(int(m.group(1)))
                                                OPTION_LIVE_PRICES[stk_val] = round(ltp, 2)
                                                OPTION_LIVE_PRICES[f"{stk_val}_{m.group(2)}"] = round(ltp, 2)
                                                OPTION_LIVE_DEPTH[stk_val] = depth_data
                                                OPTION_LIVE_DEPTH[f"{stk_val}_{m.group(2)}"] = depth_data
                                            except Exception:
                                                pass

            except Exception as e:
                print(f"[-] Index WS Streaming Notice: {e}", flush=True)
                await asyncio.sleep(2)



    def run_async():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(ws_loop())
        except Exception as e:
            print(f"[-] Index WS Loop Notice: {e}")

    t = threading.Thread(target=run_async, daemon=True)
    t.start()





@app.route("/")
def index():
    """Serve main HTML page."""
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/env-credentials", methods=["GET"])
def get_env_credentials():
    """Pre-fills form credentials from .env if available."""
    return jsonify({
        "consumerKey": config("NEO_CONSUMER_KEY", default=""),
        "mobileNumber": config("NEO_MOBILE_NUMBER", default=""),
        "ucc": config("NEO_UCC", default=""),
        "mpin": config("NEO_MPIN", default="")
    })


@app.route("/api/login", methods=["POST"])
def login_route():
    """User login endpoint."""
    data = request.get_json() or {}
    
    consumer_key = data.get("consumerKey", "").strip()
    mobile_number = data.get("mobileNumber", "").strip()
    ucc = data.get("ucc", "").strip()
    mpin = data.get("mpin", "").strip()
    totp = data.get("totp", "").strip()
    username = data.get("username", "").strip()

    if mobile_number and not mobile_number.startswith("+"):
        if len(mobile_number) == 10 and mobile_number.isdigit():
            mobile_number = "+91" + mobile_number

    if consumer_key and mobile_number and ucc and mpin and totp and NeoAPI is not None:
        try:
            print(f"[*] Attempting Kotak Neo Authentication for UCC: {ucc}...")
            client = NeoAPI(consumer_key=consumer_key, environment="prod")
            
            login_res = client.totp_login(mobile_number=mobile_number, ucc=ucc, totp=totp)
            if isinstance(login_res, dict) and "error" in login_res:
                err = login_res["error"]
                msg = err[0].get("message") if isinstance(err, list) and err else str(err)
                return jsonify({"success": False, "message": f"TOTP Login Error: {msg}"}), 400

            validate_res = client.totp_validate(mpin=mpin)
            if isinstance(validate_res, dict) and "error" in validate_res:
                err = validate_res["error"]
                msg = err[0].get("message") if isinstance(err, list) and err else str(err)
                return jsonify({"success": False, "message": f"MPIN Validation Error: {msg}"}), 400

            greeting_name = "Trader"
            if isinstance(validate_res, dict) and "data" in validate_res:
                greeting_name = validate_res["data"].get("greetingName", ucc)

            SESSION_DATA["client"] = client
            SESSION_DATA["user"] = {"displayName": greeting_name, "ucc": ucc}

            # Start real-time Kotak Neo WebSocket live index streaming
            try:
                start_ws_index_listener(client)
            except Exception as e:
                print(f"[-] Index WS Launch Notice: {e}")

            # Download fresh master scrip files from Kotak Neo broker server in background & reload
            sync_master_scrips_in_background(client)

            return jsonify({
                "success": True,
                "message": "Kotak Neo API Login Successful!",
                "user": {
                    "username": ucc,
                    "displayName": greeting_name,
                    "ucc": ucc,
                    "loginTime": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            })

        except Exception as e:
            print(f"[-] Kotak Neo Authentication Exception: {e}")
            return jsonify({"success": False, "message": f"Kotak Neo Authentication Failed: {str(e)}"}), 400

    display_user = username or ucc or "Trader"
    display_name = display_user.title() if len(display_user) > 1 else display_user

    return jsonify({
        "success": True,
        "message": "Login Successful (Demo Session)",
        "user": {
            "username": display_user,
            "displayName": display_name,
            "ucc": display_user.upper(),
            "loginTime": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    })


@app.route("/api/broker/status", methods=["GET"])
def get_broker_status():
    """Returns current broker login status and session details."""
    user = SESSION_DATA.get("user")
    client = SESSION_DATA.get("client")
    
    if user:
        return jsonify({
            "status": "success",
            "isLoggedIn": True,
            "broker": "Kotak Neo",
            "username": user.get("displayName", user.get("username", "Trader")),
            "ucc": user.get("ucc", "N/A"),
            "loginTime": user.get("loginTime", time.strftime("%Y-%m-%d %H:%M:%S")),
            "environment": "Production" if client else "Demo Mode",
            "connectionStatus": "LOGGED IN",
            "scripMasterStatus": "Synchronized" if client else "Demo Scrips"
        })
    else:
        return jsonify({
            "status": "success",
            "isLoggedIn": False,
            "broker": "Kotak Neo",
            "username": "Not Logged In",
            "ucc": "N/A",
            "loginTime": "N/A",
            "environment": "N/A",
            "connectionStatus": "DISCONNECTED",
            "scripMasterStatus": "Pending Login"
        })


# -------------------------------------------------------------------
# PROFILE & OMS LIMIT ORDER SETTINGS ENDPOINTS
# -------------------------------------------------------------------

def calculate_limit_order_price(
    ltp: float,
    action: str,
    trading_symbol: str = "",
    best_bid: float = 0.0,
    best_ask: float = 0.0
) -> float:
    """
    Calculates entry price for a Limit Order (order_type='L') via Kotak Neo using
    Approach 1: Best Bid / Best Ask Pricing (Industry Standard).

    Formula & Logic:
    - SELL order: Marketable against buyers (Best Bid).
      Price = (Best Bid - entry_limit_offset) if Best Bid > 0 else (LTP - entry_limit_offset).
      This guarantees the limit sell order crosses the spread and matches immediately with the
      highest available buyer in the order book, preventing unfilled orders in illiquid strikes.
    - BUY order: Marketable against sellers (Best Ask).
      Price = (Best Ask + entry_limit_offset) if Best Ask > 0 else (LTP + entry_limit_offset).
      This guarantees the limit buy order crosses the spread and matches immediately with the
      lowest available seller in the order book.
    - Exchange Tick Size: Strictly aligned to 0.05 multiples.
    """
    offset_type = PROFILE_SETTINGS.get("omsLimitOffsetType", "Points (₹)")
    offset_val = float(PROFILE_SETTINGS.get("omsLimitOffsetValue", 0.5))

    # Resolve live bid / ask from Level 4 WebSocket depth if not explicitly passed
    if (best_bid <= 0 or best_ask <= 0) and trading_symbol:
        depth = get_live_market_depth(trading_symbol)
        if best_bid <= 0:
            best_bid = float(depth.get("bid", 0.0) or 0.0)
        if best_ask <= 0:
            best_ask = float(depth.get("ask", 0.0) or 0.0)
        if ltp <= 0:
            ltp = float(depth.get("ltp", 0.0) or 0.0)

    action_upper = action.strip().upper()
    is_buy = action_upper in ["BUY", "B"]

    # Select base price from order book depth
    if is_buy:
        base_price = best_ask if best_ask > 0 else ltp
    else:
        base_price = best_bid if best_bid > 0 else ltp

    if base_price <= 0:
        base_price = ltp if ltp > 0 else 0.05

    if "Percent" in offset_type:
        calc_offset = base_price * (offset_val / 100.0)
    else:
        calc_offset = offset_val

    if is_buy:
        price = base_price + calc_offset
    else:
        price = base_price - calc_offset

    price = max(0.05, price)
    tick_aligned = round(round(price / 0.05) * 0.05, 2)
    return max(0.05, tick_aligned)


def calculate_exit_limit_order_price(
    ltp: float,
    close_action: str,
    trading_symbol: str = "",
    best_bid: float = 0.0,
    best_ask: float = 0.0
) -> float:
    """
    Calculates Limit Price for Exit / Square Off order (order_type='L') using
    Approach 1: Best Bid / Best Ask Pricing (Industry Standard).

    Formula & Logic:
    - To close short (BUY): Marketable against Best Ask (lowest seller).
      Price = (Best Ask + offset) if Best Ask > 0 else (LTP + offset).
    - To close long (SELL): Marketable against Best Bid (highest buyer).
      Price = (Best Bid - offset) if Best Bid > 0 else (LTP - offset).
    - Exchange Tick Size: Strictly aligned to 0.05 multiples.
    """
    offset_type = PROFILE_SETTINGS.get("omsLimitOffsetType", "Points (₹)")
    offset_val = float(PROFILE_SETTINGS.get("omsLimitOffsetValue", 0.5))

    if (best_bid <= 0 or best_ask <= 0) and trading_symbol:
        depth = get_live_market_depth(trading_symbol)
        if best_bid <= 0:
            best_bid = float(depth.get("bid", 0.0) or 0.0)
        if best_ask <= 0:
            best_ask = float(depth.get("ask", 0.0) or 0.0)
        if ltp <= 0:
            ltp = float(depth.get("ltp", 0.0) or 0.0)

    act = close_action.strip().upper()
    is_buy = act in ["BUY", "B"]

    if is_buy:
        base_price = best_ask if best_ask > 0 else ltp
    else:
        base_price = best_bid if best_bid > 0 else ltp

    if base_price <= 0:
        base_price = ltp if ltp > 0 else 0.05

    if "Percent" in offset_type:
        calc_offset = base_price * (offset_val / 100.0)
    else:
        calc_offset = offset_val

    if is_buy:
        price = base_price + calc_offset
    else:
        price = base_price - calc_offset

    price = max(0.05, price)
    tick_aligned = round(round(price / 0.05) * 0.05, 2)
    return max(0.05, tick_aligned)


def calculate_sl_prices(entry_price: float, leg_action: str, sl_val: float, sl_type: str, trigger_limit_diff: float = 1.0, tick_size: float = 0.05) -> tuple:
    """
    Calculates (sl_trigger, sl_limit, sl_tx_type) for a matching Stop-Loss order (order_type='SL'):
    - Percentage: Entry Price +/- (Entry Price * SL%)
    - Points: Entry Price +/- SL Points
    - A limit margin is added above (for SELL) / below (for BUY) trigger price (trigger_limit_diff)
      to guarantee immediate fill even in volatile market conditions.
    - All prices are strictly aligned to the broker's 0.05 tick size.
    """
    is_short = str(leg_action).strip().upper() in ["SELL", "S"]
    sl_v = float(sl_val or 0)

    if "Percent" in str(sl_type) or "%" in str(sl_type):
        sl_points = entry_price * (sl_v / 100.0)
    else:
        sl_points = sl_v

    if is_short:
        # Short position: SL triggers when market rises.
        # SL order is a BUY (B) to close short.
        sl_trigger = entry_price + sl_points
        sl_limit = sl_trigger + float(trigger_limit_diff or 1.0)
        sl_tx_type = "B"
    else:
        # Long position: SL triggers when market drops.
        # SL order is a SELL (S) to close long.
        sl_trigger = entry_price - sl_points
        sl_limit = max(0.05, sl_trigger - float(trigger_limit_diff or 1.0))
        sl_tx_type = "S"

    # Align to 0.05 tick size
    sl_trigger = round(round(max(0.05, sl_trigger) / tick_size) * tick_size, 2)
    sl_limit = round(round(max(0.05, sl_limit) / tick_size) * tick_size, 2)

    return sl_trigger, sl_limit, sl_tx_type


INDEX_STRIKE_STEPS: Dict[str, int] = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "SENSEX": 100,
    "BANKEX": 100
}

# -------------------------------------------------------------------
# PRE-WARMING ENGINE & OPTION CHAIN STRIKE RESOLVER (250ms Fast Loop)
# -------------------------------------------------------------------

PREWARM_STORE: Dict[str, Dict[str, Any]] = {}
OPTION_CHAIN_CACHE: Dict[str, Dict[str, Any]] = {}
LAST_BROKER_POSITIONS_CACHE: Dict[str, Any] = {"timestamp": 0, "positions": {}}

def get_cached_broker_positions(client, max_age_sec: float = 6.0) -> Dict[str, Any]:
    """Returns cached broker positions if fresh (< max_age_sec), or fetches fresh ones from client."""
    now_ts = time.time()
    if client is not None and (now_ts - LAST_BROKER_POSITIONS_CACHE.get("timestamp", 0) > max_age_sec):
        try:
            pos_res = client.positions()
            pos_data = pos_res.get("data") if isinstance(pos_res, dict) and isinstance(pos_res.get("data"), list) else (pos_res if isinstance(pos_res, list) else [])
            pos_map = {}
            for p in pos_data:
                if isinstance(p, dict):
                    p_sym = str(p.get("trdSym") or "").strip()
                    p_tok = str(p.get("tok") or "").strip()
                    buy_q = int(float(p.get("cfBuyQty", 0) or 0)) + int(float(p.get("flBuyQty", 0) or 0))
                    sell_q = int(float(p.get("cfSellQty", 0) or 0)) + int(float(p.get("flSellQty", 0) or 0))
                    net_q = buy_q - sell_q
                    if "netQty" in p and p.get("netQty") is not None:
                        try:
                            net_q = int(float(p["netQty"]))
                        except Exception:
                            pass
                    p["computedNetQty"] = net_q
                    if p_sym:
                        pos_map[p_sym] = p
                    if p_tok:
                        pos_map[p_tok] = p
            LAST_BROKER_POSITIONS_CACHE["timestamp"] = now_ts
            LAST_BROKER_POSITIONS_CACHE["positions"] = pos_map
        except Exception:
            pass
    return LAST_BROKER_POSITIONS_CACHE.get("positions", {})

def norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def calculate_option_theoretical_price(spot: float, strike: float, dte_days: float, opt_type: str = "CE", symbol: str = "NIFTY") -> float:
    """
    Calculates standard Black-Scholes theoretical price for candidate strike.
    Provides realistic option premium across ITM, ATM, and OTM strikes when live quotes are pending.
    """
    if spot <= 0 or strike <= 0:
        return 10.0
    iv = max(0.0, spot - strike) if opt_type == "CE" else max(0.0, strike - spot)
    T = max(0.25, dte_days) / 365.0
    r = 0.07  # 7% risk free rate
    sigma = 0.14 if symbol in ["SENSEX", "BANKEX"] else 0.135
    try:
        d1 = (math.log(spot / strike) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        if opt_type == "CE":
            bs_price = spot * norm_cdf(d1) - strike * math.exp(-r * T) * norm_cdf(d2)
        else:
            bs_price = strike * math.exp(-r * T) * norm_cdf(-d2) - spot * norm_cdf(-d1)
        return round(max(0.05, bs_price), 2)
    except Exception:
        return round(max(0.05, iv + 150.0 * (0.85 ** (abs(spot - strike) / 50.0))), 2)


def resolve_expiry_details(symbol_upper: str, expiry_str: str = None) -> tuple:
    """
    Resolves (exp_code, iso_expiry, exp_datetime, display_expiry).
    e.g. ('06OCT26', '2026-10-06', datetime(...), '06-OCT-2026')
    """
    today = datetime.date.today()
    dt = None
    if expiry_str:
        clean_exp = str(expiry_str).strip()
        for fmt in ["%d-%b-%Y", "%d-%b-%y", "%d%b%y", "%d%b%Y", "%Y-%m-%d"]:
            try:
                dt = datetime.datetime.strptime(clean_exp, fmt)
                break
            except Exception:
                continue

    # Fallback to nearest future active tradeable expiry if dt is None or past
    if not dt or dt.date() < today:
        future_expiries = sorted([d for d in INDEX_EXPIRIES_MAP.get(symbol_upper, set()) if d.date() >= today])
        if future_expiries:
            dt = future_expiries[0]
        else:
            days_ahead = (3 - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            target_date = today + datetime.timedelta(days=days_ahead)
            dt = datetime.datetime.combine(target_date, datetime.time())

    exp_code = dt.strftime("%d%b%y").upper()
    iso_expiry = dt.strftime("%Y-%m-%d")
    display_expiry = dt.strftime("%d-%b-%Y").upper()
    return exp_code, iso_expiry, dt, display_expiry


def get_option_chain_candidates(sym_base: str, opt_type: str, strategy_expiry: str = None) -> List[Dict[str, Any]]:
    """
    Fetches / resolves full option chain candidates around spot LTP for index & option type.
    Uses real live WebSocket ticks (OPTION_LIVE_PRICES), REST option chain API, and realistic pricing.
    """
    symbol_upper = sym_base.upper().replace(" 50", "").strip()
    spot_ltp = 22716.20 if "SENSEX" not in symbol_upper else 79500.00
    if "NIFTY" in symbol_upper and MARKET_CACHE.get("NIFTY") and MARKET_CACHE["NIFTY"]["price"] > 0:
        spot_ltp = MARKET_CACHE["NIFTY"]["price"]
    elif "SENSEX" in symbol_upper and MARKET_CACHE.get("SENSEX") and MARKET_CACHE["SENSEX"]["price"] > 0:
        spot_ltp = MARKET_CACHE["SENSEX"]["price"]

    step = INDEX_STRIKE_STEPS.get(symbol_upper, 50)
    atm = int(round(spot_ltp / step) * step)
    opt_type_code = "CE" if opt_type.title() in ["Call", "CE"] else "PE"

    exp_code, iso_expiry, exp_dt, display_expiry = resolve_expiry_details(symbol_upper, strategy_expiry)
    today = datetime.date.today()
    dte_days = max(0.5, (exp_dt.date() - today).days)

    client = SESSION_DATA.get("client")
    chain_quotes = {}
    base_cache_key = f"{symbol_upper}_{iso_expiry}"
    now_ts = time.time()

    # Unified 30-Second Cache for both CE and PE + 10-Second Cooldown on broker 429/errors
    cached_entry = OPTION_CHAIN_CACHE.get(base_cache_key)
    use_cache = False
    if cached_entry:
        age = now_ts - cached_entry.get("timestamp", 0)
        if cached_entry.get("cooldown", False):
            if age < 10.0:
                use_cache = True
        elif age < 30.0:
            use_cache = True

    if use_cache and cached_entry:
        chain_quotes = cached_entry.get(opt_type_code, {})
    elif client:
        try:
            exch = "bse_fo" if symbol_upper in ["SENSEX", "BANKEX"] else "nse_fo"
            res = client.option_chain(exchange=exch, underlying=symbol_upper, expiry=iso_expiry, count=40)
            if isinstance(res, dict) and "data" in res:
                c_data = res["data"]
                ce_quotes = {}
                pe_quotes = {}

                for call_item in c_data.get("call", []):
                    inst = call_item.get("inst") or call_item.get("instrument") or {}
                    quote = call_item.get("quote") or {}
                    stk_raw = float(inst.get("strkPrc") or inst.get("strikePrice") or inst.get("pStrikePrice") or inst.get("dStrikePrice") or 0)
                    if stk_raw > 100000:
                        stk_raw = stk_raw / 100.0
                    ltp = float(quote.get("ltp") or quote.get("lastPrice") or 0)
                    b_bid = float(quote.get("bp") or quote.get("bPrice") or quote.get("buyPrice") or 0.0)
                    b_ask = float(quote.get("sp") or quote.get("sPrice") or quote.get("sellPrice") or 0.0)
                    tok = str(inst.get("pSymbol") or inst.get("pTok") or inst.get("tok") or inst.get("instrument_token") or "").strip()
                    trd_sym = str(inst.get("pSymbolName") or inst.get("pTrdSymbol") or "").strip()
                    if not trd_sym:
                        matched = MASTER_CONTRACTS_MAP.get((symbol_upper, exp_code, int(stk_raw), "CE"))
                        trd_sym = matched[0] if matched else f"{symbol_upper}{exp_code}{int(stk_raw)}CE"

                    if stk_raw > 0 and ltp > 0:
                        ce_quotes[int(stk_raw)] = ltp
                        OPTION_LIVE_PRICES[trd_sym] = ltp
                        OPTION_LIVE_PRICES[str(int(stk_raw))] = ltp
                        depth_item = {
                            "ltp": round(ltp, 2),
                            "bid": round(b_bid, 2),
                            "ask": round(b_ask, 2),
                            "updated": time.time()
                        }
                        OPTION_LIVE_DEPTH[trd_sym] = depth_item
                        if tok:
                            TOKEN_TO_SYMBOL_MAP[tok] = trd_sym
                            OPTION_LIVE_DEPTH[tok] = depth_item
                            register_option_for_live_ws(trd_sym, tok, exch)

                for put_item in c_data.get("put", []):
                    inst = put_item.get("inst") or put_item.get("instrument") or {}
                    quote = put_item.get("quote") or {}
                    stk_raw = float(inst.get("strkPrc") or inst.get("strikePrice") or inst.get("pStrikePrice") or inst.get("dStrikePrice") or 0)
                    if stk_raw > 100000:
                        stk_raw = stk_raw / 100.0
                    ltp = float(quote.get("ltp") or quote.get("lastPrice") or 0)
                    b_bid = float(quote.get("bp") or quote.get("bPrice") or quote.get("buyPrice") or 0.0)
                    b_ask = float(quote.get("sp") or quote.get("sPrice") or quote.get("sellPrice") or 0.0)
                    tok = str(inst.get("pSymbol") or inst.get("pTok") or inst.get("tok") or inst.get("instrument_token") or "").strip()
                    trd_sym = str(inst.get("pSymbolName") or inst.get("pTrdSymbol") or "").strip()
                    if not trd_sym:
                        matched = MASTER_CONTRACTS_MAP.get((symbol_upper, exp_code, int(stk_raw), "PE"))
                        trd_sym = matched[0] if matched else f"{symbol_upper}{exp_code}{int(stk_raw)}PE"

                    if stk_raw > 0 and ltp > 0:
                        pe_quotes[int(stk_raw)] = ltp
                        OPTION_LIVE_PRICES[trd_sym] = ltp
                        OPTION_LIVE_PRICES[str(int(stk_raw))] = ltp
                        depth_item = {
                            "ltp": round(ltp, 2),
                            "bid": round(b_bid, 2),
                            "ask": round(b_ask, 2),
                            "updated": time.time()
                        }
                        OPTION_LIVE_DEPTH[trd_sym] = depth_item
                        if tok:
                            TOKEN_TO_SYMBOL_MAP[tok] = trd_sym
                            OPTION_LIVE_DEPTH[tok] = depth_item
                            register_option_for_live_ws(trd_sym, tok, exch)

                OPTION_CHAIN_CACHE[base_cache_key] = {
                    "CE": ce_quotes,
                    "PE": pe_quotes,
                    "timestamp": now_ts,
                    "cooldown": False
                }
                chain_quotes = ce_quotes if opt_type_code == "CE" else pe_quotes
            else:
                # Broker returned 429 or error response without 'data' -> apply 10s backoff cooldown to prevent rapid retry storms
                existing_ce = cached_entry.get("CE", {}) if cached_entry else {}
                existing_pe = cached_entry.get("PE", {}) if cached_entry else {}
                OPTION_CHAIN_CACHE[base_cache_key] = {
                    "CE": existing_ce,
                    "PE": existing_pe,
                    "timestamp": now_ts,
                    "cooldown": True
                }
                chain_quotes = existing_ce if opt_type_code == "CE" else existing_pe
        except Exception:
            existing_ce = cached_entry.get("CE", {}) if cached_entry else {}
            existing_pe = cached_entry.get("PE", {}) if cached_entry else {}
            OPTION_CHAIN_CACHE[base_cache_key] = {
                "CE": existing_ce,
                "PE": existing_pe,
                "timestamp": now_ts,
                "cooldown": True
            }
            chain_quotes = existing_ce if opt_type_code == "CE" else existing_pe
    elif cached_entry:
        chain_quotes = cached_entry.get(opt_type_code, {})

    candidates = []

    # Spectrum of 81 candidate strikes around ATM (-40 to +40 steps)
    for i in range(-40, 41):
        stk = atm + i * step
        if stk <= 0:
            continue
        
        matched = MASTER_CONTRACTS_MAP.get((symbol_upper, exp_code, int(stk), opt_type_code))
        if matched:
            trd_sym = matched[0]
            tok = matched[1]
            seg = matched[2]
        else:
            trd_sym = f"{symbol_upper}{exp_code}{int(stk)}{opt_type_code}"
            tok = ""
            seg = "bse_fo" if symbol_upper in ["SENSEX", "BANKEX"] else "nse_fo"

        # Priority 1: Real-time Live WebSocket Tick LTP
        if trd_sym in OPTION_LIVE_PRICES:
            ltp = OPTION_LIVE_PRICES[trd_sym]
        elif str(int(stk)) in OPTION_LIVE_PRICES:
            ltp = OPTION_LIVE_PRICES[str(int(stk))]
        # Priority 2: Kotak Neo REST Option Chain Quote LTP
        elif int(stk) in chain_quotes:
            ltp = chain_quotes[int(stk)]
        # Priority 3: Black-Scholes / Intrinsic + Time Value Estimate
        else:
            ltp = calculate_option_theoretical_price(spot_ltp, float(stk), dte_days, opt_type_code, symbol_upper)

        register_option_for_live_ws(trd_sym, tok, seg)
        candidates.append({
            "strike": int(stk),
            "opt_type": opt_type_code,
            "trading_symbol": trd_sym,
            "ltp": ltp,
            "token": tok,
            "expiry": exp_code
        })

    # Include any extra strikes in chain_quotes not present in -40..+40
    existing_strikes = {c["strike"] for c in candidates}
    for stk_int, ltp_val in chain_quotes.items():
        if stk_int not in existing_strikes and stk_int > 0:
            matched = MASTER_CONTRACTS_MAP.get((symbol_upper, exp_code, int(stk_int), opt_type_code))
            trd_sym = matched[0] if matched else f"{symbol_upper}{exp_code}{int(stk_int)}{opt_type_code}"
            live_p = OPTION_LIVE_PRICES.get(trd_sym, ltp_val)
            tok = matched[1] if matched else ""
            seg = matched[2] if matched else ("bse_fo" if symbol_upper in ["SENSEX", "BANKEX"] else "nse_fo")
            register_option_for_live_ws(trd_sym, tok, seg)
            candidates.append({
                "strike": int(stk_int),
                "opt_type": opt_type_code,
                "trading_symbol": trd_sym,
                "ltp": live_p,
                "token": tok,
                "expiry": exp_code
            })

    return candidates


def preselect_qualifying_leg_contract(sym_base: str, leg: dict, candidates: List[Dict[str, Any]] = None, strategy_expiry: str = None) -> Dict[str, Any]:
    """
    Selects the qualifying contract for a strategy leg.
    For 'Closest Premium', evaluates live option chain candidate LTPs and selects the contract
    strictly closest to the target premium entered by the user (Option A: irrespective of ITM/ATM/OTM).
    If the leg has already executed, returns the executed contract with real-time live LTP.
    """
    symbol_upper = sym_base.upper().replace(" 50", "").strip()
    raw_crit = str(leg.get("strikeCriteria", "Closest Premium")).strip()
    crit_norm = raw_crit.lower()
    opt_type = leg.get("optionType", "Call").title()
    opt_type_code = "CE" if opt_type in ["Call", "CE"] else "PE"
    strat_exp = strategy_expiry or leg.get("strategyExpiry")

    # If this leg was already executed, stick to the executed strike and symbol!
    if leg.get("executedSymbol"):
        trd_sym = leg["executedSymbol"]
        strike = leg.get("executedStrike", 0)
        register_option_for_live_ws(trd_sym)
        live_p = OPTION_LIVE_PRICES.get(trd_sym, leg.get("executedEntryPrice", 0.0))
        depth = get_live_market_depth(trd_sym)
        return {
            "strike": strike,
            "option_ltp": round(live_p, 2),
            "best_bid": round(depth.get("bid", 0.0), 2),
            "best_ask": round(depth.get("ask", 0.0), 2),
            "target_prem": float(leg.get("strikeValue", live_p) or live_p),
            "diff": 0.0,
            "trading_symbol": trd_sym,
            "opt_type": leg.get("executedOptionType", opt_type_code),
            "criteria": "Executed Strike"
        }

    if candidates is None:
        candidates = get_option_chain_candidates(symbol_upper, opt_type, strategy_expiry=strat_exp)

    if crit_norm in ["closest premium", "closestpremium", "closest_premium", "target premium"]:
        try:
            target_prem = float(leg.get("strikeValue", 20) or 20)
        except (ValueError, TypeError):
            target_prem = 20.0

        best_cand = None
        best_diff = float("inf")

        for cand in candidates:
            # Always ensure candidate uses latest live WebSocket tick if available
            if cand["trading_symbol"] in OPTION_LIVE_PRICES:
                cand["ltp"] = OPTION_LIVE_PRICES[cand["trading_symbol"]]
            
            diff = abs(cand["ltp"] - target_prem)
            if diff < best_diff:
                best_diff = diff
                best_cand = cand

        if best_cand:
            register_option_for_live_ws(best_cand["trading_symbol"])
            latest_ltp = OPTION_LIVE_PRICES.get(best_cand["trading_symbol"], best_cand["ltp"])
            depth = get_live_market_depth(best_cand["trading_symbol"])
            return {
                "strike": best_cand["strike"],
                "option_ltp": round(latest_ltp, 2),
                "best_bid": round(depth.get("bid", 0.0), 2),
                "best_ask": round(depth.get("ask", 0.0), 2),
                "target_prem": target_prem,
                "diff": round(abs(latest_ltp - target_prem), 2),
                "trading_symbol": best_cand["trading_symbol"],
                "opt_type": best_cand["opt_type"],
                "criteria": "Closest Premium",
                "expiry": best_cand.get("expiry")
            }

    # Fallback for ATM / ITM / OTM / Strike Value
    spot_ltp = 22716.20 if "SENSEX" not in symbol_upper else 79500.00
    if "NIFTY" in symbol_upper and MARKET_CACHE.get("NIFTY") and MARKET_CACHE["NIFTY"]["price"] > 0:
        spot_ltp = MARKET_CACHE["NIFTY"]["price"]
    elif "SENSEX" in symbol_upper and MARKET_CACHE.get("SENSEX") and MARKET_CACHE["SENSEX"]["price"] > 0:
        spot_ltp = MARKET_CACHE["SENSEX"]["price"]

    step = INDEX_STRIKE_STEPS.get(symbol_upper, 50)
    atm = int(round(spot_ltp / step) * step)

    if raw_crit.upper().startswith("ITM"):
        try:
            offset = int(raw_crit.upper().replace("ITM", ""))
        except ValueError:
            offset = 1
        strike = (atm - offset * step) if opt_type_code == "CE" else (atm + offset * step)
    elif raw_crit.upper().startswith("OTM"):
        try:
            offset = int(raw_crit.upper().replace("OTM", ""))
        except ValueError:
            offset = 1
        strike = (atm + offset * step) if opt_type_code == "CE" else (atm - offset * step)
    elif raw_crit.upper() == "ATM":
        strike = atm
    else:
        try:
            strike = int(float(leg.get("strikeValue", atm)))
        except Exception:
            strike = atm

    match_cand = next((c for c in candidates if c["strike"] == strike), None)
    if match_cand:
        trd_sym = match_cand["trading_symbol"]
        ltp = OPTION_LIVE_PRICES.get(trd_sym, match_cand["ltp"])
    else:
        exp_code, _, _, _ = resolve_expiry_details(symbol_upper, strat_exp)
        matched = MASTER_CONTRACTS_MAP.get((symbol_upper, exp_code, strike, opt_type_code))
        trd_sym = matched[0] if matched else f"{symbol_upper}{exp_code}{strike}{opt_type_code}"
        ltp = OPTION_LIVE_PRICES.get(trd_sym, 120.0)

    register_option_for_live_ws(trd_sym)
    depth = get_live_market_depth(trd_sym)

    return {
        "strike": strike,
        "option_ltp": round(ltp, 2),
        "best_bid": round(depth.get("bid", 0.0), 2),
        "best_ask": round(depth.get("ask", 0.0), 2),
        "target_prem": float(leg.get("strikeValue", ltp) or ltp),
        "diff": 0.0,
        "trading_symbol": trd_sym,
        "opt_type": opt_type_code,
        "criteria": raw_crit
    }


def resolve_leg_strike_and_price(sym_base: str, leg: dict, strat_id: str = None, leg_idx: int = 0, strategy_expiry: str = None) -> tuple:
    """
    Resolves (strike_price, option_ltp, limit_price, trading_symbol) for a given strategy leg.
    Uses pre-warmed pre-selected contract if available for zero-latency entry.
    Calculates marketable limit price using Approach 1: Best Bid for Sell, Best Ask for Buy.
    """
    if strat_id and strat_id in PREWARM_STORE and PREWARM_STORE[strat_id].get("selected_legs"):
        sel_legs = PREWARM_STORE[strat_id]["selected_legs"]
        if sel_legs and leg_idx < len(sel_legs):
            sel = sel_legs[leg_idx]
            strike = sel["strike"]
            option_ltp = sel["option_ltp"]
            trd_sym = sel.get("trading_symbol", "")
            action = leg.get("position", "Sell").upper()
            best_bid = float(sel.get("best_bid", 0.0) or 0.0)
            best_ask = float(sel.get("best_ask", 0.0) or 0.0)
            limit_price = calculate_limit_order_price(
                option_ltp, action, trading_symbol=trd_sym, best_bid=best_bid, best_ask=best_ask
            )
            print(f"[+] [ZERO-LATENCY ENTRY] Strategy '{strat_id}' Leg #{leg_idx+1} using pre-warmed strike {trd_sym} @ ₹{option_ltp:.2f} (Bid: ₹{best_bid:.2f}, Ask: ₹{best_ask:.2f}, Limit: ₹{limit_price:.2f})")
            return strike, option_ltp, limit_price, trd_sym

    qual = preselect_qualifying_leg_contract(sym_base, leg, strategy_expiry=strategy_expiry)
    strike = qual["strike"]
    option_ltp = qual["option_ltp"]
    trd_sym = qual.get("trading_symbol", "")
    action = leg.get("position", "Sell").upper()
    best_bid = float(qual.get("best_bid", 0.0) or 0.0)
    best_ask = float(qual.get("best_ask", 0.0) or 0.0)
    limit_price = calculate_limit_order_price(
        option_ltp, action, trading_symbol=trd_sym, best_bid=best_bid, best_ask=best_ask
    )
    return strike, option_ltp, limit_price, trd_sym


def get_strategy_product_code(strat: dict) -> str:
    """Returns normalized broker product code: 'NRML' or 'MIS'."""
    if not isinstance(strat, dict):
        return "MIS"
    raw = str(strat.get("productCode", "")).upper()
    if "NRML" in raw or "NORMAL" in raw:
        return "NRML"
    return "MIS"


def is_transport_or_disconnect_error(response_or_exc) -> bool:
    """
    Detects whether an order placement / modification response or exception
    was caused by a network disconnect, stale keep-alive socket, or HTTP/2 transport drop.
    """
    if response_or_exc is None:
        return False
    if isinstance(response_or_exc, Exception):
        exc_str = str(response_or_exc).lower()
        if any(t in exc_str for t in [
            "server disconnected", "disconnected", "remoteprotocolerror",
            "connection reset", "connection closed", "broken pipe",
            "connection aborted", "eof occurred", "handshake failure",
            "remote end closed", "transport error", "connection error",
            "connectionterminated", "goaway", "stream_id"
        ]):
            return True
        if getattr(response_or_exc, "status", None) == 0:
            return True
    if isinstance(response_or_exc, dict):
        err_val = response_or_exc.get("error") or response_or_exc.get("Error") or response_or_exc.get("errMsg") or response_or_exc.get("message")
        if err_val is not None:
            if isinstance(err_val, Exception):
                return is_transport_or_disconnect_error(err_val)
            err_str = str(err_val).lower()
            if any(t in err_str for t in [
                "server disconnected", "disconnected", "remoteprotocolerror",
                "connection reset", "connection closed", "broken pipe",
                "connection aborted", "eof occurred", "handshake failure",
                "remote end closed", "transport error", "connection error",
                "connectionterminated", "goaway", "stream_id"
            ]):
                return True
        full_str = str(response_or_exc).lower()
        if (
            "server disconnected" in full_str
            or "remoteprotocolerror" in full_str
            or ("(0)" in full_str and "disconnect" in full_str)
            or "connectionterminated" in full_str
            or "goaway" in full_str
        ):
            return True
        if response_or_exc.get("stCode") == 0 or response_or_exc.get("status") == 0 or response_or_exc.get("status_code") == 0:
            return True
    return False


def refresh_client_http_session(client) -> bool:
    """
    Closes the existing httpx session and recreates a fresh connection pool
    to purge dead/stale HTTP/2 sockets after broker idle timeouts.
    """
    if client is None:
        return False
    try:
        rc = getattr(client, "api_client", None)
        if rc and hasattr(rc, "rest_client"):
            rc = rc.rest_client
        elif hasattr(client, "rest_client"):
            rc = client.rest_client

        if rc and hasattr(rc, "session"):
            try:
                rc.session.close()
            except Exception:
                pass
            if hasattr(rc, "_create_session"):
                rc.session = rc._create_session()
                print("[+] Refreshed Kotak Neo HTTP/2 session and connection pool.")
                return True
    except Exception as ex:
        print(f"[-] Failed to refresh Kotak Neo HTTP session: {ex}")
    return False


def safe_place_order_with_retry(client, max_retries: int = 2, retry_delay: float = 0.25, **order_kwargs) -> dict:
    """
    Executes client.place_order with automated retry on socket disconnects or stale HTTP/2 connections.
    If Kotak's gateway drops the connection (ApiException: Server disconnected), it automatically
    refreshes the underlying httpx session and immediately retries the order.
    """
    if client is None:
        return {"error": "Broker client not connected"}

    last_res = None
    for attempt in range(max_retries + 1):
        try:
            res = client.place_order(**order_kwargs)
            last_res = res
            if is_transport_or_disconnect_error(res):
                if attempt < max_retries:
                    sym_label = order_kwargs.get("trading_symbol", "N/A")
                    log_system_event(
                        f"[SOCKET RETRY] Kotak connection dropped during place_order ({sym_label}). "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak place_order failed after {max_retries} socket reconnect retries: {res}",
                        "ERROR"
                    )
                    return res
            return res
        except Exception as exc:
            last_res = {"error": exc}
            if is_transport_or_disconnect_error(exc):
                if attempt < max_retries:
                    sym_label = order_kwargs.get("trading_symbol", "N/A")
                    log_system_event(
                        f"[SOCKET RETRY] Exception during place_order ({sym_label}): {exc}. "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak place_order exception after {max_retries} retries: {exc}",
                        "ERROR"
                    )
                    return {"error": exc}
            else:
                return {"error": exc}

    return last_res or {"error": "Order placement failed"}


def safe_modify_order_with_retry(client, max_retries: int = 2, retry_delay: float = 0.25, **modify_kwargs) -> dict:
    """
    Executes client.modify_order with automated retry on socket disconnects or stale HTTP/2 connections.
    """
    if client is None:
        return {"error": "Broker client not connected"}

    last_res = None
    for attempt in range(max_retries + 1):
        try:
            res = client.modify_order(**modify_kwargs)
            last_res = res
            if is_transport_or_disconnect_error(res):
                if attempt < max_retries:
                    ord_label = modify_kwargs.get("order_id", "N/A")
                    log_system_event(
                        f"[SOCKET RETRY] Kotak connection dropped during modify_order (Order #{ord_label}). "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak modify_order failed after {max_retries} socket reconnect retries: {res}",
                        "ERROR"
                    )
                    return res
            return res
        except Exception as exc:
            last_res = {"error": exc}
            if is_transport_or_disconnect_error(exc):
                if attempt < max_retries:
                    ord_label = modify_kwargs.get("order_id", "N/A")
                    log_system_event(
                        f"[SOCKET RETRY] Exception during modify_order (Order #{ord_label}): {exc}. "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak modify_order exception after {max_retries} retries: {exc}",
                        "ERROR"
                    )
                    return {"error": exc}
            else:
                return {"error": exc}

    return last_res or {"error": "Order modification failed"}


def safe_cancel_order_with_retry(client, order_id: str, amo: str = "NO", max_retries: int = 2, retry_delay: float = 0.25) -> dict:
    """
    Executes client.cancel_order with automated retry on socket disconnects or stale HTTP/2 connections.
    """
    if client is None or not order_id:
        return {"error": "Invalid client or order_id"}

    last_res = None
    for attempt in range(max_retries + 1):
        try:
            res = client.cancel_order(order_id=str(order_id), amo=amo)
            last_res = res
            if is_transport_or_disconnect_error(res):
                if attempt < max_retries:
                    log_system_event(
                        f"[SOCKET RETRY] Kotak connection dropped during cancel_order (Order #{order_id}). "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak cancel_order failed after {max_retries} socket reconnect retries: {res}",
                        "ERROR"
                    )
                    return res
            return res
        except Exception as exc:
            last_res = {"error": exc}
            if is_transport_or_disconnect_error(exc):
                if attempt < max_retries:
                    log_system_event(
                        f"[SOCKET RETRY] Exception during cancel_order (Order #{order_id}): {exc}. "
                        f"Purging dead socket and reconnecting (Attempt {attempt+1}/{max_retries})...",
                        "WARN"
                    )
                    refresh_client_http_session(client)
                    time.sleep(retry_delay)
                    continue
                else:
                    log_system_event(
                        f"[SOCKET ERROR] Kotak cancel_order exception after {max_retries} retries: {exc}",
                        "ERROR"
                    )
                    return {"error": exc}
            else:
                return {"error": exc}

    return last_res or {"error": "Order cancellation failed"}


def cancel_pending_sl_orders_for_exited_leg(client, leg: dict, trading_symbol: str, strat_name: str = "Strategy") -> int:
    """
    Cancels all pending Stop-Loss orders for an EXITED leg on Kotak Neo.
    Guarantees that:
    1. Only the exited leg's specific symbol is cancelled (never touches other open legs or strategies).
    2. Cancels both the recorded leg['slOrderNo'] and any live pending orders for this symbol from client.order_report().
    3. Leaves open legs and other strategies 100% untouched.
    """
    if client is None or not trading_symbol:
        return 0

    cancelled_count = 0
    known_sl_id = str(leg.get("slOrderNo", "")).strip()

    # 1. Direct cancellation of recorded slOrderNo if present
    if known_sl_id and not known_sl_id.startswith("SIM-"):
        res = safe_cancel_order_with_retry(client, order_id=known_sl_id)
        if isinstance(res, dict) and (res.get("stat") == "Ok" or res.get("result")):
            cancelled_count += 1
            leg["slStatus"] = "CANCELLED"
            log_system_event(
                f"[EXIT SL CANCELLED] '{strat_name}' {trading_symbol}: Stop-Loss order #{known_sl_id} cancelled successfully!",
                "SUCCESS"
            )
        else:
            log_system_event(
                f"[EXIT SL NOTICE] Direct cancel for #{known_sl_id} ({trading_symbol}) returned: {res}. Checking broker order book...",
                "INFO"
            )
    elif known_sl_id.startswith("SIM-"):
        leg["slStatus"] = "CANCELLED"
        cancelled_count += 1
        log_system_event(f"[EXIT SL SIMULATED] Simulated Stop-Loss #{known_sl_id} marked CANCELLED.", "SUCCESS")

    # 2. Broker Order Book Sweep: Query Kotak's live order book strictly for trading_symbol
    try:
        order_book_res = None
        try:
            order_book_res = client.order_report()
        except Exception as o_err:
            if is_transport_or_disconnect_error(o_err):
                refresh_client_http_session(client)
                order_book_res = client.order_report()

        orders_list = []
        if isinstance(order_book_res, dict):
            raw = order_book_res.get("data")
            orders_list = raw if isinstance(raw, list) else [order_book_res]
        elif isinstance(order_book_res, list):
            orders_list = order_book_res

        PENDING_STATES = [
            "trigger_pending", "trigger pending", "open", "pending", "open_pending",
            "amo_req_received", "after_market_order_req_received"
        ]

        for o in orders_list:
            if not isinstance(o, dict):
                continue
            o_sym = str(o.get("trdSym") or "").strip()
            o_no = str(o.get("nOrdNo") or o.get("order_id") or o.get("orderNo") or "").strip()
            o_st = str(o.get("ordSt") or o.get("status") or "").strip().lower()

            # STRICT symbol match: ONLY cancel pending orders for THIS exited leg symbol
            if o_sym == trading_symbol and o_no and o_st in PENDING_STATES:
                log_system_event(
                    f"[ORDER BOOK SWEEP] Exited symbol {trading_symbol} has lingering pending order #{o_no} (Status: '{o_st}'). Cancelling on broker...",
                    "WARN"
                )
                c_res = safe_cancel_order_with_retry(client, order_id=o_no)
                if isinstance(c_res, dict) and (c_res.get("stat") == "Ok" or c_res.get("result")):
                    cancelled_count += 1
                    leg["slStatus"] = "CANCELLED"
                    log_system_event(
                        f"[EXIT SL SWEEP SUCCESS] Exited contract {trading_symbol}: Pending order #{o_no} cancelled on broker!",
                        "SUCCESS"
                    )
                else:
                    log_system_event(f"[EXIT SL SWEEP NOTICE] Order #{o_no} cancel response: {c_res}", "INFO")
    except Exception as sweep_ex:
        log_system_event(f"[-] Order book sweep exception for {trading_symbol}: {sweep_ex}", "DEBUG")

    return cancelled_count


def run_prewarm_fast_loop(strat_id: str, duration_sec: int = 20):
    """
    Runs a 250ms fast loop for pre-warming (20 seconds prior to entry time).
    Resolves option chain, streams quotes, and pre-selects qualifying contracts every 250ms.
    """
    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat:
        return

    sym_base = target_strat.get("symbol", "NIFTY")
    strat_expiry = target_strat.get("strategyExpiry")
    legs = target_strat.get("legs", [{}])

    PREWARM_STORE[strat_id] = {
        "status": "PRE_WARMING",
        "started_at": time.time(),
        "duration": duration_sec,
        "strategy_name": target_strat.get("name", "TBS Strategy"),
        "symbol": sym_base,
        "ticks_count": 0,
        "selected_legs": [],
        "logs": []
    }

    spot_val = MARKET_CACHE.get(sym_base, {}).get("price", 0.0)
    msg_start = f"[PRE-WARM INITIATED] Strategy '{target_strat['name']}' ({sym_base} Spot: ₹{spot_val:,.2f}) | 250ms fast loop running ({duration_sec}s countdown to entry)"
    log_system_event(msg_start, "INFO")

    # Pre-flight warm-up ping: Refresh HTTP/2 connection pool & verify active session so entry orders fire on hot socket
    client = SESSION_DATA.get("client")
    if client is not None:
        try:
            refresh_client_http_session(client)
            client.positions()
            log_system_event(f"[PRE-WARM HTTP/2 KEEPALIVE] Kotak Neo session pinged & warm for '{target_strat['name']}'.", "INFO")
        except Exception as ping_ex:
            log_system_event(f"[PRE-WARM PING NOTICE] Keepalive check: {ping_ex}", "WARN")

    start_time = time.time()

    while (time.time() - start_time) < duration_sec:
        loop_start = time.time()
        PREWARM_STORE[strat_id]["ticks_count"] += 1
        tick_no = PREWARM_STORE[strat_id]["ticks_count"]

        selected_legs = []
        for leg in legs:
            opt_type = leg.get("optionType", "Call")
            candidates = get_option_chain_candidates(sym_base, opt_type, strategy_expiry=strat_expiry)
            qualifying = preselect_qualifying_leg_contract(sym_base, leg, candidates, strategy_expiry=strat_expiry)
            selected_legs.append(qualifying)

        PREWARM_STORE[strat_id]["selected_legs"] = selected_legs
        
        if selected_legs:
            summary = ", ".join([f"{q['trading_symbol']} @ ₹{q['option_ltp']:.2f} (Diff: ₹{abs(q.get('diff', 0)):.2f})" for q in selected_legs])
            rem_sec = max(0, int(duration_sec - (time.time() - start_time)))
            log_msg = f"[PRE-WARM | T-{rem_sec:02d}s] Tracking Strike: {summary}"
            PREWARM_STORE[strat_id]["last_log"] = log_msg
            if tick_no % 4 == 1:
                log_system_event(log_msg, "INFO")

        elapsed = time.time() - loop_start
        sleep_dur = max(0.01, 0.25 - elapsed)
        time.sleep(sleep_dur)

    PREWARM_STORE[strat_id]["status"] = "COMPLETED"
    locked_str = ", ".join([f"{q['trading_symbol']} @ ₹{q['option_ltp']:.2f}" for q in selected_legs]) if selected_legs else "Resolved"
    log_system_event(f"[PRE-WARM COMPLETED] '{target_strat['name']}' locked zero-latency strike [{locked_str}]! Ready for entry execution.", "SUCCESS")


@app.route("/api/system-logs", methods=["GET"])
def get_system_logs():
    """Returns the latest system execution logs for UI display."""
    return jsonify({
        "status": "success",
        "logs": SYSTEM_LOGS[-150:]
    })


@app.route("/api/system-logs/clear", methods=["POST"])
def clear_system_logs():
    """Clears system execution logs."""
    global SYSTEM_LOGS
    SYSTEM_LOGS = []
    log_system_event("System execution logs cleared.", "INFO")
    return jsonify({"success": True, "message": "System logs cleared."})


@app.route("/api/profile/settings", methods=["GET"])
def get_profile_settings():
    """Returns current Profile & OMS settings plus active session account info."""
    user = SESSION_DATA.get("user") or {}
    username = user.get("displayName", user.get("username", "CHANDRA"))
    ucc = user.get("ucc", "Y2MEC")
    is_active = SESSION_DATA.get("client") is not None or bool(user)
    
    return jsonify({
        "status": "success",
        "account": {
            "username": username,
            "ucc": ucc,
            "connectionStatus": "ACTIVE" if is_active else "INACTIVE"
        },
        "oms": {
            "offsetType": PROFILE_SETTINGS.get("omsLimitOffsetType", "Points (₹)"),
            "offsetValue": PROFILE_SETTINGS.get("omsLimitOffsetValue", 0.5)
        },
        "display": {
            "timeMode": PROFILE_SETTINGS.get("displayTimeMode", "24-Hour Format (e.g. 21:09:15)"),
            "qtyMode": PROFILE_SETTINGS.get("displayQtyMode", "Show in Lots (Base)")
        }
    })


@app.route("/api/profile/oms-settings", methods=["POST"])
def save_oms_settings():
    """Updates Manual OMS Limit Order Settings (offset type & value)."""
    data = request.get_json() or {}
    offset_type = data.get("offsetType", "Points (₹)").strip()
    offset_value = float(data.get("offsetValue", 0.5))

    PROFILE_SETTINGS["omsLimitOffsetType"] = offset_type
    PROFILE_SETTINGS["omsLimitOffsetValue"] = offset_value

    print(f"[+] Saved OMS Settings: Offset Type={offset_type}, Offset Value={offset_value}")

    return jsonify({
        "success": True,
        "message": "Manual OMS Limit Order Settings saved successfully!",
        "settings": {
            "offsetType": offset_type,
            "offsetValue": offset_value
        }
    })


@app.route("/api/profile/display-settings", methods=["POST"])
def save_display_settings():
    """Updates General Display Settings (time format & qty display mode)."""
    data = request.get_json() or {}
    time_mode = data.get("timeMode", "24-Hour Format (e.g. 21:09:15)").strip()
    qty_mode = data.get("qtyMode", "Show in Lots (Base)").strip()

    PROFILE_SETTINGS["displayTimeMode"] = time_mode
    PROFILE_SETTINGS["displayQtyMode"] = qty_mode

    print(f"[+] Saved Display Settings: Time Mode={time_mode}, Qty Mode={qty_mode}")

    return jsonify({
        "success": True,
        "message": "General Display Settings saved successfully!",
        "settings": {
            "timeMode": time_mode,
            "qtyMode": qty_mode
        }
    })


@app.route("/api/oms/place-order", methods=["POST"])
def place_oms_manual_order():
    """
    Places a manual order via Kotak Neo broker.
    For Limit Orders (order_type='L' or 'LIMIT'), automatically applies the configured limit offset:
    Price = LTP +/- entry_limit_offset
    """
    data = request.get_json() or {}
    symbol = data.get("tradingSymbol", "NIFTY26O0622700CE").strip()
    action = data.get("transactionType", "BUY").strip().upper()
    order_type = data.get("orderType", "L").strip().upper()
    quantity = int(data.get("quantity", 1))
    raw_prod = data.get("product", "MIS")
    product = "NRML" if "NRML" in str(raw_prod).upper() else "MIS"
    user_price = float(data.get("price", 0) or 0)
    
    # Get current live depth and LTP from market depth or cache
    depth = get_live_market_depth(symbol)
    ltp = depth.get("ltp", 0.0) or 150.0
    if ltp <= 0 or ltp == 150.0:
        if "NIFTY" in symbol and ("CE" not in symbol and "PE" not in symbol) and MARKET_CACHE.get("NIFTY"):
            ltp = MARKET_CACHE["NIFTY"]["price"]
        elif "SENSEX" in symbol and ("CE" not in symbol and "PE" not in symbol) and MARKET_CACHE.get("SENSEX"):
            ltp = MARKET_CACHE["SENSEX"]["price"]
        
    final_price = user_price
    if order_type in ["L", "LIMIT"]:
        if user_price <= 0:
            final_price = calculate_limit_order_price(
                ltp, action, trading_symbol=symbol, best_bid=depth.get("bid", 0.0), best_ask=depth.get("ask", 0.0)
            )
        print(f"[*] Manual OMS Limit Order: Symbol={symbol}, Action={action}, Product={product}, LTP={ltp}, Bid={depth.get('bid', 0.0)}, Ask={depth.get('ask', 0.0)}, Final Limit Price={final_price}")

    client = SESSION_DATA.get("client")
    order_id = f"OMS-{uuid.uuid4().hex[:6].upper()}"

    if client is not None:
        try:
            exchange_seg = "bse_fo" if ("SENSEX" in symbol or "BANKEX" in symbol) else "nse_fo"
            res = safe_place_order_with_retry(
                client,
                exchange_segment=exchange_seg,
                trading_symbol=symbol,
                transaction_type="B" if action in ["BUY", "B"] else "S",
                product=product,
                order_type="L",
                quantity=str(quantity),
                price=str(final_price),
                validity="DAY"
            )
            if isinstance(res, dict) and (res.get("nOrdNo") or res.get("stat") == "Ok"):
                order_id = res.get("nOrdNo", order_id)
                print(f"[+] Kotak Neo OMS Limit Order Placed! Order ID: {order_id}")
            elif isinstance(res, dict) and (res.get("stat") == "Not_Ok" or "errMsg" in res):
                err = res.get("errMsg", "Broker rejected order")
                print(f"[-] Kotak Neo OMS Order Rejected: {err}")
                return jsonify({
                    "success": False,
                    "message": f"Kotak Neo Rejected Order: {err}"
                }), 400
        except Exception as e:
            print(f"[-] Kotak Neo OMS Order Exception: {e}")

    return jsonify({
        "success": True,
        "message": f"OMS Order submitted successfully! (Order ID: {order_id})",
        "order": {
            "orderId": order_id,
            "symbol": symbol,
            "action": action,
            "orderType": order_type,
            "product": product,
            "quantity": quantity,
            "price": final_price,
            "ltp": ltp
        }
    })


@app.route("/api/live-prices", methods=["GET"])
def get_live_prices():
    """Returns live prices for NIFTY 50 and SENSEX."""
    return jsonify({
        "status": "success",
        "timestamp": time.time(),
        "indices": {
            "NIFTY": MARKET_CACHE.get("NIFTY"),
            "SENSEX": MARKET_CACHE.get("SENSEX")
        },
        "optionPrices": {str(k): v for k, v in OPTION_LIVE_PRICES.items()},
        "optionDepth": {str(k): v for k, v in OPTION_LIVE_DEPTH.items()}
    })


@app.route("/api/stream-live-prices", methods=["GET"])
def stream_live_prices():
    """Server-Sent Events (SSE) endpoint for real-time tick-by-tick market data streaming."""
    print(f"[+] [LIVE STREAM CONNECTED] Dashboard client connected to live tick stream from {request.remote_addr}!", flush=True)
    def event_stream():
        while True:
            try:
                payload = json.dumps({
                    "status": "success",
                    "timestamp": time.time(),
                    "indices": {
                        "NIFTY": MARKET_CACHE.get("NIFTY"),
                        "SENSEX": MARKET_CACHE.get("SENSEX")
                    },
                    "optionPrices": {str(k): v for k, v in OPTION_LIVE_PRICES.items()},
                    "optionDepth": {str(k): v for k, v in OPTION_LIVE_DEPTH.items()}
                })
                yield f"data: {payload}\n\n"
                time.sleep(0.2)  # Push tick update every 200ms
            except GeneratorExit:
                break
            except Exception:
                time.sleep(1)

    return Response(event_stream(), mimetype="text/event-stream")


# -------------------------------------------------------------------
# TBS (TRADING STRATEGY BUILDER) REST ENDPOINTS
# -------------------------------------------------------------------

@app.route("/api/tbs/expiries", methods=["GET"])
def get_tbs_expiries():
    """Returns actual expiry dates for the specified index."""
    index_symbol = request.args.get("index", "NIFTY").strip().upper()
    expiries = get_index_expiries(index_symbol)
    return jsonify({
        "status": "success",
        "index": index_symbol,
        "expiries": expiries
    })


@app.route("/api/tbs/strategies", methods=["GET"])
def get_tbs_strategies():
    """Returns all created, armed, pre-warming, and active strategies."""
    saved = []
    for s in STRATEGIES_STORE:
        strat_copy = dict(s)
        sym_base = s.get("symbol", "NIFTY")
        strat_expiry = s.get("strategyExpiry")
        legs = s.get("legs", [{}])
        
        # Always resolve & trace target strike contract info for all legs
        tracked_legs = [preselect_qualifying_leg_contract(sym_base, leg, strategy_expiry=strat_expiry) for leg in legs]
        strat_copy["trackedLegs"] = tracked_legs
        if tracked_legs:
            strat_copy["trackedLeg"] = tracked_legs[0]

        if s["id"] in PREWARM_STORE:
            prewarm_copy = dict(PREWARM_STORE[s["id"]])
            # Ensure prewarm selected_legs reflect latest live ticks if available
            if prewarm_copy.get("selected_legs"):
                for q in prewarm_copy["selected_legs"]:
                    trd_sym = q.get("trading_symbol")
                    if trd_sym in OPTION_LIVE_PRICES:
                        q["option_ltp"] = round(OPTION_LIVE_PRICES[trd_sym], 2)
            strat_copy["prewarm"] = prewarm_copy
            
        saved.append(strat_copy)

    # Active strategies list includes ARMED (waiting for entry time), PRE_WARMING, and ACTIVE
    active = [s for s in saved if s.get("status") in ["ACTIVE", "PRE_WARMING", "ARMED"]]
    recent_exited = [s for s in saved if s.get("status") in ["EXITED", "STOPLOSS_HIT"]]
    
    return jsonify({
        "status": "success",
        "allStrategies": saved,
        "activeStrategies": active,
        "recentExitedStrategies": recent_exited
    })


@app.route("/api/tbs/prewarm", methods=["POST"])
def trigger_prewarm_endpoint():
    """Manually triggers 20-second pre-warming (250ms fast loop) for a strategy."""
    data = request.get_json() or {}
    strat_id = data.get("id")
    duration = int(data.get("duration", 20) or 20)

    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat and STRATEGIES_STORE:
        target_strat = STRATEGIES_STORE[0]

    if not target_strat:
        return jsonify({"success": False, "message": "No strategy found for pre-warming."}), 404

    target_strat["status"] = "PRE_WARMING"
    threading.Thread(target=run_prewarm_fast_loop, args=(target_strat["id"], duration), daemon=True).start()

    return jsonify({
        "success": True,
        "message": f"Pre-warming (250ms fast loop) initiated for 20s on '{target_strat['name']}'!",
        "strategy": target_strat
    })


@app.route("/api/tbs/create", methods=["POST"])
def create_tbs_strategy():
    """Creates a new trading strategy template with multi-leg settings."""
    data = request.get_json() or {}
    name = data.get("name", "").strip()

    if not name:
        return jsonify({"success": False, "message": "Strategy Name is required."}), 400

    legs = data.get("legs", [])
    if not legs:
        # Fallback default leg
        legs = [{
            "lots": int(data.get("lots", 1) or 1),
            "position": data.get("action", "Sell").title(),
            "optionType": data.get("optionType", "Call").title(),
            "strikeCriteria": data.get("strikeSelection", "Closest Premium"),
            "strikeValue": float(data.get("closestPremium", 20) or 20),
            "slEnable": True,
            "slType": data.get("slType", "Points (Pts)"),
            "slValue": float(data.get("slValue", 10) or 10),
            "targetEnable": False,
            "targetType": data.get("targetType", "Points (Pts)"),
            "targetValue": float(data.get("targetValue", 0) or 0)
        }]
    else:
        for leg in legs:
            crit = str(leg.get("strikeCriteria", "Closest Premium")).strip()
            leg["strikeCriteria"] = crit
            try:
                leg["strikeValue"] = float(leg.get("strikeValue", 20) or 20)
            except (ValueError, TypeError):
                leg["strikeValue"] = 20.0

    first_leg = legs[0] if legs else {}
    sl_enabled = first_leg.get("slEnable", True)
    if not sl_enabled:
        sl_desc = "None"
    else:
        sl_val = first_leg.get("slValue", 10)
        sl_desc = f"{sl_val} Pts" if str(first_leg.get("slType", "")).startswith("Points") else f"{sl_val}%"

    first_crit_norm = str(first_leg.get('strikeCriteria', '')).strip().lower()
    if first_crit_norm in ["closest premium", "closestpremium", "closest_premium", "target premium"]:
        strike_desc = f"Premium closest to ₹{first_leg.get('strikeValue', 20)}"
    else:
        strike_desc = first_leg.get('strikeCriteria', 'ATM')

    strat_id = data.get("id")
    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None) if strat_id else None

    if target_strat:
        # UPDATE EXISTING STRATEGY IN-PLACE
        target_strat["name"] = name
        target_strat["symbol"] = data.get("index", "NIFTY").replace(" 50", "").strip()
        target_strat["lotsPairs"] = int(first_leg.get("lots", 1))
        target_strat["entryTime"] = data.get("entryTime", "09:20")
        target_strat["exitTime"] = data.get("exitTime", "15:15")
        target_strat["sl"] = sl_desc
        target_strat["strikeSelection"] = strike_desc
        target_strat["scripIndex"] = data.get("scripIndex", "NIFTY 50 (NSE_FO)")
        target_strat["strategyExpiry"] = data.get("strategyExpiry") or (get_index_expiries(data.get("index", "NIFTY"))[0]["date"] if get_index_expiries(data.get("index", "NIFTY")) else datetime.date.today().strftime("%d-%b-%Y").upper())
        target_strat["entryType"] = data.get("entryType", "Time Based")
        target_strat["exitType"] = data.get("exitType", "Time Based")
        target_strat["productCode"] = data.get("productCode", "NRML (Normal Carrying)")
        target_strat["underlyingSource"] = data.get("underlyingSource", "Cash (Spot Index LTP)")
        target_strat["squareOffType"] = data.get("squareOffType", "Partial (Square off hit leg only)")
        target_strat["trailSLToBreakEven"] = data.get("trailSLToBreakEven", "None")
        target_strat["move_sl_to_cost"] = (data.get("trailSLToBreakEven") not in ["None", "", None] or bool(data.get("move_sl_to_cost", False)))
        target_strat["refPrice"] = data.get("refPrice", "Traded Price")
        target_strat["delayEntry"] = int(data.get("delayEntry", 0) or 0)
        target_strat["overallTargetProfit"] = float(data.get("overallTargetProfit", 0) or 0)
        target_strat["overallStopLoss"] = float(data.get("overallStopLoss", 0) or 0)
        target_strat["overallReEntrySL"] = data.get("overallReEntrySL", "None")
        target_strat["enableOverallTrailingSL"] = bool(data.get("enableOverallTrailingSL", False))
        target_strat["legs"] = legs
        target_strat["updatedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")

        if target_strat["entryType"] == "Manual Entry":
            target_strat["entryTime"] = "Manual (Desk)"
        if target_strat["exitType"] == "Manual Exit":
            target_strat["exitTime"] = "Manual (Desk)"

        dte_val = calculate_dte(target_strat.get("strategyExpiry", ""))
        dte_suffix = f" ({dte_val} DTE)" if target_strat.get("strategyExpiry") else ""
        log_system_event(f"[STRATEGY UPDATED] '{target_strat['name']}' | Symbol: {target_strat['symbol']} | Product: {get_strategy_product_code(target_strat)} | Expiry: {target_strat['strategyExpiry']}{dte_suffix} | Entry: {target_strat['entryTime']} | Exit: {target_strat['exitTime']} | SL: {target_strat['sl']} | Strike: {target_strat['strikeSelection']}", "SUCCESS")

        return jsonify({"success": True, "message": "Strategy updated successfully!", "strategy": target_strat})

    new_strat = {
        "id": f"strat-{uuid.uuid4().hex[:8]}",
        "name": name,
        "symbol": data.get("index", "NIFTY").replace(" 50", "").strip(),
        "broker": "Kotak Neo",
        "lotsPairs": int(first_leg.get("lots", 1)),
        "batches": 1,
        "entryTime": data.get("entryTime", "09:20"),
        "entryInterval": 0,
        "exitTime": data.get("exitTime", "15:15"),
        "exitInterval": 0,
        "sl": sl_desc,
        "strikeSelection": strike_desc,
        "status": "SAVED", # SAVED, ARMED, PRE_WARMING, ACTIVE, EXITED, STOPLOSS_HIT
        "isExecuted": False,
        
        # Extended General Settings
        "scripIndex": data.get("scripIndex", "NIFTY 50 (NSE_FO)"),
        "strategyExpiry": data.get("strategyExpiry") or (get_index_expiries(data.get("index", "NIFTY"))[0]["date"] if get_index_expiries(data.get("index", "NIFTY")) else datetime.date.today().strftime("%d-%b-%Y").upper()),
        "entryType": data.get("entryType", "Time Based"),
        "exitType": data.get("exitType", "Time Based"),
        "productCode": data.get("productCode", "NRML (Normal Carrying)"),
        "underlyingSource": data.get("underlyingSource", "Cash (Spot Index LTP)"),
        "squareOffType": data.get("squareOffType", "Partial (Square off hit leg only)"),
        "trailSLToBreakEven": data.get("trailSLToBreakEven", "None"),
        "move_sl_to_cost": (data.get("trailSLToBreakEven") not in ["None", "", None] or bool(data.get("move_sl_to_cost", False))),
        
        # Execution & Overall Strategy Settings
        "refPrice": data.get("refPrice", "Traded Price"),
        "delayEntry": int(data.get("delayEntry", 0) or 0),
        "overallTargetProfit": float(data.get("overallTargetProfit", 0) or 0),
        "overallStopLoss": float(data.get("overallStopLoss", 0) or 0),
        "overallReEntrySL": data.get("overallReEntrySL", "None"),
        "enableOverallTrailingSL": bool(data.get("enableOverallTrailingSL", False)),
        
        # Multi-leg Array
        "legs": legs,
        
        "orderId": None,
        "entryPrice": 0.0,
        "pnl": 0.0,
        "createdAt": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    if new_strat["entryType"] == "Manual Entry":
        new_strat["entryTime"] = "Manual (Desk)"
    if new_strat["exitType"] == "Manual Exit":
        new_strat["exitTime"] = "Manual (Desk)"

    STRATEGIES_STORE.append(new_strat)
    dte_val = calculate_dte(new_strat.get("strategyExpiry", ""))
    dte_suffix = f" ({dte_val} DTE)" if new_strat.get("strategyExpiry") else ""
    log_system_event(f"[STRATEGY CREATED] '{new_strat['name']}' | Symbol: {new_strat['symbol']} | Product: {get_strategy_product_code(new_strat)} | Expiry: {new_strat['strategyExpiry']}{dte_suffix} | Entry: {new_strat['entryTime']} | Exit: {new_strat['exitTime']} | SL: {new_strat['sl']} | Strike: {new_strat['strikeSelection']}", "SUCCESS")

    return jsonify({"success": True, "message": "Strategy saved successfully!", "strategy": new_strat})


@app.route("/api/tbs/update", methods=["POST", "PUT"])
def update_tbs_strategy():
    """Updates an existing strategy."""
    return create_tbs_strategy()


@app.route("/api/tbs/strategy/<strat_id>", methods=["GET"])
def get_tbs_strategy_by_id(strat_id):
    """Returns a single strategy by ID."""
    strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not strat:
        return jsonify({"success": False, "message": "Strategy not found."}), 404
    return jsonify({"success": True, "strategy": strat})


@app.route("/api/tbs/manual-entry", methods=["POST"])
def manual_entry_tbs_strategy():
    """Manually triggers trade entry for a strategy from Manual Desk (OMS)."""
    data = request.get_json() or {}
    strat_id = data.get("id")

    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat and STRATEGIES_STORE:
        target_strat = STRATEGIES_STORE[0]

    if not target_strat:
        return jsonify({"success": False, "message": "No strategy found for manual entry."}), 404

    target_strat["status"] = "ACTIVE"
    target_strat["activatedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")
    target_strat["entryModeUsed"] = "MANUAL_DESK"

    client = SESSION_DATA.get("client")
    order_ref = f"MAN-ENTRY-{uuid.uuid4().hex[:6].upper()}"

    sym_base = target_strat.get("symbol", "NIFTY")
    lots = int(target_strat.get("lotsPairs", 1))
    lot_sz = get_index_lot_size(sym_base)
    tot_qty = str(lots * lot_sz)
    strat_prod = get_strategy_product_code(target_strat)

    if client is not None:
        try:
            legs = target_strat.get("legs", [])
            if not legs:
                legs = [{}]
            trigger_limit_diff = float(PROFILE_SETTINGS.get("triggerLimitDiff", 1.0))

            def execute_single_manual_leg(idx, leg):
                if idx > 0:
                    time.sleep(idx * 0.05)  # 50ms stagger between parallel legs to prevent concurrent h2 stream collision
                strike, option_ltp, limit_price, trd_sym = resolve_leg_strike_and_price(sym_base, leg, strategy_expiry=target_strat.get("strategyExpiry"))
                opt_type_code = "CE" if leg.get("optionType", "Call").title() in ["Call", "CE"] else "PE"
                symbol_name = trd_sym or f"{sym_base}26SEP{strike}{opt_type_code}"
                leg_action = leg.get("position", "Buy").upper()
                tx_type = "B" if leg_action in ["BUY", "B"] else "S"
                depth_info = get_live_market_depth(symbol_name)
                b_bid = depth_info.get("bid", 0.0)
                b_ask = depth_info.get("ask", 0.0)
                pricing_detail = f"Best Bid: ₹{b_bid:.2f}" if tx_type == "S" else f"Best Ask: ₹{b_ask:.2f}"
                print(f"[*] Placing Manual Desk Marketable Limit Order: {symbol_name}, Action={leg_action}, Qty={tot_qty}, Product={strat_prod}, Option LTP={option_ltp}, {pricing_detail}, Limit Price={limit_price}...")
                res = safe_place_order_with_retry(
                    client,
                    exchange_segment="bse_fo" if sym_base in ["SENSEX", "BANKEX"] else "nse_fo",
                    trading_symbol=symbol_name,
                    transaction_type=tx_type,
                    product=strat_prod,
                    order_type="L",
                    quantity=tot_qty,
                    price=str(limit_price),
                    validity="DAY"
                )
                confirmed_no = None
                if isinstance(res, dict) and (res.get("nOrdNo") or res.get("stat") == "Ok"):
                    confirmed_no = res.get("nOrdNo", order_ref)
                    print(f"[+] Manual Desk Limit Order Executed with Kotak Neo! Order No: {confirmed_no}")
                    leg["entryOrderNo"] = str(confirmed_no)
                    leg["executedSymbol"] = symbol_name
                    leg["executedStrike"] = strike
                    leg["executedEntryPrice"] = option_ltp
                    leg["executedOptionType"] = opt_type_code

                    # Submit matching Stoploss order if enabled
                    sl_enable = leg.get("slEnable", True)
                    sl_val = float(leg.get("slValue", 0) or 0)
                    sl_type = str(leg.get("slType", "Points (Pts)"))
                    if sl_enable and sl_val > 0:
                        sl_trigger, sl_limit, sl_tx_type = calculate_sl_prices(
                            entry_price=option_ltp,
                            leg_action=leg_action,
                            sl_val=sl_val,
                            sl_type=sl_type,
                            trigger_limit_diff=trigger_limit_diff
                        )
                        sl_res = safe_place_order_with_retry(
                            client,
                            exchange_segment="bse_fo" if sym_base in ["SENSEX", "BANKEX"] else "nse_fo",
                            trading_symbol=symbol_name,
                            transaction_type=sl_tx_type,
                            product=strat_prod,
                            order_type="SL",
                            quantity=tot_qty,
                            price=str(sl_limit),
                            trigger_price=str(sl_trigger),
                            validity="DAY"
                        )
                        if isinstance(sl_res, dict) and (sl_res.get("nOrdNo") or sl_res.get("result")):
                            sl_no = str(sl_res.get("nOrdNo") or sl_res.get("result"))
                            leg["slOrderNo"] = sl_no
                            leg["slTriggerPrice"] = sl_trigger
                            leg["slLimitPrice"] = sl_limit
                            leg["slStatus"] = "PENDING"
                            log_system_event(f"[MANUAL DESK SL CONFIRMED] Leg #{idx+1}: Kotak Neo SL Order No: {sl_no} | Trigger: ₹{sl_trigger:.2f} | Limit: ₹{sl_limit:.2f}", "SUCCESS")
                return confirmed_no

            num_workers = min(4, max(1, len(legs)))
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(execute_single_manual_leg, idx, leg) for idx, leg in enumerate(legs)]
                for fut in futures:
                    c_no = fut.result()
                    if c_no:
                        order_ref = c_no
        except Exception as e:
            print(f"[-] Manual Desk Order Placement Notice: {e}")

    target_strat["orderId"] = order_ref
    first_p = float(target_strat["legs"][0].get("executedEntryPrice", 140.0)) if target_strat.get("legs") else 140.0
    target_strat["entryPrice"] = round(first_p, 2)

    # Hybrid Execution Phase 2: Background Trade Fill Reconciliation & SL Fine-Tuning
    if client is not None:
        reconcile_entry_fills_and_sl_in_background(target_strat, client)

    return jsonify({
        "success": True,
        "message": f"Manual Entry triggered successfully for '{target_strat['name']}' ({lots} Lots = {tot_qty} Qty)!",
        "strategy": target_strat
    })


@app.route("/api/tbs/manual-exit", methods=["POST"])
def manual_exit_tbs_strategy():
    """Manually triggers immediate trade exit / square off for a strategy from Manual Desk (OMS) via 3-Step Exit Cycle."""
    data = request.get_json() or {}
    strat_id = data.get("id")

    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat:
        active_strats = [s for s in STRATEGIES_STORE if s.get("status") == "ACTIVE"]
        if active_strats:
            target_strat = active_strats[0]

    if not target_strat:
        return jsonify({"success": False, "message": "No active strategy found to square off."}), 404

    res_strat = execute_strategy_exit(target_strat, reason="MANUAL_OMS_EXIT")
    return jsonify({
        "success": True,
        "message": f"Manual Exit / Square Off completed for '{res_strat['name']}'!",
        "strategy": res_strat
    })



@app.route("/api/tbs/activate", methods=["POST"])
def activate_tbs_strategy():
    """
    Arms / activates a strategy.
    If scheduled entry time is in the future, arms the strategy for automated pre-warming and entry.
    If entry time has passed or manual entry, executes immediately.
    """
    data = request.get_json() or {}
    strat_id = data.get("id")

    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat:
        return jsonify({"success": False, "message": "Strategy not found."}), 404

    entry_type = target_strat.get("entryType", "Time Based")
    now = datetime.datetime.now()
    should_execute_now = False

    if entry_type == "Time Based":
        entry_time = target_strat.get("entryTime", "09:20")
        try:
            eh, em = map(int, entry_time.split(":"))
            target_entry = now.replace(hour=eh, minute=em, second=0, microsecond=0)
            diff = (target_entry - now).total_seconds()
            if diff <= 2:
                should_execute_now = True
        except Exception:
            should_execute_now = True
    else:
        should_execute_now = True

    if should_execute_now:
        log_system_event(f"[STRATEGY ACTIVATION] '{target_strat['name']}' activated for IMMEDIATE execution.", "INFO")
        res_strat = execute_strategy_entry(target_strat)
        return jsonify({
            "success": True,
            "message": f"Strategy '{res_strat['name']}' activated & executed!",
            "strategy": res_strat
        })
    else:
        target_strat["status"] = "ARMED"
        msg = f"[STRATEGY ARMED] '{target_strat['name']}' armed for scheduled entry at {target_strat.get('entryTime')} (Exit: {target_strat.get('exitTime')} | SL: {target_strat.get('sl')}). 20s fast-loop pre-warming scheduled before entry."
        log_system_event(msg, "INFO")
        return jsonify({
            "success": True,
            "message": f"Strategy '{target_strat['name']}' submitted & armed for {target_strat.get('entryTime')}! Pre-warming (250ms) will start 20s before entry time.",
            "strategy": target_strat
        })


@app.route("/api/tbs/deactivate", methods=["POST"])
def deactivate_tbs_strategy():
    """Deactivates / squares off an active strategy manually."""
    data = request.get_json() or {}
    strat_id = data.get("id")

    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat:
        return jsonify({"success": False, "message": "Strategy not found."}), 404

    is_active = target_strat.get("isExecuted", False) or target_strat.get("status") == "ACTIVE"
    if is_active:
        log_system_event(f"[MANUAL STOP] Stop request received for LIVE strategy '{target_strat['name']}'. Squaring off open position immediately...", "WARN")
    else:
        log_system_event(f"[MANUAL STOP] Stop request received for ARMED strategy '{target_strat['name']}' (Status: {target_strat.get('status')}). Disarming schedule...", "INFO")

    res_strat = execute_strategy_exit(target_strat, reason="MANUAL_STOP_BUTTON")
    return jsonify({"success": True, "message": f"Strategy '{res_strat['name']}' stopped.", "strategy": res_strat})


@app.route("/api/tbs/move-sl-to-cost", methods=["POST"])
def manual_move_sl_to_cost():
    """Manually triggers Move SL to Cost adjustment for remaining leg."""
    data = request.get_json() or {}
    strat_id = data.get("id")
    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    if not target_strat:
        return jsonify({"success": False, "message": "Strategy not found."}), 404
    hit_leg_idx = int(data.get("hitLegIndex", 0))
    trigger_tbs_modify_stoploss_to_cost(target_strat, hit_leg_idx=hit_leg_idx, hit_order_no="MANUAL_TRIGGER")
    return jsonify({"success": True, "message": f"Move SL to Cost triggered for '{target_strat['name']}'.", "strategy": target_strat})


@app.route("/api/tbs/delete", methods=["DELETE"])
def delete_tbs_strategy():
    """Deletes a strategy."""
    data = request.get_json() or {}
    strat_id = data.get("id")

    global STRATEGIES_STORE
    target_strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
    strat_name = target_strat.get("name", strat_id) if target_strat else strat_id
    STRATEGIES_STORE = [s for s in STRATEGIES_STORE if s["id"] != strat_id]
    log_system_event(f"Strategy '{strat_name}' deleted.", "INFO")

    return jsonify({"success": True, "message": "Strategy deleted successfully."})


def execute_strategy_entry(target_strat: dict) -> dict:
    """
    Executes FO Entry & Immediate Stoploss Order Execution via Kotak Neo SDK.
    Respects 'delayEntry' seconds setting before placing orders if configured.
    """
    target_strat["status"] = "ACTIVE"
    target_strat["isExecuted"] = True
    target_strat["activatedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")

    sym_base = target_strat.get("symbol", "NIFTY")
    lots = int(target_strat.get("lotsPairs", 1))
    lot_sz = get_index_lot_size(sym_base)
    tot_qty = str(lots * lot_sz)
    legs = target_strat.get("legs", [{}])
    strat_prod = get_strategy_product_code(target_strat)

    log_system_event(f"[ENTRY INITIATED] Strategy '{target_strat['name']}' ({sym_base}) | Product: {strat_prod} | Legs: {len(legs)} | Total Lots: {lots} ({tot_qty} Qty)", "INFO")

    # Check for configured Entry Delay (Seconds, 0-50)
    delay_sec = int(target_strat.get("delayEntry", 0) or 0)
    if delay_sec > 0:
        log_system_event(f"Entry Delay active: Holding execution for {delay_sec} seconds on strategy '{target_strat['name']}'...", "WARN")
        time.sleep(delay_sec)

    client = SESSION_DATA.get("client")
    order_ref = f"ORD-{uuid.uuid4().hex[:6].upper()}"

    executed_ltps = []
    entry_order_confirmations = []
    sl_orders_placed = []

    trigger_limit_diff = float(PROFILE_SETTINGS.get("triggerLimitDiff", 1.0))

    if client is not None:
        try:
            def execute_single_entry_leg(idx, leg):
                if idx > 0:
                    time.sleep(idx * 0.05)  # 50ms stagger between parallel legs to prevent concurrent h2 stream collision
                leg_strike, leg_ltp, leg_limit, leg_symbol = resolve_leg_strike_and_price(
                    sym_base, leg, target_strat["id"], idx, strategy_expiry=target_strat.get("strategyExpiry")
                )
                leg["executedEntryPrice"] = leg_ltp

                opt_type_code = "CE" if leg.get("optionType", "Call").title() in ["Call", "CE"] else "PE"
                symbol_name = leg.get("executedSymbol") or leg_symbol or f"{sym_base}26SEP{leg_strike}{opt_type_code}"
                leg["executedSymbol"] = symbol_name
                leg["executedStrike"] = leg_strike
                leg["executedOptionType"] = opt_type_code
                register_option_for_live_ws(symbol_name)
                leg_action = leg.get("position", "Sell").upper()
                tx_type = "B" if leg_action in ["BUY", "B"] else "S"
                
                depth_info = get_live_market_depth(symbol_name)
                b_bid = depth_info.get("bid", 0.0)
                b_ask = depth_info.get("ask", 0.0)
                pricing_side = f"Best Bid: ₹{b_bid:.2f}" if tx_type == "S" else f"Best Ask: ₹{b_ask:.2f}"
                log_msg = f"[ENTRY ORDER SUBMITTED] Leg #{idx+1}/{len(legs)}: Placing Marketable Limit Order for {symbol_name} ({leg_action}, Qty: {tot_qty}, Product: {strat_prod}, LTP: ₹{leg_ltp:.2f}, {pricing_side}, Limit: ₹{leg_limit:.2f})"
                log_system_event(log_msg, "INFO")

                res = safe_place_order_with_retry(
                    client,
                    exchange_segment="bse_fo" if sym_base in ["SENSEX", "BANKEX"] else "nse_fo",
                    trading_symbol=symbol_name,
                    transaction_type=tx_type,
                    product=strat_prod,
                    order_type="L",
                    quantity=tot_qty,
                    price=str(leg_limit),
                    validity="DAY"
                )
                
                confirmed_order_no = None
                if isinstance(res, dict):
                    if res.get("nOrdNo"):
                        confirmed_order_no = str(res["nOrdNo"])
                    elif res.get("stat") == "Ok" or res.get("result"):
                        confirmed_order_no = str(res.get("result") or f"ORD-{uuid.uuid4().hex[:6].upper()}")
                    elif res.get("stat") == "Not_Ok" or res.get("stCode") == 100008 or "unauthorized" in str(res.get("errMsg", "")).lower():
                        err_msg = res.get("errMsg", "Unauthorized")
                        log_system_event(f"KOTAK API REJECTED: 401 Unauthorized (stCode: {res.get('stCode')}, Message: '{err_msg}'). Session token expired! Please re-login via Broker Settings.", "ERROR")
                    else:
                        log_system_event(f"KOTAK API ENTRY NOTICE Leg #{idx+1} ({symbol_name}): {res}", "WARN")

                if confirmed_order_no:
                    leg["entryOrderNo"] = str(confirmed_order_no)
                    log_system_event(f"[ENTRY ORDER FILLED] Leg #{idx+1} {symbol_name}: Kotak Neo Order No: {confirmed_order_no} | Fill Price: ₹{leg_ltp:.2f}", "SUCCESS")

                    # IMMEDIATELY submit matching Stoploss order if Stoploss is enabled for this leg
                    sl_enable = leg.get("slEnable", True)
                    sl_val = float(leg.get("slValue", 0) or 0)
                    sl_type = str(leg.get("slType", "Points (Pts)"))

                    if sl_enable and sl_val > 0:
                        entry_price = leg_ltp
                        sl_trigger, sl_limit, sl_tx_type = calculate_sl_prices(
                            entry_price=entry_price,
                            leg_action=leg_action,
                            sl_val=sl_val,
                            sl_type=sl_type,
                            trigger_limit_diff=trigger_limit_diff
                        )

                        log_system_event(f"[STOPLOSS ORDER SUBMITTED] Leg #{idx+1} ({symbol_name}): {sl_tx_type} | Product: {strat_prod} | Trigger: ₹{sl_trigger:.2f} | Limit: ₹{sl_limit:.2f}", "INFO")

                        sl_res = safe_place_order_with_retry(
                            client,
                            exchange_segment="bse_fo" if sym_base in ["SENSEX", "BANKEX"] else "nse_fo",
                            trading_symbol=symbol_name,
                            transaction_type=sl_tx_type,
                            product=strat_prod,
                            order_type="SL",
                            quantity=tot_qty,
                            price=str(sl_limit),
                            trigger_price=str(sl_trigger),
                            validity="DAY"
                        )

                        sl_order_no = None
                        if isinstance(sl_res, dict):
                            sl_order_no = sl_res.get("nOrdNo") or sl_res.get("result")
                            if sl_res.get("stat") == "Not_Ok" or sl_res.get("stCode") == 100008 or "unauthorized" in str(sl_res.get("errMsg", "")).lower():
                                log_system_event(f"KOTAK API SL REJECTED: 401 Unauthorized (stCode: {sl_res.get('stCode')}). Session token expired!", "ERROR")
                        
                        if sl_order_no:
                            log_system_event(f"[STOPLOSS ORDER CONFIRMED] Leg #{idx+1}: Kotak Neo SL Order No: {sl_order_no}", "SUCCESS")
                            leg["slOrderNo"] = str(sl_order_no)
                            leg["slTriggerPrice"] = sl_trigger
                            leg["slLimitPrice"] = sl_limit
                            leg["slStatus"] = "PENDING"
                            leg["slMovedToCost"] = False
                            return (leg_ltp, confirmed_order_no, sl_order_no)
                        else:
                            log_system_event(f"[STOPLOSS ORDER RESPONSE] {sl_res}", "INFO")
                            return (leg_ltp, confirmed_order_no, None)
                    else:
                        log_system_event(f"[STOPLOSS] Leg #{idx+1} ({symbol_name}): Stoploss is DISABLED (SL: None). Position running without SL order.", "INFO")
                        return (leg_ltp, confirmed_order_no, None)
                else:
                    return (leg_ltp, None, None)

            num_workers = min(4, max(1, len(legs)))
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(execute_single_entry_leg, idx, leg) for idx, leg in enumerate(legs)]
                for fut in futures:
                    try:
                        ltp_val, entry_ord, sl_ord = fut.result()
                        executed_ltps.append(ltp_val)
                        if entry_ord:
                            entry_order_confirmations.append(entry_ord)
                            order_ref = entry_ord
                        if sl_ord:
                            sl_orders_placed.append(sl_ord)
                    except Exception as fe:
                        log_system_event(f"Leg execution thread error: {fe}", "ERROR")
        except Exception as e:
            log_system_event(f"Live Broker Order Execution Exception: {e}", "ERROR")
    else:
        for idx, leg in enumerate(legs):
            leg_strike, leg_ltp, leg_limit, leg_symbol = resolve_leg_strike_and_price(sym_base, leg, target_strat["id"], idx, strategy_expiry=target_strat.get("strategyExpiry"))
            executed_ltps.append(leg_ltp)
            leg["executedEntryPrice"] = leg_ltp
            opt_type_code = "CE" if leg.get("optionType", "Call").title() in ["Call", "CE"] else "PE"
            symbol_name = leg.get("executedSymbol") or leg_symbol or f"{sym_base}26SEP{leg_strike}{opt_type_code}"
            leg["executedSymbol"] = symbol_name
            leg["executedStrike"] = leg_strike
            leg["executedOptionType"] = opt_type_code

            sl_enable = leg.get("slEnable", True)
            sl_val = float(leg.get("slValue", 0) or 0)
            sl_type = str(leg.get("slType", "Points (Pts)"))

            if sl_enable and sl_val > 0:
                sim_trig, sim_lim, _ = calculate_sl_prices(
                    entry_price=leg_ltp,
                    leg_action=leg.get("position", "Sell"),
                    sl_val=sl_val,
                    sl_type=sl_type,
                    trigger_limit_diff=trigger_limit_diff
                )
                leg["slOrderNo"] = f"SIM-SL-{idx+1}"
                leg["slTriggerPrice"] = sim_trig
                leg["slLimitPrice"] = sim_lim
                leg["slStatus"] = "PENDING"
                leg["slMovedToCost"] = False

            register_option_for_live_ws(symbol_name)
            log_system_event(f"[SIMULATED ENTRY] Leg #{idx+1}: Contract locked @ ₹{leg_ltp:.2f} (Limit: ₹{leg_limit:.2f})", "SUCCESS")

    target_strat["orderId"] = order_ref
    target_strat["entryPrice"] = round(float(executed_ltps[0] if executed_ltps else 140.0), 2)
    target_strat["entryOrderConfirmations"] = entry_order_confirmations
    target_strat["slOrdersPlaced"] = sl_orders_placed
    log_system_event(f"[POSITION LIVE] Strategy '{target_strat['name']}' is now ACTIVE in market! Locked Entry: ₹{target_strat['entryPrice']:.2f} | Scheduled Exit: {target_strat.get('exitTime')}", "SUCCESS")

    # Monitor & Stoploss Adjustment (Move SL to Cost)
    trail_opt = str(target_strat.get("trailSLToBreakEven", "None")).strip()
    move_sl_enabled = (
        target_strat.get("move_sl_to_cost", False) or
        trail_opt not in ["None", "", "none"] or
        "cost" in trail_opt.lower() or
        "break-even" in trail_opt.lower()
    )
    target_strat["move_sl_to_cost"] = move_sl_enabled

    if move_sl_enabled and len(legs) > 1:
        log_system_event(f"[MOVE SL TO COST ARMED] Strategy '{target_strat['name']}' has Move SL to Cost enabled across {len(legs)} legs. Launching 500ms order-book polling...", "INFO")
        threading.Thread(target=poll_order_book_and_adjust_sl, args=(target_strat["id"],), daemon=True).start()

    # Hybrid Execution Phase 2: Background Trade Fill Reconciliation & SL Fine-Tuning
    if client is not None:
        reconcile_entry_fills_and_sl_in_background(target_strat, client)

    return target_strat


def reconcile_entry_fills_and_sl_in_background(target_strat: dict, client):
    """
    Hybrid Execution Phase 2:
    Runs in background after entry & SL orders are placed.
    Queries Kotak Neo order report and positions to retrieve the actual exchange
    fill execution prices (e.g. ₹99.60 vs estimated ₹100.00).
    Reconciles leg['executedEntryPrice'] so application MTM matches Kotak Neo to the exact paisa,
    and fine-tunes pending Stop-Loss orders if the fill differed from the pre-order quote.
    """
    def _reconcile_worker():
        if client is None:
            return
        # Brief 0.6s delay for exchange matching and broker trade ledger update
        time.sleep(0.6)

        legs = target_strat.get("legs", [])
        sym_base = target_strat.get("symbol", "NIFTY")
        trigger_limit_diff = float(PROFILE_SETTINGS.get("triggerLimitDiff", 1.0))
        lot_sz = get_index_lot_size(sym_base)
        lots = int(target_strat.get("lotsPairs", 1) or 1)

        # Retry up to 3 times with 0.8s intervals
        for attempt in range(3):
            try:
                # 1. Fetch order report from broker
                order_book_res = None
                try:
                    order_book_res = client.order_report()
                except Exception:
                    pass

                orders_by_id = {}
                if isinstance(order_book_res, dict):
                    raw_data = order_book_res.get("data")
                    orders_list = raw_data if isinstance(raw_data, list) else [order_book_res]
                    for o in orders_list:
                        if isinstance(o, dict):
                            ord_no = str(o.get("nOrdNo") or o.get("order_id") or o.get("orderNo") or "").strip()
                            if ord_no:
                                orders_by_id[ord_no] = o

                # 2. Also fetch positions as backup
                positions_map = {}
                try:
                    pos_res = client.positions()
                    if isinstance(pos_res, dict) and isinstance(pos_res.get("data"), list):
                        for p in pos_res["data"]:
                            if isinstance(p, dict):
                                ts = str(p.get("trdSym") or "").strip()
                                if ts:
                                    positions_map[ts] = p
                except Exception:
                    pass

                reconciled_any = False
                for idx, leg in enumerate(legs):
                    sym = leg.get("executedSymbol", "")
                    entry_ord_no = str(leg.get("entryOrderNo", "")).strip()
                    pos = leg.get("position", "Sell").upper()
                    old_entry = float(leg.get("executedEntryPrice", 0.0))

                    real_fill_price = None

                    # Check order report first
                    if entry_ord_no and entry_ord_no in orders_by_id:
                        ord_obj = orders_by_id[entry_ord_no]
                        st = str(ord_obj.get("ordSt") or ord_obj.get("status") or "").lower()
                        if st in ["complete", "traded", "executed", "filled"]:
                            try:
                                avg_p = float(ord_obj.get("avgPrc") or ord_obj.get("flPrc") or 0.0)
                                if avg_p > 0:
                                    real_fill_price = round(avg_p, 2)
                            except (ValueError, TypeError):
                                pass

                    # Fallback to positions if order report didn't have avgPrc
                    if real_fill_price is None and sym in positions_map:
                        pos_obj = positions_map[sym]
                        try:
                            avg_key = "sellAvgPrc" if pos in ["SELL", "S"] else "buyAvgPrc"
                            avg_val = float(pos_obj.get(avg_key) or pos_obj.get("avgPrc") or 0.0)
                            if avg_val > 0:
                                real_fill_price = round(avg_val, 2)
                        except (ValueError, TypeError):
                            pass

                    if real_fill_price and real_fill_price > 0:
                        diff = abs(real_fill_price - old_entry)
                        if diff >= 0.01:
                            leg["executedEntryPrice"] = real_fill_price
                            reconciled_any = True
                            log_system_event(
                                f"[HYBRID FILL RECONCILED] Leg #{idx+1} ({sym}): Actual Broker Fill: ₹{real_fill_price:.2f} (Estimated was ₹{old_entry:.2f}). App MTM synchronized 100%!",
                                "SUCCESS"
                            )

                            # Fine-tune matching SL order if present
                            sl_ord_no = str(leg.get("slOrderNo", "")).strip()
                            sl_enable = leg.get("slEnable", True)
                            sl_val = float(leg.get("slValue", 0) or 0)
                            sl_type = str(leg.get("slType", "Points (Pts)"))

                            if sl_ord_no and not sl_ord_no.startswith("SIM-") and sl_enable and sl_val > 0:
                                new_sl_trigger, new_sl_limit, _ = calculate_sl_prices(
                                    entry_price=real_fill_price,
                                    leg_action=pos,
                                    sl_val=sl_val,
                                    sl_type=sl_type,
                                    trigger_limit_diff=trigger_limit_diff
                                )
                                old_sl_trigger = float(leg.get("slTriggerPrice", 0.0))
                                if abs(new_sl_trigger - old_sl_trigger) >= 0.05:
                                    leg_lots = int(leg.get("lots", lots))
                                    leg_qty = str(leg_lots * lot_sz)
                                    try:
                                        mod_res = safe_modify_order_with_retry(
                                            client,
                                            order_id=sl_ord_no,
                                            price=str(new_sl_limit),
                                            order_type="SL",
                                            quantity=leg_qty,
                                            validity="DAY",
                                            trigger_price=str(new_sl_trigger)
                                        )
                                        if isinstance(mod_res, dict) and (mod_res.get("nOrdNo") or mod_res.get("stat") == "Ok"):
                                            leg["slTriggerPrice"] = new_sl_trigger
                                            leg["slLimitPrice"] = new_sl_limit
                                            log_system_event(
                                                f"[HYBRID SL RE-ALIGNED] Leg #{idx+1} ({sym}): Broker SL Order #{sl_ord_no} updated to ₹{new_sl_trigger:.2f} (aligned to real fill ₹{real_fill_price:.2f})",
                                                "SUCCESS"
                                            )
                                    except Exception as me:
                                        print(f"[-] SL fine-tune exception for Leg #{idx+1}: {me}")

                if reconciled_any:
                    first_leg_entry = float(legs[0].get("executedEntryPrice", 0.0)) if legs else 0.0
                    if first_leg_entry > 0:
                        target_strat["entryPrice"] = first_leg_entry
                    break # Successfully reconciled all legs!

            except Exception as e:
                print(f"[-] Reconcile worker error on attempt {attempt+1}: {e}")

            time.sleep(0.8)

    threading.Thread(target=_reconcile_worker, daemon=True).start()


def trigger_tbs_modify_stoploss_to_cost(target_strat: dict, hit_leg_idx: int, hit_order_no: str) -> None:
    """
    Rewrites the remaining leg's stoploss trigger price to match its original entry execution price,
    shielding the strategy from losses on reversal.
    Calls client.modify_order() with the new trigger price and corresponding limit price.
    """
    legs = target_strat.get("legs", [])
    sym_base = target_strat.get("symbol", "NIFTY")
    trigger_limit_diff = float(PROFILE_SETTINGS.get("triggerLimitDiff", 1.0))
    client = SESSION_DATA.get("client")

    for rem_idx, rem_leg in enumerate(legs):
        if rem_idx == hit_leg_idx:
            continue
        if rem_leg.get("slHit", False) or rem_leg.get("slMovedToCost", False):
            continue

        rem_sl_order_no = rem_leg.get("slOrderNo")
        rem_symbol = rem_leg.get("executedSymbol", "")
        rem_pos = rem_leg.get("position", "Sell").upper()
        rem_lots = int(rem_leg.get("lots", 1))
        rem_lot_sz = get_index_lot_size(sym_base)
        rem_qty = str(rem_lots * rem_lot_sz)
        orig_entry_price = float(rem_leg.get("executedEntryPrice", 0.0))

        if orig_entry_price <= 0:
            continue

        # New trigger price is the original entry execution price (cost)
        new_trigger_price = round(orig_entry_price, 2)

        # Calculate new limit price with triggerLimitDiff
        if rem_pos in ["SELL", "S"]:
            # Leg was Sell -> SL order is a Buy order (B)
            new_limit_price = round(new_trigger_price + trigger_limit_diff, 2)
        else:
            # Leg was Buy -> SL order is a Sell order (S)
            new_limit_price = round(max(0.05, new_trigger_price - trigger_limit_diff), 2)

        log_system_event(
            f"[MODIFYING SL TO COST] Leg #{rem_idx+1} ({rem_symbol}): Moving SL Trigger to Original Entry Cost ₹{new_trigger_price:.2f} (Limit: ₹{new_limit_price:.2f})...",
            "WARN"
        )

        if client is not None and rem_sl_order_no and not str(rem_sl_order_no).startswith("SIM-"):
            try:
                mod_res = safe_modify_order_with_retry(
                    client,
                    order_id=str(rem_sl_order_no),
                    price=str(new_limit_price),
                    order_type="SL",
                    quantity=str(rem_qty),
                    validity="DAY",
                    trigger_price=str(new_trigger_price),
                    disclosed_quantity="0",
                    amo="NO"
                )

                confirmed_id = None
                if isinstance(mod_res, dict):
                    confirmed_id = mod_res.get("nOrdNo") or mod_res.get("result")
                    if not confirmed_id and mod_res.get("stat") == "Ok":
                        confirmed_id = rem_sl_order_no

                if confirmed_id:
                    rem_leg["slOrderNo"] = str(confirmed_id)
                    rem_leg["slTriggerPrice"] = new_trigger_price
                    rem_leg["slLimitPrice"] = new_limit_price
                    rem_leg["slMovedToCost"] = True
                    target_strat["slMovedToCost"] = True
                    log_system_event(
                        f"[MOVE SL TO COST SUCCESS] Leg #{rem_idx+1} ({rem_symbol}) Stop-Loss updated to Entry Cost ₹{new_trigger_price:.2f}! Broker Order: {confirmed_id}. Position shielded from reversal losses.",
                        "SUCCESS"
                    )
                else:
                    err_msg = mod_res.get("errMsg") or mod_res.get("message") or mod_res if isinstance(mod_res, dict) else str(mod_res)
                    log_system_event(
                        f"Kotak Neo modify_order for Leg #{rem_idx+1} ({rem_symbol}): {err_msg}",
                        "WARN"
                    )
                    rem_leg["slTriggerPrice"] = new_trigger_price
                    rem_leg["slLimitPrice"] = new_limit_price
                    rem_leg["slMovedToCost"] = True
                    target_strat["slMovedToCost"] = True
            except Exception as e:
                log_system_event(f"Kotak Neo modify_order exception for Leg #{rem_idx+1}: {e}", "ERROR")
                rem_leg["slTriggerPrice"] = new_trigger_price
                rem_leg["slLimitPrice"] = new_limit_price
                rem_leg["slMovedToCost"] = True
                target_strat["slMovedToCost"] = True
        else:
            # Paper trading / simulated mode
            rem_leg["slTriggerPrice"] = new_trigger_price
            rem_leg["slLimitPrice"] = new_limit_price
            rem_leg["slMovedToCost"] = True
            target_strat["slMovedToCost"] = True
            log_system_event(
                f"[SIMULATED MOVE SL TO COST] Leg #{rem_idx+1} ({rem_symbol}) Stop-Loss moved to Entry Cost ₹{new_trigger_price:.2f}.",
                "SUCCESS"
            )


triggers_tbs_modify_stoploss_to_cost = trigger_tbs_modify_stoploss_to_cost


def poll_order_book_and_adjust_sl(strat_id: str):
    """
    Polls the broker order book (client.order_report()) every 500ms for active strategy.
    If move_sl_to_cost is enabled and one leg's stoploss order transitions to a
    terminal complete/traded state, immediately triggers trigger_tbs_modify_stoploss_to_cost
    for the remaining leg(s).
    """
    log_system_event(f"[SL MONITOR ACTIVE] Started 500ms order-book polling (Move SL to Cost) for strategy ID '{strat_id}'...", "INFO")

    while True:
        try:
            strat = next((s for s in STRATEGIES_STORE if s["id"] == strat_id), None)
            if not strat or strat.get("status") != "ACTIVE":
                break

            if not strat.get("move_sl_to_cost", False):
                break

            legs = strat.get("legs", [])
            # Stop if all other legs already moved to cost or exited
            unhit_legs = [l for l in legs if not l.get("slHit", False)]
            if len(unhit_legs) <= 1 and any(l.get("slMovedToCost", False) for l in legs):
                break

            client = SESSION_DATA.get("client")
            if client is not None:
                order_book_res = None
                try:
                    order_book_res = client.order_report()
                except Exception:
                    pass

                orders_list = []
                if isinstance(order_book_res, dict):
                    orders_list = order_book_res.get("data") if isinstance(order_book_res.get("data"), list) else [order_book_res]
                elif isinstance(order_book_res, list):
                    orders_list = order_book_res

                # Map orders by order number
                orders_by_id = {}
                for o in orders_list:
                    if isinstance(o, dict):
                        ord_no = str(o.get("nOrdNo") or o.get("order_id") or o.get("orderNo") or "").strip()
                        if ord_no:
                            orders_by_id[ord_no] = o

                for idx, leg in enumerate(legs):
                    sl_ord_no = str(leg.get("slOrderNo", "")).strip()
                    if sl_ord_no and not leg.get("slHit", False):
                        broker_order = orders_by_id.get(sl_ord_no)
                        if broker_order:
                            ord_status = str(broker_order.get("ordSt") or broker_order.get("status") or "").strip().lower()
                            is_terminal = ord_status in ["complete", "traded", "executed", "filled"]
                            if not is_terminal:
                                try:
                                    qty = float(broker_order.get("qty") or 0)
                                    fld = float(broker_order.get("fldQty") or 0)
                                    if qty > 0 and fld >= qty:
                                        is_terminal = True
                                except Exception:
                                    pass

                            if is_terminal:
                                leg["slHit"] = True
                                leg["slStatus"] = "COMPLETE"
                                log_system_event(
                                    f"[SL ORDER TRADED] Leg #{idx+1} ({leg.get('executedSymbol')}) Stop-Loss order #{sl_ord_no} reached terminal state '{ord_status.upper()}'! Executing Move SL to Cost for remaining leg(s)...",
                                    "WARN"
                                )
                                trigger_tbs_modify_stoploss_to_cost(strat, idx, sl_ord_no)
                                break
            else:
                # Simulated paper-trade monitoring
                for idx, leg in enumerate(legs):
                    if not leg.get("slHit", False) and leg.get("executedSymbol"):
                        sym = leg["executedSymbol"]
                        curr_ltp = float(OPTION_LIVE_PRICES.get(sym, leg.get("executedEntryPrice", 0)))
                        pos = leg.get("position", "Sell").upper()
                        sl_trig = float(leg.get("slTriggerPrice", 0))
                        hit = False
                        if pos in ["SELL", "S"] and sl_trig > 0 and curr_ltp >= sl_trig:
                            hit = True
                        elif pos in ["BUY", "B"] and sl_trig > 0 and curr_ltp <= sl_trig:
                            hit = True

                        if hit:
                            leg["slHit"] = True
                            leg["slStatus"] = "COMPLETE"
                            log_system_event(
                                f"[SIMULATED SL HIT] Leg #{idx+1} ({sym}) Stop-Loss triggered @ ₹{curr_ltp:.2f} (Trigger: ₹{sl_trig:.2f})! Executing Move SL to Cost for remaining leg(s)...",
                                "WARN"
                            )
                            trigger_tbs_modify_stoploss_to_cost(strat, idx, "SIM-SL")
                            break
        except Exception:
            pass

        time.sleep(0.5)


def chase_and_confirm_exit_order(
    client,
    order_id: str,
    trading_symbol: str,
    close_tx_type: str,
    initial_limit_price: float,
    quantity: int,
    max_attempts: int = 4,
    check_interval: float = 1.5
) -> tuple:
    """
    Exit Order Chaser: Actively verifies that a square-off limit order gets filled on the exchange.
    If the order remains OPEN/UNFILLED after each check_interval, it queries the live market depth
    and calls client.modify_order() to step up (for BUY) or step down (for SELL) the price to the
    latest Best Ask/Bid until the order is 100% filled.

    Returns:
        (is_filled: bool, final_fill_price: float)
    """
    current_price = initial_limit_price
    is_buy = str(close_tx_type).upper() in ["BUY", "B"]
    offset_type = PROFILE_SETTINGS.get("omsLimitOffsetType", "Points (₹)")
    offset_val = float(PROFILE_SETTINGS.get("omsLimitOffsetValue", 0.5))

    for attempt in range(max_attempts + 1):
        # Allow matching engine to execute
        time.sleep(check_interval)

        # 1. Primary Check: Broker Live Positions (Did net quantity drop to 0?)
        try:
            pos_res = client.positions()
            if isinstance(pos_res, dict) and isinstance(pos_res.get("data"), list):
                for cp in pos_res["data"]:
                    if str(cp.get("trdSym", "")).strip() == trading_symbol:
                        net_q = 0
                        if "netQty" in cp and cp.get("netQty") is not None:
                            net_q = int(float(cp["netQty"]))
                        else:
                            net_q = int(float(cp.get("cfBuyQty", 0))) + int(float(cp.get("flBuyQty", 0))) - int(float(cp.get("cfSellQty", 0))) - int(float(cp.get("flSellQty", 0)))
                        if net_q == 0:
                            log_system_event(
                                f"[EXIT CHASER: CONFIRMED FILLED] {trading_symbol} net open quantity is 0 on broker positions! (Order #{order_id})",
                                "SUCCESS"
                            )
                            return True, current_price
        except Exception:
            pass

        # 2. Secondary Check: Order Status via client.order_report()
        try:
            ord_res = client.order_report(order_id=str(order_id))
            ord_data = None
            if isinstance(ord_res, dict):
                raw = ord_res.get("data")
                if isinstance(raw, list) and len(raw) > 0:
                    ord_data = raw[0]
                elif isinstance(raw, dict):
                    ord_data = raw
                elif ord_res.get("nOrdNo"):
                    ord_data = ord_res

            if ord_data and isinstance(ord_data, dict):
                st = str(ord_data.get("ordSt") or ord_data.get("status") or ord_data.get("orderStatus") or "").upper()
                avg_p = float(ord_data.get("avgPrc") or ord_data.get("avgPrice") or ord_data.get("flPrc") or current_price or 0.0)

                # Terminal complete states
                if st in ["TRAD", "COMPLETE", "EXECUTED", "FILLED"]:
                    log_system_event(
                        f"[EXIT CHASER: CONFIRMED FILLED] {trading_symbol} Order #{order_id} filled @ ₹{avg_p:.2f} (Status: {st})!",
                        "SUCCESS"
                    )
                    return True, avg_p

                if st in ["CAN", "CANCELLED", "REJ", "REJECTED"]:
                    log_system_event(
                        f"[EXIT CHASER NOTICE] {trading_symbol} Order #{order_id} ended with status: {st}.",
                        "WARN"
                    )
                    return False, current_price
        except Exception:
            pass

        # If we reached maximum chase attempts, break to final check
        if attempt >= max_attempts:
            break

        # 3. Order is still OPEN -> Query fresh depth and chase the price!
        depth = get_live_market_depth(trading_symbol)
        fresh_bid = float(depth.get("bid", 0.0) or 0.0)
        fresh_ask = float(depth.get("ask", 0.0) or 0.0)
        fresh_ltp = float(depth.get("ltp", 0.0) or 0.0)

        # Calculate next chased price
        if is_buy:
            # Closing short position: chase lowest seller (Ask)
            base_p = fresh_ask if fresh_ask > 0 else (fresh_ltp if fresh_ltp > 0 else current_price)
            step_offset = base_p * (offset_val / 100.0) if "Percent" in offset_type else offset_val
            target_p = base_p + step_offset
            # Step up at least 0.50 points higher than previous limit price
            new_price = max(target_p, current_price + 0.50)
        else:
            # Closing long position: chase highest buyer (Bid)
            base_p = fresh_bid if fresh_bid > 0 else (fresh_ltp if fresh_ltp > 0 else current_price)
            step_offset = base_p * (offset_val / 100.0) if "Percent" in offset_type else offset_val
            target_p = base_p - step_offset
            # Step down at least 0.50 points lower than previous limit price
            new_price = min(target_p, current_price - 0.50)

        new_price = max(0.05, round(round(new_price / 0.05) * 0.05, 2))

        log_system_event(
            f"[EXIT CHASER: RE-PRICING #{attempt+1}/{max_attempts}] {trading_symbol} Order #{order_id} still OPEN! Chasing limit price: ₹{current_price:.2f} -> ₹{new_price:.2f} (Depth: Bid ₹{fresh_bid:.2f}, Ask ₹{fresh_ask:.2f})...",
            "WARN"
        )

        try:
            mod_res = safe_modify_order_with_retry(
                client,
                order_id=str(order_id),
                price=str(new_price),
                order_type="L",
                quantity=str(quantity),
                validity="DAY"
            )
            if isinstance(mod_res, dict):
                st_code = mod_res.get("stCode")
                if st_code == 100003 or "complete" in str(mod_res.get("errMsg", "")).lower():
                    log_system_event(
                        f"[EXIT CHASER: ALREADY FILLED] {trading_symbol} Order #{order_id} filled while modifying!",
                        "SUCCESS"
                    )
                    return True, current_price
            current_price = new_price
        except Exception as me:
            log_system_event(f"Exit Chaser modify_order notice: {me}", "WARN")

    # Final positions check after all chase attempts
    try:
        pos_check = client.positions()
        if isinstance(pos_check, dict) and isinstance(pos_check.get("data"), list):
            for cp in pos_check["data"]:
                if str(cp.get("trdSym", "")).strip() == trading_symbol:
                    cq = int(float(cp.get("netQty", 0) if cp.get("netQty") is not None else 0))
                    if cq == 0:
                        return True, current_price
    except Exception:
        pass

    return False, current_price


def execute_strategy_exit(target_strat: dict, reason: str = "SCHEDULED_EXIT_TIME") -> dict:
    """
    Executes the 3-Step Strategy Exit and Square-Off Sequence:
    1. Cancels any open, unfilled stoploss orders (client.cancel_order()).
    2. Queries current price for active strategy contracts (via broker quotes / live feed).
    3. Places Limit Orders (order_type='L') to close net quantities of the remaining legs, squaring off positions.
    """
    was_executed = target_strat.get("isExecuted", False) or target_strat.get("status") in ["ACTIVE", "EXIT_PARTIAL", "EXITING"]

    if not was_executed:
        target_strat["status"] = "EXITED"
        target_strat["isExecuted"] = False
        target_strat["exitedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")
        target_strat["exitReason"] = reason
        log_system_event(
            f"Strategy '{target_strat['name']}' stopped before entry execution ({reason}). No broker orders placed.",
            "INFO"
        )
        return target_strat

    target_strat["status"] = "EXITING"
    target_strat["exitReason"] = reason

    sym_base = target_strat.get("symbol", "NIFTY")
    lots = int(target_strat.get("lotsPairs", 1))
    lot_sz = get_index_lot_size(sym_base)
    default_leg_qty = lots * lot_sz
    legs = target_strat.get("legs", [])
    client = SESSION_DATA.get("client")
    strat_prod = get_strategy_product_code(target_strat)

    log_system_event(
        f"[EXIT CYCLE INITIATED] Strategy '{target_strat['name']}' | Trigger: {reason} | Product: {strat_prod} | Commencing 3-step square-off sequence across {len(legs)} legs...",
        "WARN"
    )

    # -------------------------------------------------------------------------
    # STEP 1: PRE-FLIGHT VERIFICATION & SESSION KEEPALIVE FOR EXIT
    # -------------------------------------------------------------------------
    log_system_event(
        f"[EXIT STEP 1/3] Refreshing broker keepalive session and verifying active contracts for '{target_strat['name']}'...",
        "INFO"
    )
    if client is not None:
        try:
            refresh_client_http_session(client)
        except Exception:
            pass

    # Cancel SL orders ONLY for legs that were ALREADY closed earlier (e.g. SL hit earlier in the day)
    # Open legs retain their Stop-Loss until their square-off order is confirmed filled in Step 3!
    for idx, leg in enumerate(legs):
        leg_sym = leg.get("executedSymbol", "")
        if (leg.get("slHit", False) or leg.get("isClosed", False) or leg.get("slStatus") == "COMPLETE") and leg_sym:
            cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))

    # -------------------------------------------------------------------------
    # STEP 2: RESOLVE CURRENT MARKET PRICES FROM REAL-TIME WEBSOCKET (0ms Latency)
    # -------------------------------------------------------------------------
    log_system_event(
        f"[EXIT STEP 2/3] Resolving real-time market prices from live WebSocket feed...",
        "INFO"
    )

    for idx, leg in enumerate(legs):
        if not leg.get("executedSymbol"):
            strike, ltp_c, _, trd_sym_c = resolve_leg_strike_and_price(
                sym_base, leg, target_strat["id"], idx, strategy_expiry=target_strat.get("strategyExpiry")
            )
            opt_type_code = "CE" if leg.get("optionType", "Call").title() in ["Call", "CE"] else "PE"
            leg["executedSymbol"] = trd_sym_c or f"{sym_base}26SEP{strike}{opt_type_code}"
            leg["executedStrike"] = strike

    for idx, leg in enumerate(legs):
        leg_sym = leg.get("executedSymbol", "")
        queried_price = 0.0

        # Instant in-memory WebSocket lookup (0ms latency, zero REST calls!)
        if leg_sym in OPTION_LIVE_PRICES and OPTION_LIVE_PRICES[leg_sym] > 0:
            queried_price = float(OPTION_LIVE_PRICES[leg_sym])
        elif str(leg.get("executedStrike", 0)) in OPTION_LIVE_PRICES:
            queried_price = float(OPTION_LIVE_PRICES[str(leg.get("executedStrike"))])
        else:
            qual = preselect_qualifying_leg_contract(sym_base, leg, strategy_expiry=target_strat.get("strategyExpiry"))
            queried_price = float(qual.get("option_ltp", leg.get("executedEntryPrice", 140.0)))

        queried_price = round(max(0.05, queried_price), 2)
        leg["currentExitLTP"] = queried_price
        if not (leg.get("slHit", False) or leg.get("isClosed", False)) or float(leg.get("executedExitPrice", 0) or 0) <= 0:
            leg["executedExitPrice"] = queried_price
        log_system_event(
            f"[EXIT STEP 2/3: PRICE QUERIED] Leg #{idx+1} ({leg_sym}): Live WebSocket LTP = ₹{queried_price:.2f}",
            "INFO"
        )

    # -------------------------------------------------------------------------
    # STEP 3: PLACE LIMIT ORDERS CONCURRENTLY (Using Pre-Cached Positions)
    # -------------------------------------------------------------------------
    log_system_event(
        f"[EXIT STEP 3/3] Calculating net open quantities and placing Limit Square-Off Orders concurrently...",
        "INFO"
    )

    # Fetch live broker positions using fresh or pre-warmed cache (0ms latency!)
    broker_positions_map = get_cached_broker_positions(client, max_age_sec=5.0)

    total_realized_pnl = 0.0

    def close_single_leg(idx, leg):
        leg_sym = leg.get("executedSymbol", "")
        leg_pos = leg.get("position", "Sell").upper()
        leg_lots = int(leg.get("lots", lots))
        configured_qty = leg_lots * lot_sz
        entry_price = float(leg.get("executedEntryPrice", 0.0))
        exit_price = float(leg.get("executedExitPrice", entry_price))

        # Determine remaining net open quantity
        net_qty = 0
        close_tx_type = "B" if leg_pos in ["SELL", "S"] else "S"

        # Check if leg was already closed via Stop-Loss or marked closed
        leg_already_closed = (
            leg.get("slHit", False) or
            leg.get("slStatus") == "COMPLETE" or
            (leg.get("isClosed", False) and not leg.get("exitFailed", False)) or
            leg.get("isSquaredOff", False)
        )

        if leg_sym in broker_positions_map:
            b_net = broker_positions_map[leg_sym].get("computedNetQty", 0)
            if b_net != 0:
                net_qty = abs(b_net)
                close_tx_type = "S" if b_net > 0 else "B"
                leg_already_closed = False
            else:
                net_qty = 0
                leg_already_closed = True
        elif leg_already_closed:
            net_qty = 0
        else:
            net_qty = configured_qty
            close_tx_type = "B" if leg_pos in ["SELL", "S"] else "S"

        # Calculate Leg P&L
        if leg_pos in ["SELL", "S"]:
            leg_pnl = (entry_price - exit_price) * configured_qty
        else:
            leg_pnl = (exit_price - entry_price) * configured_qty
        leg["realizedPnL"] = round(leg_pnl, 2)

        # If net quantity is 0, this leg has already been squared off
        if net_qty <= 0 or leg_already_closed:
            leg["isClosed"] = True
            leg["exitFailed"] = False
            log_system_event(
                f"[EXIT STEP 3/3: LEG SQUARED-OFF] Leg #{idx+1} ({leg_sym}): Net open quantity is 0 (already closed via Stoploss/Trade). Skipping order placement. P&L: {'+' if leg_pnl>=0 else ''}₹{leg_pnl:.2f}",
                "INFO"
            )
            if client is not None and leg_sym:
                cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))
            return (leg_pnl, True)

        # Remaining net quantity exists -> Place Limit Order to Square Off
        limit_price = calculate_exit_limit_order_price(exit_price, close_tx_type, trading_symbol=leg_sym)
        action_label = "BUY" if close_tx_type == "B" else "SELL"
        seg = "bse_fo" if sym_base in ["SENSEX", "BANKEX"] else "nse_fo"
        if leg_sym in SYMBOL_TO_TOKEN_MAP and SYMBOL_TO_TOKEN_MAP[leg_sym][1]:
            seg = SYMBOL_TO_TOKEN_MAP[leg_sym][1]

        log_system_event(
            f"[EXIT STEP 3/3: SQUARE-OFF ORDER] Leg #{idx+1} ({leg_sym}): Placing Limit Order (L) to close net quantity {net_qty} ({action_label} @ Limit ₹{limit_price:.2f}, LTP: ₹{exit_price:.2f})...",
            "WARN"
        )

        if client is not None:
            success = False
            for attempt in range(1, 3):
                try:
                    res = safe_place_order_with_retry(
                        client,
                        exchange_segment=seg,
                        trading_symbol=leg_sym,
                        transaction_type=close_tx_type,
                        product=strat_prod,
                        order_type="L",
                        quantity=str(net_qty),
                        price=str(limit_price),
                        validity="DAY"
                    )
                    if isinstance(res, dict) and (res.get("nOrdNo") or res.get("stat") == "Ok"):
                        ord_id = res.get("nOrdNo") or res.get("result")
                        leg["exitOrderNo"] = str(ord_id)
                        log_system_event(
                            f"[EXIT ORDER SUBMITTED] Leg #{idx+1} ({leg_sym}): Broker Order #{ord_id} placed @ Limit ₹{limit_price:.2f}. Launching Exit Order Chaser...",
                            "INFO"
                        )
                        is_filled, final_fill_p = chase_and_confirm_exit_order(
                            client=client,
                            order_id=str(ord_id),
                            trading_symbol=leg_sym,
                            close_tx_type=close_tx_type,
                            initial_limit_price=limit_price,
                            quantity=net_qty,
                            max_attempts=4,
                            check_interval=1.5
                        )
                        if is_filled:
                            leg["isClosed"] = True
                            leg["exitFailed"] = False
                            leg["executedExitPrice"] = final_fill_p
                            log_system_event(
                                f"[EXIT ORDER FILLED] Leg #{idx+1} ({leg_sym}) squared off successfully @ ₹{final_fill_p:.2f}! Broker Order: #{ord_id}",
                                "SUCCESS"
                            )
                            # Once this leg's order gets exited at exit time -> cancel all pending SL orders for this exited leg:
                            cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))
                            success = True
                            break
                        else:
                            log_system_event(
                                f"[EXIT CHASER TIMEOUT] Leg #{idx+1} ({leg_sym}) order #{ord_id} could not be confirmed filled after chaser retries (Attempt #{attempt}).",
                                "WARN"
                            )
                    elif isinstance(res, dict) and (res.get("stat") == "Not_Ok" or res.get("stCode") == 100008):
                        log_system_event(
                            f"KOTAK API SQUARE OFF REJECTED: 401 Unauthorized (stCode: 100008). Session token expired!",
                            "ERROR"
                        )
                        break
                    else:
                        log_system_event(f"[EXIT ORDER NOTICE] Leg #{idx+1} ({leg_sym}) Attempt #{attempt}: {res}", "WARN")
                except Exception as e:
                    log_system_event(f"Live Broker Square Off Order Exception for {leg_sym} (Attempt #{attempt}): {e}", "ERROR")
                    # Check whether order was accepted by broker despite the HTTP timeout
                    time.sleep(1.0)
                    try:
                        pos_check = client.positions()
                        p_list = pos_check.get("data") if isinstance(pos_check, dict) and isinstance(pos_check.get("data"), list) else []
                        for cp in p_list:
                            if cp.get("trdSym") == leg_sym:
                                cq = int(float(cp.get("cfBuyQty", 0))) + int(float(cp.get("flBuyQty", 0))) - int(float(cp.get("cfSellQty", 0))) - int(float(cp.get("flSellQty", 0)))
                                if "netQty" in cp and cp.get("netQty") is not None:
                                    cq = int(float(cp["netQty"]))
                                if cq == 0:
                                    leg["isClosed"] = True
                                    leg["exitFailed"] = False
                                    log_system_event(f"[EXIT CONFIRMED] Leg #{idx+1} ({leg_sym}) verified closed on broker positions despite timeout!", "SUCCESS")
                                    # Cancel pending SL for this exited leg
                                    cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))
                                    success = True
                                    break
                    except Exception:
                        pass
                    if success:
                        break

            if not success:
                leg["isClosed"] = False
                leg["exitFailed"] = True
                log_system_event(
                    f"[CRITICAL ERROR] Leg #{idx+1} ({leg_sym}) FAILED to square off due to broker timeout/rejection! Position is STILL OPEN. Stop-Loss order remains active to protect open leg. Please square off via Manual Desk!",
                    "ERROR"
                )
                return (leg_pnl, False)
            return (leg_pnl, True)
        else:
            # Paper trading / simulation mode
            leg["exitOrderNo"] = f"SIM-EXIT-{idx+1}"
            leg["isClosed"] = True
            leg["exitFailed"] = False
            cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))
            log_system_event(
                f"[SIMULATED SQUARE-OFF] Leg #{idx+1} ({leg_sym}): Squared off {net_qty} Qty ({action_label}) @ ₹{limit_price:.2f} (LTP: ₹{exit_price:.2f}). P&L: {'+' if leg_pnl>=0 else ''}₹{leg_pnl:.2f}",
                "SUCCESS"
            )
            return (leg_pnl, True)

    num_workers = min(4, max(1, len(legs)))
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(close_single_leg, idx, leg) for idx, leg in enumerate(legs)]
        for fut in futures:
            try:
                leg_pnl_val, is_closed_val = fut.result()
                total_realized_pnl += leg_pnl_val
            except Exception as fe:
                log_system_event(f"Square-off worker error: {fe}", "ERROR")

    # Post-execution safety pass: cancel pending SL only for confirmed closed legs (leaving open legs untouched)
    if client is not None:
        for idx, leg in enumerate(legs):
            leg_sym = leg.get("executedSymbol", "")
            if leg.get("isClosed", False) and not leg.get("exitFailed", False) and leg_sym:
                cancel_pending_sl_orders_for_exited_leg(client, leg, leg_sym, strat_name=target_strat.get("name", "Strategy"))

    target_strat["finalMTM"] = round(total_realized_pnl, 2)
    target_strat["liveMTM"] = round(total_realized_pnl, 2)
    sign = "+" if total_realized_pnl >= 0 else "-"

    all_closed = all(leg.get("isClosed", False) for leg in legs)
    if all_closed:
        if "STOPLOSS" in reason.upper():
            target_strat["status"] = "STOPLOSS_HIT"
        else:
            target_strat["status"] = "EXITED"
        target_strat["isExecuted"] = False
        target_strat["exitedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")
        log_system_event(
            f"[EXIT CYCLE COMPLETE] Strategy '{target_strat['name']}' fully squared off! Final Realized P&L: {sign}₹{abs(total_realized_pnl):.2f} (Reason: {reason})",
            "SUCCESS"
        )
    else:
        target_strat["status"] = "EXIT_PARTIAL"
        target_strat["isExecuted"] = True
        log_system_event(
            f"[EXIT CYCLE INCOMPLETE] Strategy '{target_strat['name']}' has UNCLOSED legs due to broker timeout! Status: EXIT_PARTIAL. Position remains active. Re-click Stop / Square Off to retry.",
            "ERROR"
        )
    return target_strat


def start_background_scheduler():
    """Starts background thread to monitor strategy pre-warming, scheduled entry, scheduled exit, overall target/SL & per-leg SL."""
    def scheduler_loop():
        while True:
            try:
                now = datetime.datetime.now()

                for s in list(STRATEGIES_STORE):
                    status = s.get("status", "SAVED")

                    # 1. SCHEDULED ENTRY & PRE-WARMING (Only for ARMED or PRE_WARMING strategies!)
                    if status in ["ARMED", "PRE_WARMING"]:
                        entry_type = s.get("entryType", "Time Based")
                        if entry_type == "Time Based":
                            entry_time = s.get("entryTime", "09:20")
                            try:
                                eh, em = map(int, entry_time.split(":"))
                                target_entry = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                                diff_entry = (target_entry - now).total_seconds()

                                # 20 seconds prior to entry time: Trigger 250ms Pre-Warming!
                                if 0 <= diff_entry <= 20 and status == "ARMED":
                                    s["status"] = "PRE_WARMING"
                                    msg = f"20s before entry time ({entry_time}) reached for '{s['name']}'! Initiating 250ms Pre-Warming Fast Loop..."
                                    log_system_event(msg, "WARN")
                                    threading.Thread(target=run_prewarm_fast_loop, args=(s["id"], max(1, int(diff_entry))), daemon=True).start()

                                # At entry time (diff <= 0): Execute Entry!
                                elif diff_entry <= 0 and status in ["PRE_WARMING", "ARMED"]:
                                    msg = f"Scheduled Entry Time ({entry_time}) Fired for '{s['name']}'! Executing entry with pre-selected zero-latency contracts!"
                                    log_system_event(msg, "INFO")
                                    execute_strategy_entry(s)
                            except Exception as e:
                                log_system_event(f"Scheduler entry error: {e}", "ERROR")

                    # 2. OVERALL TARGET, OVERALL STOP-LOSS & PER-LEG SL MONITORING (For ACTIVE strategies)
                    elif status == "ACTIVE":
                        sym_base = s.get("symbol", "NIFTY")
                        lots_pairs = int(s.get("lotsPairs", 1))
                        lot_sz = get_index_lot_size(sym_base)
                        legs = s.get("legs", [{}])
                        
                        total_mtm = 0.0
                        any_leg_sl_hit = False

                        for idx, leg in enumerate(legs):
                            leg_pos = leg.get("position", "Sell").upper()
                            leg_lots = int(leg.get("lots", 1))
                            qty = leg_lots * lot_sz
                            
                            leg_entry_p = float(leg.get("executedEntryPrice", s.get("entryPrice", 140.0)))
                            qualifying = preselect_qualifying_leg_contract(sym_base, leg, strategy_expiry=s.get("strategyExpiry"))
                            current_ltp = float(qualifying.get("option_ltp", leg_entry_p))
                            
                            if leg_pos in ["BUY", "B"]:
                                leg_pnl = (current_ltp - leg_entry_p) * qty
                            else:
                                leg_pnl = (leg_entry_p - current_ltp) * qty
                                
                            total_mtm += leg_pnl

                            sl_enabled = leg.get("slEnable", True)
                            if sl_enabled and leg_entry_p > 0:
                                sl_val = float(leg.get("slValue", 0) or 0)
                                sl_type = str(leg.get("slType", "Points (Pts)"))
                                if sl_val > 0:
                                    max_allowed_loss_pts = sl_val if "Points" in sl_type else leg_entry_p * (sl_val / 100.0)
                                    if leg_pos in ["SELL", "S"]:
                                        if (current_ltp - leg_entry_p) >= max_allowed_loss_pts:
                                            any_leg_sl_hit = True
                                    else:
                                        if (leg_entry_p - current_ltp) >= max_allowed_loss_pts:
                                            any_leg_sl_hit = True

                        s["liveMTM"] = round(total_mtm, 2)

                        # Periodic 10-Second Live Position Progress Log in Terminal
                        now_ts = time.time()
                        if now_ts - s.get("_last_monitor_ts", 0) >= 10:
                            s["_last_monitor_ts"] = now_ts

                            # Continuous Broker Position Price Reconciliation (Keeps MTM Penny-Synced)
                            client = SESSION_DATA.get("client")
                            if client is not None:
                                try:
                                    b_positions = get_cached_broker_positions(client, max_age_sec=6.0)
                                    for leg in legs:
                                        leg_sym = leg.get("executedSymbol", "")
                                        if leg_sym in b_positions:
                                            p_info = b_positions[leg_sym]
                                            pos_dir = leg.get("position", "Sell").upper()
                                            avg_k = "sellAvgPrc" if pos_dir in ["SELL", "S"] else "buyAvgPrc"
                                            b_avg = float(p_info.get(avg_k) or p_info.get("avgPrc") or 0.0)
                                            if b_avg > 0 and abs(b_avg - float(leg.get("executedEntryPrice", 0.0))) >= 0.01:
                                                leg["executedEntryPrice"] = round(b_avg, 2)
                                                if leg == legs[0]:
                                                    s["entryPrice"] = round(b_avg, 2)
                                except Exception:
                                    pass

                            first_leg = legs[0] if legs else {}
                            exec_sym = first_leg.get("executedSymbol", sym_base)
                            entry_p = float(first_leg.get("executedEntryPrice", s.get("entryPrice", 0)))
                            qualifying = preselect_qualifying_leg_contract(sym_base, first_leg, strategy_expiry=s.get("strategyExpiry"))
                            curr_ltp = float(qualifying.get("option_ltp", entry_p))
                            sl_info = "SL: None"
                            if first_leg.get("slEnable") and float(first_leg.get("slValue", 0)) > 0:
                                sl_val = float(first_leg["slValue"])
                                sl_type = str(first_leg.get("slType", "Points"))
                                sl_lvl = entry_p + sl_val if first_leg.get("position", "Sell").upper().startswith("S") else entry_p - sl_val
                                dist = abs(curr_ltp - sl_lvl)
                                sl_info = f"SL: ₹{sl_lvl:.2f} (Dist: {dist:.2f} pts)"
                            
                            sign = "+" if total_mtm >= 0 else "-"
                            pts_diff = (curr_ltp - entry_p) if first_leg.get("position", "Sell").upper().startswith("B") else (entry_p - curr_ltp)
                            pts_sign = "+" if pts_diff >= 0 else ""
                            log_system_event(
                                f"[LIVE MONITOR] '{s['name']}' | Contract: {exec_sym} @ ₹{curr_ltp:.2f} (Entry: ₹{entry_p:.2f} | {pts_sign}{pts_diff:.2f} pts) | "
                                f"MTM: {sign}₹{abs(total_mtm):.2f} | {sl_info} | Exit @ {s.get('exitTime')}",
                                "INFO"
                            )
                        
                        # Overall Stop Loss Check (Rs.)
                        overall_sl = float(s.get("overallStopLoss", 0) or 0)
                        if overall_sl > 0 and total_mtm <= -overall_sl:
                            log_msg = f"OVERALL STOP LOSS HIT! Strategy '{s['name']}' MTM loss reached -₹{abs(total_mtm):.2f} (Limit: -₹{overall_sl:.2f}). Squaring off..."
                            log_system_event(log_msg, "ERROR")
                            execute_strategy_exit(s, reason=f"OVERALL_STOPLOSS_HIT (-₹{overall_sl:.2f})")
                            continue

                        # Overall Target Profit Check (Rs.)
                        overall_target = float(s.get("overallTargetProfit", 0) or 0)
                        if overall_target > 0 and total_mtm >= overall_target:
                            log_msg = f"OVERALL TARGET PROFIT HIT! Strategy '{s['name']}' MTM profit reached +₹{total_mtm:.2f} (Target: +₹{overall_target:.2f}). Squaring off..."
                            log_system_event(log_msg, "SUCCESS")
                            execute_strategy_exit(s, reason=f"OVERALL_TARGET_HIT (+₹{overall_target:.2f})")
                            continue

                        # Individual Leg SL Hit Check
                        if any_leg_sl_hit:
                            sq_type = s.get("squareOffType", "Partial")
                            if ("Partial" in sq_type or s.get("move_sl_to_cost")) and len(legs) > 1:
                                # When Move SL to Cost or Partial square-off is enabled,
                                # the 500ms poller monitors and adjusts the remaining leg to cost.
                                pass
                            else:
                                log_system_event(f"LEG STOP LOSS HIT for '{s['name']}'! Squaring off...", "WARN")
                                execute_strategy_exit(s, reason="LEG_STOPLOSS_HIT")
                                continue

                        # Scheduled Time Exit Check
                        exit_type = s.get("exitType", "Time Based")
                        if exit_type == "Time Based":
                            exit_time = s.get("exitTime", "15:15")
                            try:
                                xh, xm = map(int, exit_time.split(":"))
                                target_exit = now.replace(hour=xh, minute=xm, second=0, microsecond=0)
                                diff_exit = (target_exit - now).total_seconds()

                                # 5-second exit pre-warming: pre-cache live positions in background so T=0 is 0ms!
                                client = SESSION_DATA.get("client")
                                if 0 < diff_exit <= 5.0 and client is not None:
                                    if time.time() - LAST_BROKER_POSITIONS_CACHE.get("timestamp", 0) > 4.0:
                                        threading.Thread(target=get_cached_broker_positions, args=(client, 0.0), daemon=True).start()

                                if diff_exit <= 0:
                                    log_system_event(f"Scheduled Exit Time ({exit_time}) reached for '{s['name']}'! Squaring off...", "INFO")
                                    execute_strategy_exit(s, reason="SCHEDULED_EXIT_TIME")
                            except Exception:
                                pass
            except Exception:
                pass

            # High-precision sleep: switches to 50ms fast loop during 10s countdowns for sub-second precision
            min_sleep = 1.0
            for s in list(STRATEGIES_STORE):
                st = s.get("status")
                if st in ["ARMED", "PRE_WARMING", "ACTIVE"]:
                    if st in ["ARMED", "PRE_WARMING"] and s.get("entryType") == "Time Based":
                        try:
                            eh, em = map(int, s.get("entryTime", "09:20").split(":"))
                            te = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                            de = (te - now).total_seconds()
                            if 0 <= de <= 10.0:
                                min_sleep = min(min_sleep, 0.05)
                        except Exception:
                            pass
                    if st == "ACTIVE" and s.get("exitType") == "Time Based":
                        try:
                            xh, xm = map(int, s.get("exitTime", "15:15").split(":"))
                            tx = now.replace(hour=xh, minute=xm, second=0, microsecond=0)
                            dx = (tx - now).total_seconds()
                            if 0 <= dx <= 10.0:
                                min_sleep = min(min_sleep, 0.05)
                        except Exception:
                            pass

            time.sleep(min_sleep)

    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()


def attempt_auto_login_on_startup():
    """Automatically logs into Kotak Neo API on server startup if environment / .env credentials exist."""
    ck = config("NEO_CONSUMER_KEY", default="").strip()
    mob = config("NEO_MOBILE_NUMBER", default="").strip()
    ucc = config("NEO_UCC", default="").strip()
    mpin = config("NEO_MPIN", default="").strip()
    totp = config("NEO_TOTP", default="").strip()

    if ck and mob and ucc and mpin and totp and NeoAPI is not None:
        try:
            print(f"[*] [AUTO-LOGIN] Attempting Kotak Neo Authentication on server startup for UCC: {ucc}...")
            client = NeoAPI(consumer_key=ck, environment="prod")
            l_res = client.totp_login(mobile_number=mob, ucc=ucc, totp=totp)
            v_res = client.totp_validate(mpin=mpin)
            greeting_name = ucc
            if isinstance(v_res, dict) and "data" in v_res:
                greeting_name = v_res["data"].get("greetingName", ucc)
            SESSION_DATA["client"] = client
            SESSION_DATA["user"] = {"displayName": greeting_name, "ucc": ucc, "loginTime": time.strftime("%Y-%m-%d %H:%M:%S")}
            start_ws_index_listener(client)
            sync_master_scrips_in_background(client)
            log_system_event(f"Kotak Neo WebSocket auto-authenticated for {ucc}! Live streaming active.", "SUCCESS")
        except Exception as e:
            print(f"[-] Auto-login startup notice: {e}")


def open_browser_in_chrome():
    """Automatically opens the trading dashboard in Google Chrome upon server launch."""
    url = "http://127.0.0.1:5000"
    chrome_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    ]
    import webbrowser
    for path in chrome_paths:
        if os.path.isfile(path):
            try:
                webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(path))
                webbrowser.get("chrome").open(url)
                return
            except Exception:
                pass
    webbrowser.open(url)


if __name__ == "__main__":
    attempt_auto_login_on_startup()
    start_background_scheduler()
    threading.Timer(1.25, open_browser_in_chrome).start()
    print("\n" + "=" * 60)
    print("      TRADING DASHBOARD WEB SERVER RUNNING")
    print("      Access App at: http://127.0.0.1:5000")
    print("=" * 60 + "\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
