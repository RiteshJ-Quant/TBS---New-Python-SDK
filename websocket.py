"""
Kotak Neo API - Live Futures & Options (F&O) WebSocket Streaming (websocket.py)

This script:
1. Downloads and saves Master Scrip CSV files (nse_fo.csv, bse_fo.csv, mcx_fo.csv, etc.)
   directly into your workspace folder: C:\\Users\\USER\\Desktop\\Trading\\Learn to Code\\TBS - New Python SDK
2. Parses F&O contract master data (Futures, Call Options, Put Options).
3. Authenticates via login.py and opens an async SFeed WebSocket connection.
4. Subscribes to live market feeds (LTP, Open, High, Low, Close, Change, Volume, OI)
   for selected F&O contracts and streams live price ticks in real time.

Usage:
    python websocket.py
"""

import asyncio
import os
import sys
import pandas as pd
import httpx
from typing import List, Dict, Any, Tuple, Optional
from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import SFeedWebSocket, WsToken, SFeedScrip, SFeedScripLite
from login import login

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DIR = os.path.join(BASE_DIR, "master_scrips")


def download_master_scrips(client: NeoAPI) -> Dict[str, str]:
    """
    Downloads Master Scrip CSV files from Kotak Neo API and saves them
    directly into the workspace directory as well as master_scrips/ subfolder.
    """
    os.makedirs(MASTER_DIR, exist_ok=True)
    print("\n" + "=" * 65)
    print("        Downloading Master Scrip Files from Kotak Neo Server")
    print("=" * 65)

    res = client.scrip_master()
    downloaded_files = {}

    if isinstance(res, dict) and "filesPaths" in res:
        urls = res["filesPaths"]
        print(f"Found {len(urls)} Master Scrip files on broker server.")

        for url in urls:
            raw_filename = url.split("/")[-1]
            # Clean filename (e.g. nse_cm-v1.csv -> nse_cm.csv)
            clean_filename = raw_filename.replace("-v1", "")

            root_path = os.path.join(BASE_DIR, clean_filename)
            sub_path = os.path.join(MASTER_DIR, clean_filename)

            print(f" -> Downloading {clean_filename}...")
            try:
                r = httpx.get(url, timeout=60.0)
                if r.status_code == 200:
                    # Save to root workspace directory
                    with open(root_path, "wb") as f:
                        f.write(r.content)
                    # Save to subfolder master_scrips/
                    with open(sub_path, "wb") as f:
                        f.write(r.content)

                    downloaded_files[clean_filename] = root_path
                    size_mb = len(r.content) / (1024 * 1024)
                    print(f"    [+] Saved: {root_path} ({size_mb:.2f} MB)")
                else:
                    print(f"    [-] Download failed for {clean_filename}: HTTP Status {r.status_code}")
            except Exception as e:
                print(f"    [-] Download Error for {clean_filename}: {e}")

        print("=" * 65 + "\n")
    else:
        print(f"[-] Could not retrieve master scrip file paths: {res}")

    return downloaded_files


def load_fo_df(csv_path: str) -> pd.DataFrame:
    """Loads and cleans the F&O master CSV dataframe."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Master scrip file not found: {csv_path}")

    # Read CSV
    df = pd.read_csv(csv_path, low_memory=False)

    # Clean column names (strip leading/trailing whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Handle strike price column
    if "dStrikePrice;" in df.columns:
        df.rename(columns={"dStrikePrice;": "dStrikePrice"}, inplace=True)

    if "dStrikePrice" in df.columns:
        df["strike_price"] = pd.to_numeric(df["dStrikePrice"], errors="coerce") / 100.0
    else:
        df["strike_price"] = 0.0

    return df


def select_fo_tokens(df: pd.DataFrame) -> List[Tuple[str, str, str]]:
    """
    Interactive prompt to select F&O tokens from loaded master scrip dataframe.
    Returns list of tuples: (token, trading_symbol, exchange_segment)
    """
    print("\n" + "=" * 65)
    print("            SELECT F&O CONTRACTS FOR LIVE STREAMING")
    print("=" * 65)
    print(" 1. Popular Index Contracts (NIFTY & BANKNIFTY Futures + Top Options)")
    print(" 2. Search & Select F&O Contracts by Symbol Name (e.g. RELIANCE, NIFTY, SBIN)")
    print(" 3. Enter Custom Instrument Tokens or Trading Symbols")
    print("=" * 65)

    choice = input("\nSelect option (1-3) [Default: 1]: ").strip()
    selected_scrips = []

    if choice in ("", "1"):
        # Filter popular NIFTY & BANKNIFTY contracts
        indices = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
        print("\nFinding active Futures and Options for NIFTY, BANKNIFTY & FINNIFTY...")
        sub_df = df[df["pSymbolName"].isin(indices)]

        # Take futures and top 10 options
        fut_df = sub_df[sub_df["pOptionType"] == "XX"].head(6)
        opt_df = sub_df[sub_df["pOptionType"].isin(["CE", "PE"])].head(10)

        combined = pd.concat([fut_df, opt_df])
        for _, row in combined.iterrows():
            selected_scrips.append((str(row["pSymbol"]), str(row["pTrdSymbol"]), str(row["pExchSeg"])))

    elif choice == "2":
        sym_query = input("\nEnter underlying symbol name (e.g. RELIANCE, NIFTY, BANKNIFTY, SBIN): ").strip().upper()
        if not sym_query:
            sym_query = "NIFTY"

        match_df = df[df["pSymbolName"].str.upper() == sym_query]
        if match_df.empty:
            print(f"No exact match for '{sym_query}'. Searching partial matches...")
            match_df = df[df["pSymbolName"].str.upper().str.contains(sym_query, na=False)]

        if match_df.empty:
            print(f"No contracts found for query '{sym_query}'.")
            return []

        print(f"\nFound {len(match_df):,} contracts for '{sym_query}'.")
        print("Filter by Contract Type:")
        print("  1. Futures Only (FUT)")
        print("  2. Call Options Only (CE)")
        print("  3. Put Options Only (PE)")
        print("  4. All Contracts (Futures + Options)")
        type_choice = input("Select Type (1-4) [Default: 4]: ").strip()

        if type_choice == "1":
            match_df = match_df[match_df["pOptionType"] == "XX"]
        elif type_choice == "2":
            match_df = match_df[match_df["pOptionType"] == "CE"]
        elif type_choice == "3":
            match_df = match_df[match_df["pOptionType"] == "PE"]

        display_df = match_df.head(20)
        print(f"\nMatching Contracts (Showing top {len(display_df)}):")
        for idx, row in display_df.reset_index(drop=True).iterrows():
            stk = f" | Strike: {row['strike_price']:.2f}" if row['pOptionType'] in ('CE', 'PE') else ""
            print(f"  {idx+1}. {row['pTrdSymbol']} (Token: {row['pSymbol']}{stk})")

        selection = input("\nEnter contract numbers to subscribe (e.g. 1,2,3 or press Enter for top 10): ").strip()
        if selection.lower() == "all" or not selection:
            for _, row in display_df.head(10).iterrows():
                selected_scrips.append((str(row["pSymbol"]), str(row["pTrdSymbol"]), str(row["pExchSeg"])))
        else:
            parts = [p.strip() for p in selection.split(",") if p.strip().isdigit()]
            for p in parts:
                idx = int(p) - 1
                if 0 <= idx < len(display_df):
                    row = display_df.iloc[idx]
                    selected_scrips.append((str(row["pSymbol"]), str(row["pTrdSymbol"]), str(row["pExchSeg"])))

    elif choice == "3":
        custom_in = input("\nEnter token numbers or trading symbols separated by commas: ").strip()
        parts = [p.strip() for p in custom_in.split(",") if p.strip()]
        for p in parts:
            selected_scrips.append((p, p, "nse_fo"))

    return selected_scrips


async def stream_live_prices(client: NeoAPI, scrip_tuples: List[Tuple[str, str, str]]):
    """
    Connects to Kotak Neo SFeed WebSocket and streams real-time F&O price ticks.
    """
    if not scrip_tuples:
        print("No scrips selected for live streaming.")
        return

    tokens_to_sub = [WsToken(segment, token) for token, sym, segment in scrip_tuples]

    print("\n" + "=" * 75)
    print("            KOTAK NEO SFEED WEBSOCKET LIVE PRICE FEED")
    print("=" * 75)
    print(f"Subscribing to {len(tokens_to_sub)} F&O contracts:")
    for token, sym, segment in scrip_tuples:
        print(f"  - {sym:<25} (Token: {token}, Segment: {segment})")
    print("=" * 75)
    print("Press Ctrl+C at any time to stop streaming.\n")

    # Create SFeed WebSocket connection from logged-in client session
    async with client.create_websocket() as ws:
        # Batch-subscribe all tokens in a single frame
        await ws.subscribe_scrips(tokens_to_sub)
        print("[+] Subscribed successfully! Streaming real-time market ticks...\n")

        try:
            async for message in ws:
                if isinstance(message, SFeedScrip):
                    sym = getattr(message, "trading_symbol", None) or getattr(message, "instrument_token", "N/A")
                    ltp = getattr(message, "last_traded_price", 0.0)
                    chg = getattr(message, "net_change", 0.0)
                    chg_pct = getattr(message, "net_change_percent", 0.0)
                    vol = getattr(message, "volume_traded_today", 0)
                    oi = getattr(message, "open_interest", 0)
                    open_p = getattr(message, "open_price", 0.0)
                    high_p = getattr(message, "high_price", 0.0)
                    low_p = getattr(message, "low_price", 0.0)
                    close_p = getattr(message, "close_price", 0.0)

                    sign = "+" if chg >= 0 else ""
                    print(
                        f"⚡ [{sym:<24}] LTP: {ltp:>10.2f} | Chg: {sign}{chg:>6.2f} ({sign}{chg_pct:.2f}%) "
                        f"| O:{open_p:.2f} H:{high_p:.2f} L:{low_p:.2f} C:{close_p:.2f} | Vol:{vol:,} | OI:{oi:,}"
                    )
                elif isinstance(message, SFeedScripLite):
                    sym = getattr(message, "trading_symbol", None) or getattr(message, "instrument_token", "N/A")
                    ltp = getattr(message, "last_traded_price", 0.0)
                    chg_pct = getattr(message, "net_change_percent", 0.0)
                    print(f"⚡ [{sym:<24}] LTP: {ltp:>10.2f} | Chg%: {chg_pct:.2f}%")
        except asyncio.CancelledError:
            print("\nStreaming session ended.")
        except Exception as err:
            print(f"\nWebSocket streaming error: {err}")


def get_master_scrip_path(filename: str = "nse_fo.csv") -> Optional[str]:
    """Checks if master scrip file exists in master_scrips/ subfolder or root folder."""
    sub_path = os.path.join(MASTER_DIR, filename)
    root_path = os.path.join(BASE_DIR, filename)

    if os.path.exists(sub_path):
        return sub_path
    if os.path.exists(root_path):
        return root_path
    return None


def main():
    print("=" * 75)
    print("     Kotak Neo API - Live Futures & Options (F&O) WebSocket Streamer")
    print("=" * 75)

    # Step 1: Login via login.py
    try:
        client = login()
    except Exception as e:
        print(f"\n[ERROR] Authentication failed: {e}")
        sys.exit(1)

    # Step 2: Check & Download Master Scrip Files automatically if missing
    existing_path = get_master_scrip_path("nse_fo.csv")

    if not existing_path:
        print("\nMaster Scrip CSV file 'nse_fo.csv' not found locally. Downloading automatically...")
        downloaded = download_master_scrips(client)
        nse_fo_path = downloaded.get("nse_fo.csv") or get_master_scrip_path("nse_fo.csv")
    else:
        print(f"\n[+] Using existing Master Scrip file: {existing_path}")
        nse_fo_path = existing_path

    # Step 3: Load Master Scrip Dataframe
    print("\nLoading F&O Master Scrip data into memory...")
    try:
        df = load_fo_df(nse_fo_path)
        print(f"[+] Successfully loaded {len(df):,} F&O contracts from {os.path.basename(nse_fo_path)}")
    except Exception as e:
        print(f"[-] Failed to load master scrip CSV: {e}")
        sys.exit(1)

    # Step 4: Select F&O Scrips for Live Feed
    scrips_to_stream = select_fo_tokens(df)
    if not scrips_to_stream:
        print("No scrips selected. Exiting.")
        sys.exit(0)

    # Step 5: Start Async WebSocket Streaming
    try:
        asyncio.run(stream_live_prices(client, scrips_to_stream))
    except KeyboardInterrupt:
        print("\nLive price streaming stopped by user.")


if __name__ == "__main__":
    main()
