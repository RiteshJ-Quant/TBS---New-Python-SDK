"""
Kotak Neo API - NIFTY & SENSEX Full Option Chain Viewer (optionchain.py)

This script fetches and displays the option chain for both NIFTY (NSE)
and SENSEX (BSE) for their latest expiry directly from Kotak Neo API in clean tabular format.

Usage:
    python optionchain.py
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any

# Silence raw JSON logging from SDK stdout before importing NeoAPI
os.environ["NEO_LOG_LEVEL"] = "NOLOG"
logging.getLogger("neo_api_client").setLevel(logging.WARNING)

from neo_api_client import NeoAPI
import pandas as pd

# Optional load from .env / environment
try:
    from decouple import config
except ImportError:
    def config(key: str, default: str = "") -> str:
        return os.environ.get(key, default)


def get_consumer_key() -> str:
    key = config("NEO_CONSUMER_KEY", default="").strip()
    if not key:
        key = input("Enter Consumer Key: ").strip()
    return key


def format_option_chain_table(data: Dict[str, Any], title: str):
    """Formats and prints an Option Chain in a clean tabular view."""
    if not isinstance(data, dict):
        print(f"[-] Invalid response format for {title}: {data}")
        return

    # Handle both wrapped and unwrapped response dicts
    chain_data = data.get("data") if isinstance(data.get("data"), dict) else data

    spot_info = chain_data.get("spot", {})
    common_info = chain_data.get("common_data", {})
    fut_info = chain_data.get("future", {})

    spot_ltp = float(spot_info.get("ltp", 0.0)) if spot_info.get("ltp") else 0.0
    expiry_date = common_info.get("expiryDt", "N/A")
    mkt_lot = common_info.get("mktLot", "N/A")
    fut_ltp = float(fut_info.get("ltp", 0.0)) if fut_info.get("ltp") else 0.0

    call_list = chain_data.get("call", [])
    put_list = chain_data.get("put", [])

    if not call_list and not put_list:
        print(f"[-] No option chain contracts returned for {title}.")
        return

    # Map puts by strike price for side-by-side alignment
    put_dict = {}
    for item in put_list:
        inst = item.get("inst", {})
        stk_str = inst.get("strkPrc") or inst.get("strikePrice")
        if stk_str:
            try:
                stk_val = float(stk_str)
                put_dict[stk_val] = item
            except ValueError:
                pass

    tot_call_oi = 0
    tot_put_oi = 0
    tot_call_vol = 0
    tot_put_vol = 0

    table_data = []

    for c_item in call_list:
        c_inst = c_item.get("inst", {})
        c_quote = c_item.get("quote", {})
        c_oi_data = c_item.get("oi") or c_item.get("openInterest") or {}

        stk_str = c_inst.get("strkPrc") or c_inst.get("strikePrice") or "0"
        try:
            stk = float(stk_str)
        except ValueError:
            continue

        c_ltp = float(c_quote.get("ltp", 0.0)) if c_quote.get("ltp") else 0.0
        c_vol = int(c_quote.get("vol", c_quote.get("volume", 0))) if c_quote.get("vol") or c_quote.get("volume") else 0
        c_oi = int(c_oi_data.get("cur", c_oi_data.get("current", 0))) if c_oi_data.get("cur") or c_oi_data.get("current") else 0
        c_mney = (c_inst.get("moneyness") or c_inst.get("optType") or "").upper()

        tot_call_oi += c_oi
        tot_call_vol += c_vol

        # Get matching Put
        p_item = put_dict.get(stk, {})
        p_inst = p_item.get("inst", {})
        p_quote = p_item.get("quote", {})
        p_oi_data = p_item.get("oi") or p_item.get("openInterest") or {}

        p_ltp = float(p_quote.get("ltp", 0.0)) if p_quote.get("ltp") else 0.0
        p_vol = int(p_quote.get("vol", p_quote.get("volume", 0))) if p_quote.get("vol") or p_quote.get("volume") else 0
        p_oi = int(p_oi_data.get("cur", p_oi_data.get("current", 0))) if p_oi_data.get("cur") or p_oi_data.get("current") else 0
        p_mney = (p_inst.get("moneyness") or p_inst.get("optType") or "").upper()

        tot_put_oi += p_oi
        tot_put_vol += p_vol

        is_atm = " [ATM]" if (c_mney == "ATM" or p_mney == "ATM") else ""
        strike_label = f"{stk:.2f}{is_atm}"

        table_data.append({
            "CALL OI": f"{c_oi:,}",
            "CALL VOL": f"{c_vol:,}",
            "CALL LTP": f"{c_ltp:.2f}",
            "CALL MON": c_mney,
            "STRIKE": strike_label,
            "PUT MON": p_mney,
            "PUT LTP": f"{p_ltp:.2f}",
            "PUT VOL": f"{p_vol:,}",
            "PUT OI": f"{p_oi:,}",
        })

    # Convert to pandas DataFrame for tabular presentation
    df = pd.DataFrame(table_data)

    # Calculate DTE if valid expiry date
    dte_str = ""
    if expiry_date and expiry_date != "N/A":
        for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"]:
            try:
                exp_dt = datetime.datetime.strptime(expiry_date.strip(), fmt).date()
                dte_val = max(0, (exp_dt - datetime.date.today()).days)
                dte_str = f" | {dte_val} DTE"
                break
            except Exception:
                pass

    print("\n" + "=" * 115)
    print(f"                       {title.upper()} OPTION CHAIN (EXPIRY: {expiry_date}{dte_str})")
    print("=" * 115)
    print(f" Spot Price: {spot_ltp:,.2f}  |  Futures Price: {fut_ltp:,.2f}  |  Market Lot: {mkt_lot}")
    print("=" * 115)

    # Configure pandas display for full table view
    pd.set_option("display.max_rows", 200)
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.width", 130)
    pd.set_option("display.colheader_justify", "right")

    print(df.to_string(index=False))

    print("-" * 115)
    pcr_oi = tot_put_oi / tot_call_oi if tot_call_oi > 0 else 0.0
    pcr_vol = tot_put_vol / tot_call_vol if tot_call_vol > 0 else 0.0
    print(f" TOTAL CALL OI : {tot_call_oi:>14,}   |   TOTAL PUT OI : {tot_put_oi:>14,}   |   PCR (OI)  : {pcr_oi:.2f}")
    print(f" TOTAL CALL VOL: {tot_call_vol:>14,}   |   TOTAL PUT VOL: {tot_put_vol:>14,}   |   PCR (VOL) : {pcr_vol:.2f}")
    print("=" * 115 + "\n")


def main():
    consumer_key = get_consumer_key()
    if not consumer_key:
        print("[-] Consumer Key required.")
        sys.exit(1)

    print("\nConnecting to Kotak Neo API...")
    client = NeoAPI(consumer_key=consumer_key, environment="prod")

    # 1. NIFTY Option Chain
    print("\nFetching NIFTY Option Chain (NSE F&O)...")
    try:
        nifty_chain = client.option_chain(exchange="nse_fo", underlying="NIFTY", count=30)
        format_option_chain_table(nifty_chain, "NIFTY 50 (NSE)")
    except Exception as e:
        print(f"[-] Error fetching NIFTY option chain: {e}")

    # 2. SENSEX Option Chain
    print("\nFetching SENSEX Option Chain (BSE F&O)...")
    try:
        sensex_chain = client.option_chain(exchange="bse_fo", underlying="SENSEX", count=30)
        format_option_chain_table(sensex_chain, "SENSEX (BSE)")
    except Exception as e:
        print(f"[-] Error fetching SENSEX option chain: {e}")


if __name__ == "__main__":
    main()
