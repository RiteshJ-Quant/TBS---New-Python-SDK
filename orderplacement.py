"""
Kotak Neo API - Interactive Order Placement Script (orderplacement.py)

This script authenticates with Kotak Neo API using login.py,
interactively prompts the user for all necessary order parameters (Segment, Symbol, Qty, Order Type, Price, Product, etc.),
provides an order confirmation summary, and places the order with the Kotak Neo broker.

Usage:
    python orderplacement.py
"""

import sys
from typing import Optional, Dict, Any, List, Tuple
from neo_api_client import NeoAPI
from login import login


def prompt_choice(prompt_text: str, options: List[Tuple[str, str]], default_index: int = 0) -> str:
    """Helper function to prompt user to select from a numbered list of options."""
    print(f"\n{prompt_text}:")
    for idx, (code, desc) in enumerate(options, 1):
        default_flag = " (Default)" if idx - 1 == default_index else ""
        print(f"  {idx}. {desc} [{code}]{default_flag}")

    while True:
        user_in = input(f"Select option (1-{len(options)}) [Default: {default_index + 1}]: ").strip()
        if not user_in:
            return options[default_index][0]
        if user_in.isdigit():
            val = int(user_in)
            if 1 <= val <= len(options):
                return options[val - 1][0]
        
        # Check if user directly typed code string (e.g. "nse_cm", "B", "CNC")
        user_in_lower = user_in.lower()
        for code, desc in options:
            if user_in_lower == code.lower():
                return code
        print(f"Invalid selection. Please enter a number between 1 and {len(options)}.")


def search_symbol_helper(client: NeoAPI) -> str:
    """Helper function to search for trading symbols using search_scrip API."""
    print("\n--- Scrip Search Helper ---")
    segment = prompt_choice(
        "Select Segment for Search",
        [
            ("nse_cm", "NSE Equity Cash"),
            ("nse_fo", "NSE Derivatives (F&O)"),
            ("bse_cm", "BSE Equity Cash"),
            ("mcx_fo", "MCX Commodities"),
        ],
        default_index=0,
    )
    keyword = input("Enter search keyword (e.g., RELIANCE, NIFTY, TATAMOTORS): ").strip()
    if not keyword:
        return ""

    try:
        print(f"Searching scrips for '{keyword}' in segment '{segment}'...")
        results = client.search_scrip(exchange_segment=segment, symbol=keyword)
        if isinstance(results, list) and len(results) > 0:
            print(f"\nFound {len(results)} matching scrips (showing top 10):")
            display_count = min(10, len(results))
            for i in range(display_count):
                item = results[i]
                symbol = item.get("pTrdSymbol", item.get("pSymbol", "N/A"))
                name = item.get("pSymbolName", "")
                token = item.get("pSymbol", "")
                print(f"  {i+1}. {symbol} (Token: {token}, Name: {name})")

            choice = input(f"\nSelect scrip number (1-{display_count}) or press Enter to skip: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= display_count:
                selected_symbol = results[int(choice) - 1].get("pTrdSymbol", "")
                print(f"-> Selected Trading Symbol: {selected_symbol}")
                return selected_symbol
        elif isinstance(results, dict) and "error" in results:
            print(f"Search API notice: {results['error']}")
        else:
            print("No matching scrips found.")
    except Exception as e:
        print(f"Search failed: {e}")
    return ""


def place_broker_order(
    client: NeoAPI,
    exchange_segment: str,
    trading_symbol: str,
    transaction_type: str,
    product: str,
    order_type: str,
    quantity: str,
    price: str = "0",
    trigger_price: str = "0",
    validity: str = "DAY",
    amo: str = "NO",
    disclosed_quantity: str = "0",
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Places an order via Kotak Neo API client."""
    order_kwargs = {
        "exchange_segment": exchange_segment,
        "trading_symbol": trading_symbol,
        "transaction_type": transaction_type,
        "product": product,
        "order_type": order_type,
        "quantity": str(quantity),
        "price": str(price),
        "trigger_price": str(trigger_price),
        "validity": validity,
        "amo": amo,
        "disclosed_quantity": str(disclosed_quantity),
    }
    if tag:
        order_kwargs["tag"] = tag

    print("\nSending order placement request to Kotak Neo broker...")
    response = client.place_order(**order_kwargs)
    return response


def main():
    print("=" * 60)
    print("       Kotak Neo API - Interactive Order Placement")
    print("=" * 60)

    # Step 1: Authenticate with Kotak Neo broker
    try:
        client = login()
    except Exception as e:
        print(f"\n[ERROR] Authentication failed: {e}")
        sys.exit(1)

    # Step 2: Interactive Order Placement Loop
    while True:
        print("\n" + "-" * 60)
        print("                 ORDER PLACEMENT FORM")
        print("-" * 60)

        # 1. Exchange Segment
        exchange_segment = prompt_choice(
            "1. Select Exchange Segment",
            [
                ("nse_cm", "NSE Cash / Equity (nse_cm)"),
                ("nse_fo", "NSE Futures & Options (nse_fo)"),
                ("bse_cm", "BSE Cash / Equity (bse_cm)"),
                ("bse_fo", "BSE Futures & Options (bse_fo)"),
                ("mcx_fo", "MCX Commodities (mcx_fo)"),
            ],
            default_index=0,
        )

        # 2. Trading Symbol
        print("\n2. Trading Symbol:")
        print("   - Enter exact trading symbol (e.g., RELIANCE-EQ, INFY-EQ, NIFTY26SEP24000CE)")
        print("   - Or type 's' to search for scrip")
        sym_input = input("Enter Trading Symbol (or 's' to search): ").strip()

        if sym_input.lower() in ("s", "search"):
            trading_symbol = search_symbol_helper(client)
            if not trading_symbol:
                trading_symbol = input("Enter Trading Symbol manually (e.g. RELIANCE-EQ): ").strip()
        else:
            trading_symbol = sym_input

        while not trading_symbol:
            trading_symbol = input("Trading Symbol cannot be empty. Enter symbol (e.g. RELIANCE-EQ): ").strip()

        # 3. Transaction Type (BUY / SELL)
        transaction_type = prompt_choice(
            "3. Select Transaction Type",
            [
                ("B", "BUY"),
                ("S", "SELL"),
            ],
            default_index=0,
        )

        # 4. Product Type
        product = prompt_choice(
            "4. Select Product Type",
            [
                ("CNC", "CNC - Cash & Carry (Delivery for Equity)"),
                ("MIS", "MIS - Margin Intraday Square-off (Intraday)"),
                ("NRML", "NRML - Normal (F&O Overnight)"),
                ("MTF", "MTF - Margin Trading Facility"),
            ],
            default_index=0,
        )

        # 5. Order Type
        order_type = prompt_choice(
            "5. Select Order Type",
            [
                ("L", "L - Limit Order"),
                ("MKT", "MKT - Market Order"),
                ("SL", "SL - Stop Loss Limit Order"),
                ("SL-M", "SL-M - Stop Loss Market Order"),
            ],
            default_index=0,
        )

        # 6. Quantity
        while True:
            qty_str = input("\n6. Enter Quantity (e.g., 1, 10, 50): ").strip()
            if qty_str.isdigit() and int(qty_str) > 0:
                quantity = qty_str
                break
            print("Invalid quantity. Must be a positive integer.")

        # 7. Price (Limit price required for L and SL)
        price = "0"
        if order_type in ("L", "SL"):
            while True:
                price_input = input(f"\n7. Enter Limit Price for {order_type} order (e.g., 1500.50): ").strip()
                try:
                    p_val = float(price_input)
                    if p_val > 0:
                        price = f"{p_val:.2f}"
                        break
                except ValueError:
                    pass
                print("Invalid price. Limit orders require a price greater than 0.")

        # 8. Trigger Price (Required for SL and SL-M)
        trigger_price = "0"
        if order_type in ("SL", "SL-M"):
            while True:
                trig_input = input(f"\n8. Enter Trigger Price for {order_type} order (e.g., 1490.00): ").strip()
                try:
                    t_val = float(trig_input)
                    if t_val > 0:
                        trigger_price = f"{t_val:.2f}"
                        break
                except ValueError:
                    pass
                print("Invalid trigger price. Stop loss orders require a trigger price greater than 0.")

        # 9. Validity
        validity = prompt_choice(
            "9. Select Validity",
            [
                ("DAY", "DAY (Valid for full trading day)"),
                ("IOC", "IOC (Immediate or Cancel)"),
            ],
            default_index=0,
        )

        # 10. AMO (After Market Order)
        amo = prompt_choice(
            "10. Is this an After Market Order (AMO)?",
            [
                ("NO", "NO - Regular Market Hours Order"),
                ("YES", "YES - After Market Order (AMO)"),
            ],
            default_index=0,
        )

        # 11. Optional Tag
        tag_input = input("\n11. Enter optional Tag / Strategy Marker (or press Enter to skip): ").strip()
        tag = tag_input if tag_input else None

        # --- ORDER SUMMARY & CONFIRMATION ---
        print("\n" + "=" * 60)
        print("                 ORDER CONFIRMATION SUMMARY")
        print("=" * 60)
        print(f" Exchange Segment : {exchange_segment}")
        print(f" Trading Symbol   : {trading_symbol}")
        print(f" Action           : {'BUY' if transaction_type == 'B' else 'SELL'}")
        print(f" Product          : {product}")
        print(f" Order Type       : {order_type}")
        print(f" Quantity         : {quantity}")
        print(f" Price            : {price if price != '0' else 'MARKET (0.00)'}")
        if trigger_price != "0":
            print(f" Trigger Price    : {trigger_price}")
        print(f" Validity         : {validity}")
        print(f" AMO              : {amo}")
        if tag:
            print(f" Tag              : {tag}")
        print("=" * 60)

        confirm = input("\nDo you want to PLACE this order with Kotak Neo? (yes/no) [Default: yes]: ").strip().lower()
        if confirm in ("", "y", "yes"):
            try:
                res = place_broker_order(
                    client=client,
                    exchange_segment=exchange_segment,
                    trading_symbol=trading_symbol,
                    transaction_type=transaction_type,
                    product=product,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    trigger_price=trigger_price,
                    validity=validity,
                    amo=amo,
                    tag=tag,
                )

                print("\n" + "=" * 60)
                print("                 BROKER RESPONSE")
                print("=" * 60)
                if isinstance(res, dict):
                    stat = res.get("stat") or res.get("status")
                    order_no = res.get("nOrdNo") or res.get("order_id") or res.get("data", {}).get("nOrdNo")

                    if stat == "Ok" or res.get("stCode") == 200 or order_no:
                        print(f" [+] SUCCESS! Order Placed Successfully.")
                        print(f"     Order Number (nOrdNo): {order_no}")
                        print(f"     Status: {stat}")
                    elif "error" in res:
                        print(f" [-] ORDER REJECTED / FAILED:")
                        print(f"     Error: {res['error']}")
                    else:
                        print(f" Response: {res}")
                else:
                    print(f" Response: {res}")
                print("=" * 60)

            except Exception as err:
                print(f"\n[-] Order Placement Exception: {err}")
        else:
            print("\nOrder cancelled by user.")

        another = input("\nWould you like to place another order? (y/n) [Default: n]: ").strip().lower()
        if another not in ("y", "yes"):
            print("\nExiting Order Placement tool. Goodbye!")
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOrder placement cancelled by user.")
        sys.exit(0)
