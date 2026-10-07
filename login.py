"""
Kotak Neo API - Authentication Script (login.py)

This script prompts the user for Kotak Neo API credentials (or loads them from .env/environment),
asks for the current 6-digit TOTP from your authenticator app, and logs into the Kotak Neo broker.

Requirements:
- Consumer Key (Generated from NEO App -> More -> Trade API)
- Mobile Number (Registered mobile with country code, e.g., +91XXXXXXXXXX)
- UCC (User Client Code)
- MPIN (Trading PIN)
- TOTP (6-digit code from Google Authenticator / Authy)
"""

import getpass
import os
import sys
from typing import Optional
from neo_api_client import NeoAPI

# Optional load from .env file using python-decouple or os.getenv
try:
    from decouple import config
except ImportError:
    def config(key: str, default: str = "") -> str:
        return os.environ.get(key, default)


def get_credential(env_key: str, prompt_text: str) -> str:
    """Helper function to fetch a credential from env or prompt the user via standard input."""
    val = config(env_key, default="").strip()
    if val:
        print(f"[*] Found {env_key} in environment: {val}")
        return val

    return input(f"Enter {prompt_text}: ").strip()


def login(
    consumer_key: Optional[str] = None,
    mobile_number: Optional[str] = None,
    ucc: Optional[str] = None,
    mpin: Optional[str] = None,
    totp: Optional[str] = None,
    environment: str = "prod",
) -> NeoAPI:
    """
    Authenticates with Kotak Neo API using 2-step TOTP + MPIN authentication flow.

    Returns:
        NeoAPI: An authenticated NeoAPI client instance ready to execute orders and fetch data.
    """
    print("\n" + "=" * 55)
    print("         Kotak Neo API Broker Authentication")
    print("=" * 55 + "\n")

    # 1. Gather missing credentials interactively
    if not consumer_key:
        consumer_key = get_credential("NEO_CONSUMER_KEY", "Consumer Key")

    if not mobile_number:
        mobile_number = get_credential(
            "NEO_MOBILE_NUMBER",
            "Mobile Number (e.g. +919876543210)",
        )
        # Auto-prefix +91 if 10 digit Indian number is entered without country code
        if mobile_number and not mobile_number.startswith("+"):
            if len(mobile_number) == 10 and mobile_number.isdigit():
                mobile_number = "+91" + mobile_number
                print(f"-> Formatted Mobile Number: {mobile_number}")

    if not ucc:
        ucc = get_credential("NEO_UCC", "UCC (User Client Code)")

    if not mpin:
        mpin = get_credential("NEO_MPIN", "MPIN")

    # 2. Ask for 6-digit TOTP
    if not totp:
        totp = input("Enter 6-digit TOTP from your authenticator app: ").strip()

    # 3. Validate inputs
    if not consumer_key or not mobile_number or not ucc or not mpin or not totp:
        raise ValueError(
            "Missing required credentials. All fields (Consumer Key, Mobile, UCC, MPIN, TOTP) are required."
        )

    # 4. Initialize NeoAPI client
    print("\n[1/2] Initializing client & sending TOTP login request...")
    client = NeoAPI(
        consumer_key=consumer_key,
        environment=environment,
    )

    try:
        login_res = client.totp_login(
            mobile_number=mobile_number,
            ucc=ucc,
            totp=totp,
        )
        if isinstance(login_res, dict) and "error" in login_res:
            raise RuntimeError(f"API Error in Step 1: {login_res['error']}")

        print("[+] Step 1 (TOTP Login) Successful!")
        if isinstance(login_res, dict) and "data" in login_res:
            greeting = login_res["data"].get("greetingName")
            if greeting:
                print(f"    Welcome, {greeting}!")
    except Exception as e:
        print(f"[-] Step 1 (TOTP Login) Failed: {e}")
        raise e

    # 5. Complete authentication with MPIN
    print("\n[2/2] Validating MPIN for full trade token session...")
    try:
        validate_res = client.totp_validate(mpin=mpin)

        # Check if response returned error dictionary
        if isinstance(validate_res, dict) and "error" in validate_res:
            err_msg = validate_res["error"]
            if isinstance(err_msg, list) and len(err_msg) > 0:
                err_details = err_msg[0].get("message", str(err_msg))
            else:
                err_details = str(err_msg)
            raise RuntimeError(f"MPIN Validation Error: {err_details}")

        print("[+] Step 2 (MPIN Validation) Successful!")

        print("\n" + "=" * 55)
        print("      LOGIN SUCCESSFUL - Session Token Active!")
        print("=" * 55)

        if isinstance(validate_res, dict) and "data" in validate_res:
            data = validate_res["data"]
            print(f"Client Name : {data.get('greetingName', 'N/A')}")
            print(f"UCC         : {data.get('ucc', ucc)}")
            print(f"Data Center : {data.get('dataCenter', 'N/A')}")
            print(f"Token Type  : {data.get('kType', 'N/A')}")
            print("=" * 55 + "\n")

        return client

    except Exception as e:
        print(f"[-] Step 2 (MPIN Validation) Failed: {e}")
        raise e


if __name__ == "__main__":
    try:
        client_session = login()

        # Quick session check: fetch limits or holdings
        print("Verifying session active status by fetching account limits...")
        try:
            limits = client_session.limits()
            print("Account Limits fetched successfully!")
        except Exception as err:
            print(f"Notice: Could not fetch limits ({err}), session was still created.")

    except KeyboardInterrupt:
        print("\n\nLogin process cancelled by user.")
        sys.exit(0)
    except Exception as err:
        print(f"\n[ERROR] Login failed: {err}")
        sys.exit(1)
