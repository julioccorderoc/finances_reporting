import os
import time
import json
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from binance.spot import Spot
from binance.error import ClientError

load_dotenv()

# --- CONFIGURATION ---
DAYS_TO_LOOKBACK = 8
TIMEZONE = "America/Caracas"
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# Initialize Client
client = Spot(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)


# --- SYNC LOGIC ---
def get_server_offset():
    """
    Calculates the exact difference between your machine and Binance.
    """
    try:
        server_time = client.time()["serverTime"]
        local_time = int(time.time() * 1000)
        offset = server_time - local_time
        print("--- Clock Sync ---")
        print(f"Server Time: {server_time}")
        print(f"Local Time:  {local_time}")
        print(f"Offset:      {offset}ms")
        return offset
    except Exception as e:
        print(f"Clock sync failed: {e}")
        return 0


# Calculate once at startup
TIME_OFFSET = get_server_offset()


def get_timestamp():
    """
    Apply the offset to local time.
    We subtract 1000ms buffer
    """
    return int(time.time() * 1000) + TIME_OFFSET - 1000


def get_time_range(days):
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    return int(start_time.timestamp() * 1000), int(end_time.timestamp() * 1000)


# --- FETCH ENGINE ---
def fetch_data(fetch_func, name, use_page_pagination=False, **params):
    print(f"--- Fetching {name} ---")
    data = []
    page = 1
    limit = 100

    # 10s Window to allow for network jitter
    params["recvWindow"] = 10000

    try:
        while True:
            # Refresh timestamp for every single loop iteration
            params["timestamp"] = get_timestamp()
            current_params = params.copy()

            if use_page_pagination:
                current_params["current"] = page
                current_params["page"] = page
                current_params["size"] = limit
                current_params["rows"] = limit

            response = fetch_func(**current_params)

            results = []
            if isinstance(response, dict):
                if "data" in response:
                    results = response["data"]
                elif "rows" in response:
                    results = response["rows"]
                elif "list" in response:
                    results = response["list"]
            elif isinstance(response, list):
                results = response

            if not results:
                break
            data.extend(results)

            if len(results) < (limit / 2):
                break
            if not use_page_pagination:
                break

            page += 1
            time.sleep(0.1)

    except AttributeError:
        print(f"⚠️  Skipping {name}: Library method not found.")
    except ClientError as e:
        print(f"❌ Error {name}: {e}")
    except Exception as e:
        print(f"❌ System Error {name}: {e}")

    return data


# --- FORMATTING ---
def format_record(raw_ms, account, operation, coin, amount, remark):
    try:
        # 1. Convert ms to UTC Timestamp
        ts_utc = pd.to_datetime(raw_ms, unit="ms", utc=True)

        # 2. Convert UTC -> Caracas
        ts_local = ts_utc.tz_convert(TIMEZONE)

        # 3. Format string based on LOCAL time
        date_str = ts_local.strftime("%d-%b-%Y")
    except:
        date_str = "Unknown"
        ts_utc = 0

    return {
        "RawTime": raw_ms,
        "Date": date_str,
        "Account": account,
        "Operation": operation,
        "Coin": coin,
        "Amount": float(amount),
        "Remark": str(remark),
    }


def main():
    start_ts, end_ts = get_time_range(DAYS_TO_LOOKBACK)
    ledger = []

    # 1. P2P (Funding)
    p2p_buy = fetch_data(
        client.c2c_trade_history,
        "P2P Buy",
        True,
        tradeType="BUY",
        startTimestamp=start_ts,
        endTimestamp=end_ts,
    )
    for r in p2p_buy:
        ledger.append(
            format_record(
                r.get("createTime"),
                "Funding",
                "P2P-Buy",
                r.get("asset"),
                r.get("amount"),
                f"Order: {r.get('orderNumber')}",
            )
        )

    p2p_sell = fetch_data(
        client.c2c_trade_history,
        "P2P Sell",
        True,
        tradeType="SELL",
        startTimestamp=start_ts,
        endTimestamp=end_ts,
    )
    for r in p2p_sell:
        ledger.append(
            format_record(
                r.get("createTime"),
                "Funding",
                "P2P-Sell",
                r.get("asset"),
                -float(r.get("amount")),
                f"Order: {r.get('orderNumber')}",
            )
        )

    # 2. Deposits & Withdrawals (Spot)
    deps = fetch_data(
        client.deposit_history, "Deposits", startTime=start_ts, endTime=end_ts
    )
    for r in deps:
        ledger.append(
            format_record(
                r.get("insertTime"),
                "Spot",
                "Deposit",
                r.get("coin"),
                r.get("amount"),
                f"Net: {r.get('network')}",
            )
        )

    withs = fetch_data(
        client.withdraw_history, "Withdrawals", startTime=start_ts, endTime=end_ts
    )
    for r in withs:
        ledger.append(
            format_record(
                r.get("applyTime"),
                "Spot",
                "Withdrawal",
                r.get("coin"),
                -float(r.get("amount")),
                f"Addr: {r.get('address')}",
            )
        )

    # 3. Convert (Spot)
    converts = fetch_data(
        client.get_convert_trade_history, "Convert", startTime=start_ts, endTime=end_ts
    )
    for r in converts:
        remark = f"Converted to {r.get('toAmount')} {r.get('toAsset')}"
        ledger.append(
            format_record(
                r.get("createTime"),
                "Spot",
                "Binance Convert",
                r.get("fromAsset"),
                -float(r.get("fromAmount")),
                remark,
            )
        )

    # 4. Internal Transfers (Consolidated Single Entry)
    transfers = fetch_data(
        client.user_universal_transfer_history,
        "Internal Transfers",
        type="MAIN_FUNDING",
        startTime=start_ts,
        endTime=end_ts,
    )
    transfers.extend(
        fetch_data(
            client.user_universal_transfer_history,
            "Internal Transfers",
            type="FUNDING_MAIN",
            startTime=start_ts,
            endTime=end_ts,
        )
    )

    for r in transfers:
        t_type = r.get("type")
        if t_type == "MAIN_FUNDING":
            acc = "Funding"
            amt = float(r.get("amount"))
        else:
            acc = "Spot"
            amt = float(r.get("amount"))

        ledger.append(
            format_record(
                r.get("timestamp"),
                acc,
                "Internal Transfer",
                r.get("asset"),
                amt,
                "User Transfer",
            )
        )

    # 5. Earn (Spot/Earn)
    if hasattr(client, "simple_earn_flexible_rewards_history"):
        rewards = fetch_data(
            client.simple_earn_flexible_rewards_history,
            "Earn Rewards",
            True,
            startTime=start_ts,
            endTime=end_ts,
        )
        for r in rewards:
            ledger.append(
                format_record(
                    r.get("time"),
                    "Earn",
                    "Interest",
                    r.get("asset"),
                    r.get("rewards"),
                    r.get("type"),
                )
            )

    # 6. Binance Pay (Direct Sends/Receives)
    if hasattr(client, "pay_history"):
        pay_recs = fetch_data(
            client.pay_history, "Binance Pay", startTime=start_ts, endTime=end_ts
        )

        for r in pay_recs:
            # Pay records structure: {transactionTime, amount, currency, orderType...}
            # We want to capture money leaving (PAY) or entering (C2C_HOLD/RECEIVE)
            p_type = r.get("orderType")
            amount = float(r.get("amount", 0))
            coin = r.get("currency")

            # Identify direction
            if p_type == "PAY":
                amount = -amount  # Outflow
                op_label = "Send"
            else:
                op_label = "Send"

            note = f"Counterparty: {r.get('counterpartyId', 'Hidden')}"

            ledger.append(
                format_record(
                    r.get("transactionTime"), "Funding", op_label, coin, amount, note
                )
            )
    else:
        print(
            "⚠️  Skipping Binance Pay: Method 'pay_history' not found. (Check library version)"
        )

    # --- OUTPUT ---
    if not ledger:
        print("No transactions found.")
        return

    df = pd.DataFrame(ledger)

    # Sort by the hidden 'RawTime' column (Oldest to Newest)
    df = df.sort_values(by="RawTime", ascending=True)

    # Remove the helper column so it doesn't appear in the CSV
    df = df.drop(columns=["RawTime"])

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    base_name = f"binance_{today_str}_{DAYS_TO_LOOKBACK}"

    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    json_path = os.path.join(output_dir, f"{base_name}.json")

    df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, indent=4, default=str)

    print("\n✅ SUCCESS")
    print(f"📁 CSV: {csv_path}")
    print(f"📁 JSON: {json_path}")


if __name__ == "__main__":
    main()
