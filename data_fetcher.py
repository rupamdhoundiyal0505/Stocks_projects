import os 
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd 
import time
from fyers_apiv3 import fyersModel
from datetime import timezone

CLIENT_ID="KUC4376MFF-100"
ACCESS_TOKEN_FILE="access_token.txt"
SYMBOL="NSE:NIFTY50-INDEX"
RESOLUTION="10"
MAX_DAYS_PER_CALL = 90

IST = timezone(timedelta(hours = 5, minutes=30))
def _load_access_token():
    if not os.path.exists(ACCESS_TOKEN_FILE):
        raise FileNotFoundError(f"Access token file {ACCESS_TOKEN_FILE} not found")

    with open(ACCESS_TOKEN_FILE, "r") as f:
        return f.read().strip()

def get_fyers_client():
    token = _load_access_token()
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=token,
        is_async=False,
        log_path=""
    )
    return fyers

def fetch_historical_data(
    start_date: str,
    end_date: str,
    symbol: str = SYMBOL,
    resolution: str =RESOLUTION,
    fyers: Optional[fyersModel.SessionModel] = None,

)->pd.DataFrame:
    if fyers is None:
        fyers = get_fyers_client()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    all_candles = []

    chunk_start = start_dt

    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_CALL), end_dt) 
        data = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",
            "range_from": chunk_start.strftime("%Y-%m-%d"),
            "range_to": chunk_end.strftime("%Y-%m-%d"),
            "cont_flag": "1",

        }       
        resp = fyers.history(data = data)
        if resp.get("s") != "ok":
            raise RuntimeError(f"Failed to fetch data: {resp.get('err_r')}")
        
        candles = resp.get("candles", [])
        if candles:
            all_candles.extend(candles)
            print(f"{chunk_start} to {chunk_end} fetched successfully. No. of DP {len(candles)}")

        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(0.3)

    if not all_candles:
        raise ValueError("No data fetched")

    df  = pd.DataFrame(
        all_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["datetime"]=pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(IST).dt.tz_localize(None)
    df = df.drop(columns=["timestamp"])
    df = df[["datetime", "open", "high", "low", "close", "volume"]]
    df = df.sort_values("datetime").reset_index(drop=True)


    df=df[
        (df["datetime"].dt.time >= pd.Timestamp("09:15").time()) &
        (df["datetime"].dt.time <= pd.Timestamp("15:30").time())
    ].reset_index(drop=True)


    print(f"Total data points: {len(df)}")
    print(f" FROM : {df['datetime'].iloc[0]} to {df['datetime'].iloc[-1]}")
    return df


if __name__ == "__main__":
    df = fetch_historical_data(
        "2026-01-01",
        "2026-03-01")

    print(df.tail())
    df.to_csv("nifty_10_mins.csv", index=False)


