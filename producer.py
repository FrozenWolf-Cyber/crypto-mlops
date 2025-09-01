import os
import time
import requests
import pandas as pd
from datetime import datetime
from quixstreams import Application
from tqdm import tqdm
from db import crypto_db
from consumer_utils import state_write, state_checker, delete_state
BASE_URL = "https://api.binance.com/api/v3/klines"
# Configuration
KAFKA_BROKER = "localhost:9092"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
INTERVAL = "1m"

# Create Quix application
app = Application(broker_address=KAFKA_BROKER,
                      producer_extra_config={
        # "buffer.memory": 33554432,         # e.g., 32 MB total buffer
        "message.max.bytes": 20000000,  # 20 MB per message
        # "compression.type": "snappy",      # optional: compression to reduce payload
    })

# Define topics (one per symbol) with JSON serializer
topics = {symbol: app.topic(name=symbol, value_serializer="json") for symbol in SYMBOLS}



def get_klines(symbol, interval, start_time=None, end_time=None, limit=1000):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    return r.json()


def download_full_history(symbol, interval="1m", start_str="2017-08-01", skip_start=False):
    """
    Download full OHLCV history with resume support.
    """
    start_ts = int(pd.Timestamp(start_str).timestamp() * 1000)
    now_ts = int(datetime.now().timestamp() * 1000)

    all_candles = []

    while True:
        try:
            candles = get_klines(symbol, interval, start_time=start_ts)
        except Exception as e:
            print(f"⚠️ Error: {e}, retrying in 5s...")
            time.sleep(5)
            continue

        if not candles:
            break

        all_candles.extend(candles)


        last_close_time = candles[-1][6]
        start_ts = last_close_time + 1

        time.sleep(0.25)  # Binance rate limit

        if last_close_time >= now_ts:
            break


    df_partial = pd.DataFrame(all_candles, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "trades", "taker_base",
                "taker_quote", "ignore"
            ])
    
    ## drop close_time column
    df_partial = df_partial.drop(columns=["close_time"])
    
    df_partial["open_time"] = pd.to_datetime(df_partial["open_time"], unit="ms")
    if skip_start:
        df_partial = df_partial[df_partial["open_time"] > pd.to_datetime(start_str)]
    # df_partial["open_time"] = df_partial["open_time"].astype("int64") // 10**6
    # df_partial["close_time"] = df_partial["close_time"].astype("int64") // 10**6
    df_partial["open_time"] = pd.to_datetime(df_partial["open_time"], format='%Y-%m-%d %H:%M:%S')
    
    for col in ["open", "high", "low", "close", "volume", "taker_base", "taker_quote", "quote_asset_volume", "ignore"]:
        df_partial[col] = pd.to_numeric(df_partial[col])

    return df_partial

def send_df_to_quix(symbol, df, producer, batch_size=10000):
    # Convert datetime to ms for JSON safety
    df = df.copy()

    # Iterate in chunks
    for i in tqdm(range(0, len(df), batch_size)):
        chunk = df.iloc[i:i+batch_size]
        records = chunk.to_dict(orient="records")

        kafka_msg = topics[symbol].serialize(
            key=f"{symbol}_batch_{records[0]['open_time']}",
            value=records,
        )
        producer.produce(
            topic=topics[symbol].name,
            key=kafka_msg.key,
            value=kafka_msg.value,
        )

def get_data(symbol):
    """Read last open_time from CSV for symbol"""
    CSV_PATH = f"data/prices/{symbol}.csv"
    past_data = pd.read_csv(CSV_PATH)
    past_data["open_time"] = pd.to_datetime(past_data["open_time"])
    return past_data
    


def fetch_binance_data(symbol, start_time=None):
    """Fetch Binance Klines"""
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": 1000,
    }
    if start_time:
        params["startTime"] = start_time
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def main():
    state_write("ALL", "producer", "main", "running")
    # Initialize last_open_time per symbol
    csv_path = {symbol: f"data/prices/{symbol}.csv" for symbol in SYMBOLS}
    csv_data = {symbol: get_data(symbol) for symbol in SYMBOLS}
    csv_last_times = {symbol: csv_data[symbol]["open_time"].iloc[-1] for symbol in SYMBOLS}
    
    psql_last_times = {symbol: pd.to_datetime(crypto_db.get_last_date(symbol)) for symbol in SYMBOLS}
    print("PSQL last times:", psql_last_times)
    print("CSV Last times:", csv_last_times)
    ### chose the min to prevent lag and avoid duplicates at consumer end
    last_times = {symbol: min(csv_last_times[symbol], psql_last_times[symbol]) for symbol in SYMBOLS}
    print("Starting from times:", last_times)
    
    with app.get_producer() as producer:
        while True:
            state = state_checker("ALL", "producer", "main")
            if state == "delete":
                print("Deleteing producer as per state file...")
                state_write("ALL", "producer", "main", "deleted")
                exit(0)
            elif state == "pause":
                print("Producer paused as per state file, sleeping...")
                state_write("ALL", "producer", "main", "paused")
                time.sleep(2)
                continue
            elif state == "start":
                print("Producer started as per state file...")
                state_write("ALL", "producer", "main", "running")
            elif state == "paused":
                time.sleep(2)
                continue
            
            loop_start = time.time()
            for symbol in SYMBOLS:
                start_time = last_times[symbol]
                print(f"Fetching {symbol} data from {start_time}...")

                records = download_full_history(symbol, start_str=start_time, skip_start=True)
                if len(records) == 0:
                    print(f"No new data for {symbol}")
                    time.sleep(0.25)
                    continue
                
                last_time_recieved = pd.to_datetime(records.iloc[-1]["open_time"])
                ### insert into psql
                print(f"Fetched {len(records)} new records for {symbol}, starting from {records.iloc[0]['open_time']} to {last_time_recieved}")
                
                if psql_last_times[symbol] < last_time_recieved:
                    print(f"Inserting {len(records[records['open_time'] > psql_last_times[symbol]])} new records into PSQL for {symbol}...")
                    crypto_db.insert_df_rows(symbol, records[records["open_time"] > psql_last_times[symbol]])
                    
                records["open_time"] = records["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
                
                send_df_to_quix(symbol, records, producer)
                
                records["open_time"] = pd.to_datetime(records["open_time"], format='%Y-%m-%d %H:%M:%S')
                
                print(f"Updated last time for {symbol} to {last_times[symbol]}")
                
                #### insert into csv
                records = records[records["open_time"] > csv_last_times[symbol]]
                print(f"Appending {len(records)} new records to CSV for {symbol}...")
                csv_data[symbol] = pd.concat([csv_data[symbol], records], ignore_index=True)
                # Save to CSV
                csv_data[symbol].to_csv(csv_path[symbol], index=False)
                
                last_times[symbol] = last_time_recieved
                csv_last_times[symbol] = last_times[symbol]
                psql_last_times[symbol] = last_time_recieved
                time.sleep(0.25)
                


            # Align to ~1 minute interval
            print("Sleeping to align to 1 minute interval...")
            elapsed = time.time() - loop_start
            sleep_time = max(0, 62 - elapsed)
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
