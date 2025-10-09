from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List, Optional
import os
import pandas as pd
import numpy as np
from s3_manager import S3Manager
import time
import threading
## lock during data sync and update from db
sync_lock = threading.Lock()
price_lock = threading.Lock()
trl_lock = threading.Lock()

s3_manager = S3Manager(
   )

coins = ["BTCUSDT"]
models = ['tst', 'lightgbm']
state = {
    "prices": {},
    "trl": None,
    "first_nan_date": None,
    "trl_open_time": None,
    "price_last_sync": None,
    "trl_last_sync": None,
    "overall_last_sync": None,
}

# ------------------------------
# DATABASE CONNECTIONS (1 each)
# ------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")        # crypto prices/preds
AIRFLOW_DB = os.getenv("AIRFLOW_DB")            # batch_status / airflow logs
STATUS_DB = os.getenv("STATUS_DB")              # crypto batch events/status
TRL_DATABASE_URL = os.getenv("TRL_DATABASE_URL")                # TRL table (could be same as DATABASE_URL)
# Single connection per DB
db_engines: dict[str, Engine] = {
    "prices": create_engine(DATABASE_URL, pool_size=1, max_overflow=0),
    "airflow": create_engine(AIRFLOW_DB, pool_size=1, max_overflow=0),
    "status": create_engine(STATUS_DB, pool_size=1, max_overflow=0),
    "trl": create_engine(TRL_DATABASE_URL, pool_size=1, max_overflow=0),
}

import numpy as np
import re
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import Depends

def convert_pred_column_to_array(df, pred_col):
    """
    Converts the column `pred_col` in df to numpy arrays.
    Handles:
      - strings like "[0.83667 -0.68170 -0.0978]" (space separated)
      - strings like "[0.83667, -0.68170, -0.0978]" (comma separated)
      - already lists or arrays
      - messy spaces or mixed commas
    """
    def to_array(x):
        if isinstance(x, str):
            # Remove brackets
            s = x.strip("[]")
            # Split on comma or whitespace, filter out empty strings
            parts = [p for p in re.split(r'[,\s]+', s) if p]
            # Convert to float
            return np.array([float(p) for p in parts], dtype=float)
        elif isinstance(x, (list, tuple, np.ndarray)):
            return np.array(x, dtype=float)
        else:
            return np.array([x], dtype=float)
    
    df[pred_col] = df[pred_col].apply(to_array)
    return df

def fetch_all(engine: Engine, query: str, params: dict = None):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            columns = result.keys()
            rows = result.fetchall()
        x = [dict(zip(columns, row)) for row in rows]
        print(f"Fetched {len(x)} records")
        return x
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_open_time_range(coin: str):
    query = f"""
        SELECT MIN(open_time) AS min_time, MAX(open_time) AS max_time 
        FROM "{coin.lower()}"
    """
    result = fetch_all(db_engines["prices"], query)
    if result and result[0]['min_time'] and result[0]['max_time']:
        return pd.to_datetime(result[0]['min_time'], utc=True), pd.to_datetime(result[0]['max_time'], utc=True)
    else:
        return None, None
    
def get_open_time_range_trl():
    query = f"""
        SELECT MIN(date) AS min_time, MAX(date) AS max_time 
        FROM trl
    """
    result = fetch_all(db_engines["trl"], query)
    if result and result[0]['min_time'] and result[0]['max_time']:
        return pd.to_datetime(result[0]['min_time'], utc=True), pd.to_datetime(result[0]['max_time'], utc=True)
    else:
        return None, None
    
def get_prices_after(coin: str, start: pd.Timestamp):
    query = f"""
        SELECT * FROM "{coin.lower()}"
        WHERE open_time >= :start
        ORDER BY open_time
    """
    df = pd.DataFrame(fetch_all(db_engines["prices"], query, {"start": start}))
    if not df.empty:
        df['open_time'] = pd.to_datetime(df['open_time'], utc=True, format="mixed")
    return df


def get_trl_after(start: pd.Timestamp):
    query = f"""
        SELECT * FROM trl
        WHERE date >= :start
        ORDER BY date
    """
    df = pd.DataFrame(fetch_all(db_engines["trl"], query, {"start": start}))
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'], utc=True, format="mixed")
    return df

def update_price_from_db(start: pd.Timestamp, coin: str, prices: pd.DataFrame):
    lower_bound, _ = get_open_time_range(coin)
    if lower_bound:
        ##slice older than lower_bound
        print(f"Removing {len(prices[prices['open_time'] < lower_bound])} records from {coin} older than lower_bound {lower_bound}")
        prices = prices[prices['open_time'] >= lower_bound]
        
    new_prices = get_prices_after(coin, start)
    if not new_prices.empty:
        print(f"Fetched {len(new_prices)} new records from DB for {coin} with existing count {len(prices)}")
        # Merge predictions into new_prices
        print(new_prices.columns)
        ## instead of concat upsert them
        prices = prices[~prices['open_time'].isin(new_prices['open_time'])]
        print(f"After removing overlapping, {coin} has {len(prices)} records")
        prices = pd.concat([prices, new_prices], ignore_index=True)
        print(f"After adding new, {coin} has {len(prices)} records")
        prices = prices.sort_values(by='open_time').reset_index(drop=True)
        ## get new start, which is the min of nans among all prediction columns
        ## only prediction columns start with tst_ or lightgbm_
        ## only check newly added rows
        first_nan_date = None   
        for col in prices.columns:
            if col.startswith('tst_') or col.startswith('lightgbm_'):
                nan_df = prices[prices[col].isna()]
                if not nan_df.empty:
                    if first_nan_date is None:
                        first_nan_date = nan_df['open_time'].min()
                    else:
                        first_nan_date = min(first_nan_date, nan_df['open_time'].min())
        print("New first overall nan date", first_nan_date)
    else:
        print("No new records found in DB")
        first_nan_date = None
    
    print("Overall records price", len(prices))
    return prices, first_nan_date if first_nan_date else start

def update_trl_from_db(start: pd.Timestamp, trl: pd.DataFrame):
    lower_bound, _ = get_open_time_range_trl()
    if lower_bound:
        ##slice older than lower_bound
        print(f"Removing {len(trl[trl['date'] < lower_bound])} records from TRL older than lower_bound {lower_bound}")
        trl = trl[trl['date'] >= lower_bound]
    new_trl = get_trl_after(start)
    new_start = start
    if not new_trl.empty:
        print(f"Fetched {len(new_trl)} new TRL records from DB")
        trl = trl[~trl['title'].isin(new_trl['title'])]
        trl = pd.concat([trl, new_trl], ignore_index=True)
        trl = trl.sort_values(by='date').reset_index(drop=True)
        new_start = new_trl['date'].min()
        print(f"New TRL start date: {new_start}")
    else:
        print("No new TRL records found in DB")
        
    return trl, new_start

def update_price_periodic(sec=60):
    if time.time() - state["price_last_sync"] < sec:
        return
    if not sync_lock.locked():
        with price_lock:
            print("Periodic price update from DB...")
            first_nan_date = state["first_nan_date"]
            for coin in coins:
                print(f"Updating prices for {coin} from {first_nan_date}")
                state["prices"][coin], first_nan_date = update_price_from_db(first_nan_date, coin, state["prices"][coin])
            state["first_nan_date"] = first_nan_date
            state["price_last_sync"] = time.time()

def update_trl_periodic(sec=300):
    if time.time() - state["trl_last_sync"] < sec:
        return
    if not sync_lock.locked():
        with trl_lock:
            print("Periodic TRL update from DB...")
            trl_open_time = state["trl_open_time"]
            state["trl"], trl_open_time = update_trl_from_db(trl_open_time, state["trl"])
            state["trl_open_time"] = trl_open_time
            state["trl_last_sync"] = time.time()

            
def sync_data_periodic(): ## sync every 3 days
    ## check if any new task_name post_train_* status SUCCESS in crypto_batch_status table. If yes, do full sync
    with sync_lock:
        last_success, task_name = get_last_successful_post_train()
        print(f"Last successful post_train task at {last_success}, last overall sync at {state['overall_last_sync'], task_name}")
        if state["overall_last_sync"] != last_success:
            print(f"[TASK SUCCESS:{task_name}] Detected new successful post_train task since last sync {state['overall_last_sync']} -> {last_success}")
            print("Periodic full data sync...")
            sync_data()
            state["overall_last_sync"] = last_success ## update to latest just before sync. a new task might come during sync which needs another sync later
            print(f"Updated overall last sync to {state['overall_last_sync']}")
            return last_success
        print("No new successful post_train tasks since last sync")
    return last_success
        
def get_last_successful_post_train():
    query = """
        SELECT task_name, MAX(updated_at) AS last_success
        FROM crypto_batch_status
        WHERE task_name LIKE 'post_train_%' AND status = 'SUCCESS'
        GROUP BY task_name
        ORDER BY last_success DESC
        LIMIT 1
    """
    result = fetch_all(db_engines["status"], query)
    if result and result[0]['last_success']:
        return pd.to_datetime(result[0]['last_success'], utc=True), result[0]['task_name']
    else:
        return None, None

def sync_data():
    with sync_lock:
        if not os.path.exists("data"):
            os.makedirs("data")
        if not os.path.exists("data/prices"):
            os.makedirs("data/prices")

        article_path = f"data/articles/articles.csv" 
        s3_manager.download_df(article_path, bucket='mlops', key=f'articles/articles.parquet')

        for coin in coins:
            prices_path = f"data/prices/{coin}.csv"
            s3_manager.download_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')

            for model in models:
                s3_manager.download_available_predictions(coin, model)

        s3_manager.download_available_predictions("preds", "trl")

        prices = {}
        trl = None
        version_trl = os.listdir(f"data/predictions/preds/trl")
        ### only load .csv
        version_trl = [v for v in version_trl if v.endswith('.csv')]
        print(f"Available versions for trl: {version_trl}")
        for version in version_trl:
            print(f"Processing TRL version: {version}")
            if trl is None:
                trl = pd.read_csv(f"data/predictions/preds/trl/{version}") #title, pred
                articles = pd.read_csv(f"data/articles/articles.csv") #title, date
                articles['date'] = pd.to_datetime(articles['date'], utc=True, format="mixed")
                ## select only available pred dates
                article = articles[articles['title'].isin(trl['title'])]
                ## rows might be out of order, match title and assign date to trl

                trl = pd.merge(
                    trl,
                    article[['title', 'date']],
                    on='title',
                    how='left'
                )
                # Drop duplicate column if exists
                if 'date_y' in trl.columns:
                    trl = trl.drop(columns=['date_x']).rename(columns={'date_y': 'date'})


                print(f"Loaded predictions for trl {len(trl)} records")
                print("Number of NaNs in pred:", trl['pred'].isna().sum())
                print(f"Start: {trl['date'].min()}, End: {trl['date'].max()}")
                trl.rename(columns={"pred": f"trl_{int(version.split('.')[0][1:])}"}, inplace=True)
            else:
                ## merge the column pred_{version} into trl
                # print(trl.columns)
                temp = pd.read_csv(f"data/predictions/preds/trl/{version}")
                version = int(version.split(".")[0][1:])
                trl = trl.merge(
                    temp[['title', 'pred']].rename(columns={'pred': f'trl_{version}'}),
                    on='title',
                    how='left'
                )

                print(f"Nans in trl_{version}:", trl[f'trl_{version}'].isna().sum())

        # print(trl.columns)
        trl_open_time, trl_last_time = get_open_time_range_trl()
        print(f"TRL DB open_time: {trl_open_time}, db_last_time: {trl_last_time}")
        trl["date"] = pd.to_datetime(trl["date"], utc=True, format="mixed")
        ## choose only title, link, date, trl_{version} columns, price, label
        trl = trl[['title', 'link', 'date'] + [col for col in trl.columns if col.startswith('trl_')]]
        
        print(f"Loaded TRL, {len(trl)} records")
        print(f"Start: {trl['date'].min()}, End: {trl['date'].max()}")

        ### download new data from DB if needed
        trl, trl_open_time = update_trl_from_db(trl_open_time, trl)

        for coin in coins:
            prices[coin] = pd.read_csv(f"data/prices/{coin}.csv")
            prices[coin]["open_time"] = pd.to_datetime(prices[coin]["open_time"], utc=True, format="mixed")
            print(f"Loaded prices for {coin}, {len(prices[coin])} records")
            print(f"Start: {prices[coin]['open_time'].min()}, End: {prices[coin]['open_time'].max()}")
            first_nan_date = None
            for model in models:
                available_versions = os.listdir(f"data/predictions/{coin}/{model}")
                available_versions = [v for v in available_versions if v.endswith('.csv')]
                print(f"Available versions for {coin} {model}: {available_versions}")
                temp = {}

                for version in available_versions:
                    temp = pd.read_csv(f"data/predictions/{coin}/{model}/{version}") # open_time, pred
                    temp["open_time"] = pd.to_datetime(temp["open_time"], utc=True, format="mixed")
                    temp = convert_pred_column_to_array(temp, 'pred')
                    print(f"Loaded predictions for {coin} {model} {version}, {len(temp)} records")
                    print(f"Start: {temp['open_time'].min()}, End: {temp['open_time'].max()}")

                    ## add new column for this predictions in {model}_{version} as pred
                    version = int(version.split(".")[0][1:])
                    prices[coin][f"{model}_{version}"] = np.empty(len(prices[coin]), dtype=object)
                    prices[coin].loc[prices[coin]['open_time'].isin(temp['open_time']), f"{model}_{version}"] = temp['pred'].values

                    print(f"After merging, {coin} has {len(prices[coin])} records")
                    print(f"After merging nans, {coin} has {prices[coin][f'{model}_{version}'].isna().sum()} NaNs in {model}_{version}")
                    nan_df = prices[coin][prices[coin][f"{model}_{version}"].isna()]
                    print("First nan date", nan_df['open_time'].min(), "First overall nan date", first_nan_date)
                    if nan_df.empty:
                        continue
                    if first_nan_date is None:
                        first_nan_date = nan_df['open_time'].min()
                    else:
                        first_nan_date = min(first_nan_date, nan_df['open_time'].min())
            if first_nan_date is None:
                first_nan_date = prices[coin]['open_time'].max()
            #### fetch new data from DB if needed
            db_open_time, db_last_time = get_open_time_range(coin)
            print(f"DB open_time: {db_open_time}, db_last_time: {db_last_time} first_nan_date: {first_nan_date}")
            print(prices[coin])

    
            if db_last_time and db_last_time > first_nan_date:
                print(f"Fetching new price data from DB after {first_nan_date}")
                prices[coin], first_nan_date = update_price_from_db(first_nan_date, coin, prices[coin])

            print(f"Final {coin} data, {len(prices[coin])} records")
            print(f"Start: {prices[coin]['open_time'].min()}, End: {prices[coin]['open_time'].max()}")
            print(prices[coin])


        # return prices, first_nan_date, trl, trl_open_time
        state["prices"] = prices
        state["trl"] = trl
        state["first_nan_date"] = first_nan_date
        state["trl_open_time"] = trl_open_time
        state["price_last_sync"] = time.time()
        state["trl_last_sync"] = time.time()
        # state["overall_last_sync"]
    


sync_data()
state["overall_last_sync"], task_name = get_last_successful_post_train()
print(f"Initial overall last sync at {state['overall_last_sync']} from task {task_name}")


# ------------------------------
# FASTAPI APP
# ------------------------------
app = FastAPI(title="Crypto Project API")
# Allow localhost:5173 to access the backend
origins = [
    "http://localhost:3000",  # React dev server
    "http://localhost:5173",  # Vite dev server
    "http://127.0.0.1:5173",  # sometimes needed
    "https://FrozenWolf-Cyber.github.io/crypto",
    "https://crypto-bxs.pages.dev",
    "https://crypto.gokuladethya.uk"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # or ["*"] to allow all origins (for dev)
    allow_credentials=True,
    allow_methods=["*"],          # allow all HTTP methods
    allow_headers=["*"],          # allow all headers
)
security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = os.getenv("SYNC_USERNAME", "admin")
    correct_password = os.getenv("SYNC_PASSWORD", "password")
    if credentials.username != correct_username or credentials.password != correct_password:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials

@app.get("/sync")
def trigger_sync(credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    print("Manual sync triggered by", credentials.username, flush=True)
    r = sync_data_periodic()
    return {"status": r}

@app.get("/force_sync")
def trigger_force_sync(credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    print("Manual force sync triggered by", credentials.username, flush=True)
    with sync_lock:
        last_success, task_name = get_last_successful_post_train()
        print(f"Last successful post_train task at {last_success}, last overall sync at {state['overall_last_sync'], task_name}")
        print(f"[TASK SUCCESS:{task_name}] Detected new successful post_train task since last sync {state['overall_last_sync']} -> {last_success}")
        print("Periodic full data sync...")
        sync_data()
        state["overall_last_sync"] = last_success ## update to latest just before sync. a new task might come during sync which needs another sync later
        print(f"Updated overall last sync to {state['overall_last_sync']}")
        return last_success

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/prices/{coin}")
def get_prices(
    coin: str,
    start: str = Query(...),
    end: str = Query(...),
    interval: str = Query(None, description="Aggregation interval like 1h, 1d. Leave empty to skip aggregation."),
    step: int = Query(1, description="Sample every nth row. Default is 1 (all rows).")
):
    last_success = sync_data_periodic()
    update_price_periodic()

    df = state["prices"].get(coin.upper()).copy()
    if end is None:
        end = df['open_time'].max()
    if start is None:
        start = df['open_time'].min()
    start, end = pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True)
   
    print(df['open_time'].dtype)
    print(type(start), type(end))
    
    print(f"Start date: {start}, End date: {end}, Interval: {interval}, Step: {step}")
    print(f"DF: Start: {df['open_time'].min()}, End: {df['open_time'].max()}")
    print("Ttotal records after start alone: ", len(df[df['open_time'] >= start]))
    print("Total records before end alone: ", len(df[df['open_time'] <= end]))

    df = df[(df['open_time'] >= start) & (df['open_time'] <= end)]
    print(f"Initial filtered data: {len(df)} records")
    # If aggregation interval is provided
    if interval:
        df = df.set_index('open_time')
        df_agg = df.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'tst_1': 'last',
            'tst_2': 'last',
            'tst_3': 'last',
            'lightgbm_1': 'last',
            'lightgbm_2': 'last',
            'lightgbm_3': 'last',
        }).reset_index()
        df = df_agg

    ## convert to serializable formats:
    for col in df.columns:
        if col.startswith('tst_') or col.startswith('lightgbm_'):
            df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    
    ## convert datetime to iso format
    df['open_time'] = df['open_time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"After aggregation: {len(df)} records")
    # Apply sampling (every nth row)
    if step > 1:
        df = df.iloc[::step]

    return df.to_dict(orient="records")

# 2️⃣ TRL table (static)
@app.get("/trl")
def get_trl(start: str = Query(...), end: str = Query(...)):
    sync_data_periodic()
    update_trl_periodic()
    start, end = pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True)
    df = state["trl"].copy()
    if start is None:
        start = df['date'].min()
    if end is None:
        end = df['date'].max()

    print(f"Start date: {start}, End date: {end}")
    print(f"DF: Start: {df['date'].min()}, End: {df['date'].max()}")
    df = df[(df['date'] >= pd.to_datetime(start, utc=True)) & (df['date'] <= pd.to_datetime(end, utc=True))]
    print(f"TRL filtered data: {len(df)} records")
    df = df[['title', 'link', 'date'] + [col for col in df.columns if col.startswith('trl_')]]
    print(df.columns)    
    mask_invalid = df.isin([np.inf, -np.inf]) | df.isna()
    bad_rows = df[mask_invalid.any(axis=1)]

    if not bad_rows.empty:
        print("⚠️ Found invalid numeric values (NaN, inf, -inf) in TRL data:")
        print(bad_rows)
    else:
        print("✅ No invalid float values found.")
   
    return df.to_dict(orient="records")

@app.get("/last_success")
def get_last_success():
    last_success, task_name = get_last_successful_post_train()
    return {"last_success": last_success, "task_name": task_name, "overall_last_sync": state["overall_last_sync"]}

# 3️⃣ Airflow batch_status table
@app.get("/airflow/batch_status")
def get_airflow_status(coin: Optional[str] = None):
    query = "SELECT * FROM batch_status WHERE 1=1"
    params = {}
    if coin:
        query += " AND coin = :coin"
        params["coin"] = coin
    query += " ORDER BY updated_at DESC"
    return fetch_all(db_engines["airflow"], query, params)

# 4️⃣ Status DB: batch events
@app.get("/status/events")
def get_batch_events(dag_name: Optional[str] = None,
                     task_name: Optional[str] = None,
                      limit: Optional[int] = Query(None, description="Max number of recent records to return")):
    query = "SELECT * FROM crypto_batch_events WHERE 1=1"
    params = {}
    if dag_name:
        query += " AND dag_name = :dag_name"
        params["dag_name"] = dag_name
    
    if task_name:
        query += " AND task_name = :task_name"
        params["task_name"] = task_name
        
    query += " ORDER BY created_at DESC"
    
    if limit:
        query += " LIMIT :limit"
        params["limit"] = limit
    return fetch_all(db_engines["status"], query, params)

@app.get("/status/batch_status")
def get_crypto_batch_status(
    task_name: Optional[str] = None,
    dag_name: Optional[str] = None,
    limit: Optional[int] = Query(None, description="Max number of recent records to return")
):
    query = "SELECT * FROM crypto_batch_status WHERE 1=1"
    params = {}

    if dag_name:
        query += " AND dag_name = :dag_name"
        params["dag_name"] = dag_name
  
    if task_name:
        query += " AND task_name = :task_name"
        params["task_name"] = task_name

    query += " ORDER BY updated_at DESC"

    if limit:
        query += " LIMIT :limit"
        params["limit"] = limit

    return fetch_all(db_engines["status"], query, params)


