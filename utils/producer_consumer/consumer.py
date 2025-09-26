import argparse
from collections import defaultdict
from quixstreams import Application
import time
import pandas as pd
from ..database.db import crypto_db
from ..trainer.train_utils import preprocess_common, preprocess_common_batch
import requests
import gc
import os
import numpy as np
from .consumer_utils import state_checker, state_write
from tqdm import tqdm
from .logger import get_logger

KAFKA_BROKER = f"{os.environ['KAFKA_HOST']}:9092"
CONTROL_TOPIC = "control_topic"
url = "http://fastapi-ml:8000/predict"

# Keep pause/resume state
pause_flags = defaultdict(lambda: True)
delete_flags = defaultdict(lambda: False)
seq_len = 30


df_partial = None 
df_pred = None
last_time = None

def get_predictions(inp, crypto, model, version):

    for idx in range(len(inp)):
        if isinstance(inp[idx], np.ndarray):
            inp[idx] = inp[idx].tolist()

    ### batcify predictions, max 5000 per request
    all_pred = []
    for i in tqdm(range(0, len(inp), 5000)):
        batch_inp = inp[i:i+5000]
        params = {"model_name": f"{crypto.lower()}_{model.lower()}", "version": int(version[1:])-1}
        pred = requests.post(url, params=params, json=batch_inp)
        pred = pred.json()['predictions']
        if i == 0:
            all_pred = pred
        else:
            all_pred.extend(pred)
    return all_pred

def build_pipeline(app, crypto, model, version):
    logger = get_logger(f"{crypto}_{model}_{version}")
    global df_partial, df_pred, last_time
    key = (crypto, model, version)

    # --- Data topic ---
    data_topic = app.topic(name=crypto, value_deserializer="json")
    sdf = app.dataframe(topic=data_topic)

    #### We are doing this in case producer/consumer was interrupted and during rerun we start in a fresh reconciled state
    ### do this if the model was already available.
    ### fixed problem when producer already has written into .csv and db but consumer failed to listen
    ### find the all the dates where no predictions are present in the db
    pred_path = f"/opt/airflow/custom_persistent_shared/data/predictions/{crypto}/{model}/{version}.csv"
    logger.info(f"[{key}] Assumed prediction path: {pred_path}")
    params = {"model_name": f"{crypto.lower()}_{model.lower()}", "version": int(version[1:])-1}
    is_available = requests.post(f"http://fastapi-ml:8000/is_model_available", params=params).json()['available']
    logger.info(f"Model availability for {params}: {is_available}")
    if not is_available:
        logger.info(f"[{key}] Model not available yet, skipping historical inference.")
        df_pred = pd.DataFrame(columns=["open_time", "prediction"])
    else:

        ### check missing data in csv older than oldest_missing
        df_pred = pd.read_csv(pred_path)
        logger.info(f"[{key}] Loaded existing predictions from CSV, {len(df_pred)} rows.")
        df = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{crypto}.csv")
        df['open_time'] = pd.to_datetime(df['open_time'], format='%Y-%m-%d %H:%M:%S')
        df = df.sort_values("open_time").reset_index(drop=True)
        df_pred["open_time"] = pd.to_datetime(df_pred["open_time"])

        logger.info(f"[{key}] Model available, proceeding with historical inference.")
        # get missing prediction dates from db
        # missing_pred_dates = pd.to_datetime(
        #     crypto_db.get_missing_prediction_times(crypto.lower(), model.lower(), int(version[1:]))
        # )
        ## get start and end date of missing dates from db instead of all missing dates to reduce memory usage
        start_date, end_date = crypto_db.get_missing_prediction_date_range(crypto.lower(), model.lower(), int(version[1:]))
        missing_pred_dates = df["open_time"][(df["open_time"] >= start_date) & (df["open_time"] <= end_date)]
        logger.info(f"[{key}] Missing prediction dates from DB: {missing_pred_dates[:5].tolist() if len(missing_pred_dates)>0 else 'N/A'}")
        missing_pred_dates_db = missing_pred_dates.copy() ## select them for upsertion
        logger.info(f"[{key}] Found {len(missing_pred_dates)} missing prediction dates in DB.")
        oldest_missing = missing_pred_dates.min() if len(missing_pred_dates)>0 else pd.to_datetime( pd.to_datetime(crypto_db.get_last_date(crypto.lower())))
        

        csv_missing_dates = df[df["open_time"] < oldest_missing ]["open_time"]
        csv_missing_dates = csv_missing_dates[csv_missing_dates>df_pred["open_time"].max()] if not df_pred.empty else missing_pred_dates_db
        logger.info(f"[{key}] Found {len(csv_missing_dates)} missing prediction dates in CSV older than oldest missing in DB.")
        missing_pred_dates = pd.to_datetime(
            pd.concat([pd.Series(missing_pred_dates), csv_missing_dates]).drop_duplicates()
        )
        
        missing_pred_dates_db = pd.to_datetime(missing_pred_dates_db)
        
        logger.info(f"[{key}] Oldest missing prediction date in DB: {oldest_missing if oldest_missing else 'N/A'}")
        ## print sample min and max missing dates in df and db
        logger.info(f"[{key}] sample missing prediction dates in DB: {missing_pred_dates_db[:5].tolist() if len(missing_pred_dates_db)>0 else 'N/A'}")
        logger.info(f"[{key}] sample missing prediction dates in df: {df['open_time'][:5].tolist() if len(df)>0 else 'N/A'}")
        logger.info(f"[{key}] Total missing prediction dates in DB: {len(missing_pred_dates_db)}.")
        logger.info(f"[{key}] Total missing prediction dates after combining DB and CSV: {len(missing_pred_dates)}.")
        logger.info(f"[{key}] Date range of missing predictions: {missing_pred_dates.min() if len(missing_pred_dates)>0 else 'N/A'} to {missing_pred_dates.max() if len(missing_pred_dates)>0 else 'N/A'}.")
        logger.info(f"[{key}] Date range of available data: {df['open_time'].min()} to {df['open_time'].max()}.")
        logger.info(f"[{key}] Date range of existing predictions in CSV: {df_pred['open_time'].min() if not df_pred.empty else 'N/A'} to {df_pred['open_time'].max() if not df_pred.empty else 'N/A'}.")
        logger.info(f"[{key}] Total rows in price data: {len(df)}.")
         
        logger.info(f"[{key}] Found {len(missing_pred_dates)} missing prediction dates.")
        if len(missing_pred_dates) > 0:
            df = df[df['open_time']<=missing_pred_dates.max()]
            ### slice 30 rows before min
            target_date = missing_pred_dates.min()   # or any other reference date
            diff = (df['open_time'] - target_date).abs()
            ith_idx = diff.idxmin()

            if ith_idx is None or ith_idx < seq_len:
                df = df # no slicing possible
            else:
                df = df.iloc[max(0, ith_idx-seq_len+1):].reset_index(drop=True)
            logger.info(f"[{key}] Sliced DataFrame to {len(df)} rows for historical inference.")
            X_seq = preprocess_common_batch(model, df=df, seq_len=seq_len, return_first=True)
            logger.info(f"[{key}] Preprocessed {len(X_seq)} sequences for historical inference.")

            # map datetime → row index for fast lookup
            pos_map = {t: i for i, t in enumerate(df['open_time'])}

            inp = []
            rows_for_upsert = []
            db_missing_pred_dates_pred_idx = []
            logger.info(missing_pred_dates_db[:5])
            logger.info(missing_pred_dates[:5])
            missing_pred_dates_db_dates = list(missing_pred_dates_db.values)
            logger.info(missing_pred_dates_db_dates[:5])
            logger.info(missing_pred_dates_db_dates[0]==missing_pred_dates.values[0])
            logger.info(missing_pred_dates_db_dates[0], missing_pred_dates.values[0])
            for d in tqdm(missing_pred_dates):

                if d not in pos_map:
                    ## get the closest date before d
                    possible_dates = df['open_time'][df['open_time'] < d]
                    if possible_dates.empty:
                        ## use first index
                        inp.append(X_seq[0])
                    else:
                        closest_date = possible_dates.max()
                        idx = pos_map[closest_date]
                        inp.append(X_seq[idx])
                    
                else:
                    idx = pos_map[d]
                inp.append(X_seq[idx])

                # collect the ith row (the target prediction time)

                ## check if d in missing_pred_dates_db
                if d in missing_pred_dates_db_dates:
                    rows_for_upsert.append(df.iloc[idx])
                    db_missing_pred_dates_pred_idx.append(len(inp)-1)  # index in inp list

            # build df_temp_upsert once at the end
            df_temp_upsert = pd.DataFrame(rows_for_upsert) ### this will be used to upsert into db (redundancy for insert)
            logger.info(f"[{key}] Prepared {len(df_temp_upsert)} rows for DB upsertion.")
            logger.info(f"[{key}] Missing prediction dates: , prepared {len(inp)} sequences for inference.")
            pred = get_predictions(inp, crypto, model, version)
            pred_db = [pred[i] for i in db_missing_pred_dates_pred_idx]
            
            if len(pred)>0:
                logger.info(f"[{key}] Obtained {len(pred_db)} predictions for upserting into DB.")
                crypto_db.upsert_predictions(crypto.lower(), model.lower(), int(version[1:]), missing_pred_dates_db, pred_db, df_temp_upsert)
                logger.info(f"[{key}] Upserted missing predictions into DB.")

                ### upsert into csv
                df_upsert = pd.DataFrame()
                df_upsert["open_time"] = missing_pred_dates
                df_upsert["open_time"] = pd.to_datetime(df_upsert["open_time"])
                df_upsert["pred"] = pred_db
                df_upsert = df_upsert[~df_upsert["open_time"].isin(df_pred["open_time"])]
                logger.info(f"[{key}] Prepared {len(df_upsert)} new rows to upsert into CSV.")
                df_pred = pd.concat([df_pred, df_upsert], ignore_index=True)
                df_pred = df_pred.sort_values(by="open_time").reset_index(drop=True)
                df_pred = df_pred.drop_duplicates(subset=["open_time"])
                df_pred.to_csv(pred_path, index=False)
                logger.info(f"[{key}] Upserted missing predictions into CSV at {pred_path}.")

            del inp, pred, rows_for_upsert, df_upsert
            ### delete the dataframe to free memory
        del df
        gc.collect()
        logger.info(f"[{key}] Historical inference completed.")
    
    df_pred = pd.read_csv(pred_path)
    logger.info(f"[{key}] Loaded existing predictions from CSV, {len(df_pred)} rows.")
    logger.info(f"[{key}] DB predictions count: {crypto_db.get_total_pred_count(crypto.lower(), model.lower(), int(version[1:]))}")
            
    if args.start:
        logger.info(f"[{key}] Starting immediately as per --start flag.")
        state_write(crypto, model, version, "start")
    else:
        state_write(crypto, model, version, "wait")

    df_pred = pd.read_csv(pred_path)
    def maybe_process(message):
        global df_partial, df_pred, last_time
        state = state_checker(crypto, model, version)
        if state in ["wait", "pause", "delete", "start"]:
            while True:
                logger.info(f"[{key}] Consumer LOGS: Current state: {state}, pausing processing.")
                state = state_checker(crypto, model, version)
                if state == "delete":
                    logger.info(f"[{key}] Deletion requested, exiting consumer.")
                    state_write(crypto, model, version, "stopped")
                    app.stop()
                    return None
                elif state == "start":
                    logger.info(f"[{key}] Starting processing.")
                    state_write(crypto, model, version, "running")
                    break  
                time.sleep(1)
                    
            
                
            ### after resuming we need to start from last known state
            ### if the version was deleted or new v3 or new v2 we need to find where to start listening from
            df_pred = pd.read_csv(pred_path)
            logger.info(f"[{key}] Loaded existing predictions from CSV, {len(df_pred)} rows.")
            logger.info(f"[{key}] DB predictions count: {crypto_db.get_total_pred_count(crypto.lower(), model.lower(), int(version[1:]))}")
            last_csv_open_time = None
            if not df_pred.empty:
                df_pred["open_time"] = pd.to_datetime(df_pred["open_time"])
                last_csv_open_time = df_pred["open_time"].max()
                logger.info(f"[{key}] Last prediction time from CSV: {last_csv_open_time}")
            else:
                logger.info(f"[{key}] No existing predictions found.")
        
            logger.info(f"[{key}] Resuming processing.")
            ### get the minimum of last_csv_open_time and psql_last_time and start from there (ideally psql_last_time which should be equal to last_csv_open_time)
            psql_last_time = pd.to_datetime(crypto_db.get_first_missing_date(crypto.lower(), model.lower(), int(version[1:])))
            df_full = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{crypto}.csv")
            df_full["open_time"] = pd.to_datetime(df_full["open_time"])
            
            logger.info(f"[{key}] First missing prediction date from DB: {psql_last_time}")
            if psql_last_time:
                last_time = min(last_csv_open_time, psql_last_time) if last_csv_open_time else psql_last_time
            else:
                last_time = last_csv_open_time
                

            idx = df_full.index[df_full["open_time"] == last_time]
            if not idx.empty:
                pos = idx[0]  # row position of last_time
                start = max(0, pos - (seq_len - 1))  # include last_time itself
                df_partial = df_full.iloc[start:pos+1].copy()
            else:
                logger.info(f"[{key}] Last time {last_time} not found in full data, starting fresh.")
                df_partial = pd.DataFrame()  # nothing found
                
            del df_full, idx
            gc.collect()
            logger.info(f"[{key}] Reconstructed partial DataFrame with {len(df_partial)} rows for continuity, min time: {df_partial['open_time'].min() if not df_partial.empty else 'N/A'}, max time: {df_partial['open_time'].max() if not df_partial.empty else 'N/A'}.")
            logger.info(f"[{key}] Continuing from time: {last_time}")
            
        records = message  


        ### maintain a rolling window of seq_len
        
        df = pd.DataFrame(records)
        df["open_time"] = pd.to_datetime(df["open_time"], format='%Y-%m-%d %H:%M:%S')
        logger.info(f"[{key}] New data received, {len(df)} rows before filtering.")
        df = df[df["open_time"]>last_time] if last_time else df
        logger.info(f"[{key}] {len(df)} rows after filtering with last_time {last_time}.")
        if df.empty:
            return message
        l = len(df)
        if df_partial is None:
            df_partial = pd.DataFrame()
        df_partial = df_partial[-(seq_len-1):] if not df_partial.empty else df_partial
        df_partial = pd.concat([df_partial, df], ignore_index=True)
        df_partial = df_partial.drop_duplicates(subset=["open_time"])
        # df_partial = df_partial[:seq_len]
        logger.info(f"[{key}] Runninng length: {len(df_partial)}")
        df = df_partial.copy()
        
        
        for col in ["open", "high", "low", "close", "volume", "taker_base", "taker_quote", "quote_asset_volume", "ignore"]:
            df[col] = pd.to_numeric(df[col])
        
        #### filer df to only those greater than last_csv_open_time
        
        if not df.empty:
            logger.info(f"[{key}] New data received, {len(df)} rows after filtering. time range: {df['open_time'].min()} to {df['open_time'].max()}")
            X_seq = preprocess_common_batch(model, df=df, seq_len=seq_len, return_first=True)
            X_seq = X_seq[-l:]
            pred = get_predictions(X_seq, crypto, model, version)
            logger.info(f"[{key}] Obtained {len(pred)} new predictions.")
            logger.info(f"{len(pred)}, {len(df)}")
            crypto_db.upsert_predictions(crypto.lower(), model.lower(), int(version[1:]), df['open_time'][-l:], pred, df[-l:])
            logger.info(f"[{key}] Upserted new predictions into DB.")
            df = df[["open_time"]][-l:]
            df["pred"] = pred
            
            logger.info(f"[{key}] Before appending, CSV has {len(df_pred)} rows.")
            df_pred = pd.concat([df_pred, df], ignore_index=True)
            df_pred = df_pred.sort_values(by="open_time").reset_index(drop=True)
            df_pred = df_pred.drop_duplicates(subset=["open_time"])
            logger.info(f"[{key}] After appending, CSV has {len(df_pred)} rows.")
            df_pred.to_csv(pred_path, index=False)
            logger.info(f"[{key}] Appended new predictions to CSV at {pred_path}.")
        
            return message

    sdf = sdf.apply(maybe_process)

    # # --- Control topic ---
    # control_topic = app.topic(name=CONTROL_TOPIC, value_deserializer="json")
    # control_df = app.dataframe(topic=control_topic)

    # def handle_control(row):
    #     command = str(row.get("command", "")).strip().upper()
    #     parts = command.split()
    #     if len(parts) != 4:
    #         return
    #     action, c, m, v = parts
    #     action = action.upper()
    #     c = c.upper()
    #     m = m.lower()
    #     v = v.lower()
    #     logger.info(f"[{key}] Command parts: (c, m, v)=({c}, {m}, {v}), action={action}, MATCH={ (c, m, v) == key }")
        
    #     if (c, m, v) != key:
    #         return
    #     if action == "RESUME":
    #         pause_flags[key] = False
    #         logger.info(f"[{key}] Resumed.")
    #     elif action == "PAUSE":
    #         pause_flags[key] = True
    #         logger.info(f"[{key}] Paused.")
    #     elif action == "DELETE":
    #         delete_flags[key] = True
    #         logger.info(f"[{key}] Deletion flag set. Consumer will exit if paused or after current processing.")

    # control_df = control_df.update(handle_control)

    return sdf


parser = argparse.ArgumentParser()
parser.add_argument("--crypto", required=True, help="Crypto topic (e.g. BTCUSDT)")
parser.add_argument("--model", required=True, help="Model name (e.g. lightgbm)")
parser.add_argument("--version", required=True, help="Model version (e.g. v1)")
parser.add_argument("--start", action="store_true", help="Start processing immediately")
args = parser.parse_args()
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group=f"{args.model}-{args.version}-consumer",
    auto_offset_reset="earliest",
    state_dir="/opt/airflow/custom_persistent_shared/quix_state"
)
build_pipeline(app, args.crypto, args.model, args.version)
print("Consumer running, waiting for control commands...")
app.run()
