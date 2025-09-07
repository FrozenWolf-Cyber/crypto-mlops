import argparse
import time
import subprocess
import os
from s3_manager import S3Manager
from db import crypto_db
import pandas as pd
import requests
manager = S3Manager()
# from control_consumer import send_control_command
from consumer_utils import state_checker, state_write, delete_state, STATE_DIR


def create_consumer(crypto: str, model: str, version: str):
    if os.path.exists(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")):
        print(f"[WARNING] State file for {crypto} {model} {version} already exists, removing it first.")
        os.remove(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json"))
    """Download datasets and launch consumer."""
    print(f"[CREATE] Preparing consumer for {crypto} {model} {version}")

    cmd = [
        "python", "consumer.py",
        "--crypto", crypto,
        "--model", model,
        "--version", version,
    ]
    print("[CREATE] Launching:", " ".join(cmd))
    subprocess.Popen(cmd)

def main():
    parser = argparse.ArgumentParser(description="Control consumer lifecycle.")
    parser.add_argument("--crypto", required=True, help="Crypto symbol, e.g., BTCUSDT")
    parser.add_argument("--model", required=True, help="Model name, e.g., lightgbm")
    args = parser.parse_args()

    existing_versions = manager.get_existing_versions(args.crypto, args.model)
    ## new version is last one
    version = len(existing_versions)
    print(f"[INFO] New version will be v{version}")
    
    crypto = args.crypto
    model = args.model

    if len(existing_versions) <= 3:
        print(f"[INFO] Not enough existing versions to perform reconciliation. Need at least 2, found {len(existing_versions)}")
        print(f"[INFO] Downloading available predictions for {crypto} {model}... Ideally this is v{version}")
        manager.download_available_predictions(crypto, model)
        
        print(f"[INFO] Reading predictions from CSV and pushing to DB...")
        df = pd.read_csv(f"data/predictions/{crypto}/{model}/v{version}.csv")
        # crypto_db.bulk_update_predictions(crypto.lower(), model, version, df)
        
        print(f"[INFO] Triggering FastAPI to refresh models...")
        resp = requests.post("http://fastapi-ml:8000/refresh")
        print(resp.status_code, resp.text)
        
        print(f"[INFO] {crypto} {model} v{version} DB pushed successfully")
        
        print(f"[INFO] Creating consumer for {crypto} {model} v{version}...")
        create_consumer(crypto, model, f"v{version}")
        while state_checker(crypto, model, f"v{version}") == "unknown":
            time.sleep(0.5)
        state_write(crypto, model, f"v{version}", "start")
        print(f"[INFO] State file for {crypto} {model} v{version} exists, consumer launched.")
        return
    
    manager.reassign_pred_s3(crypto, model)
    print(f"[START] Restarting {crypto} {model} to reconcile versions...")
    
    print(f"[STAGE 1] Deleting existing v2 and v3 consumers...")
    # Step 1: delete v2 and v3 consumer
    for v in ["v2", "v3"]:
        if state_checker(crypto, model, v) == "unknown":
            print(f"[SKIP] No existing consumer for {crypto} {model} {v}, skipping deletion.")
            continue
        state_write(crypto, model, v, "delete" )

    for v in ["v2", "v3"]:
        if state_checker(crypto, model, v) == "unknown":
            print(f"[SKIP] No existing consumer for {crypto} {model} {v}, skipping deletion.")
        while state_checker(crypto, model, v) != "stopped":
            print(f"[WAIT] Waiting for {crypto} {model} {v} to be deleted...")
            time.sleep(5)
       
    ### trigger fastapi to load new models

    print(f"[INFO] Triggering FastAPI to refresh models...")
    resp = requests.post("http://fastapi-ml:8000/refresh")
    print(resp.status_code, resp.text)


    print(f"[STAGE 2] Shifting v3 to v2 ")

    ### set v3 column to null and rename v3 column to v2 in psql
    crypto_db.shift_predictions(crypto.lower(), model, from_version=3, to_version=2)
    
    ### cp v3 csv to v2 csv in s3
    os.remove(f"data/predictions/{crypto}/{model}/v2.csv")
    os.rename(f"data/predictions/{crypto}/{model}/v3.csv", f"data/predictions/{crypto}/{model}/v2.csv")

    print(f" Creating new v2 consumers")
    ### start v2 consumer
    create_consumer(crypto, model, "v2")
    while state_checker(crypto, model, "v2") == "unknown":
        time.sleep(0.5)
    state_write( crypto, model, f"v2", "start")
    
    print(f"[STAGE 3] Download new v3 predictions")
    ### download available predictions again (will download new v3)
    manager.download_available_predictions(crypto, model)
    

    print(f"[STAGE 4] Pushing new v3 predictions to DB and creating consumer")
    ### push new v3 pred to v3 column in psql
    df = pd.read_csv(f"data/predictions/{crypto}/{model}/v3.csv")
    df['open_time'] = pd.to_datetime(df['open_time'])

    crypto_db.bulk_update_predictions(crypto.lower(), model, 3, df)
    

    print(f"[STAGE 5] Creating new v3 consumer")
    # Step 3: Create again
    create_consumer(crypto, model, "v3")

    while state_checker(crypto, model, "v3") == "unknown":
        time.sleep(0.5)
        
    state_write( crypto, model, "v3", "start")
    print(f"[INFO] State file for {crypto} {model} {v} exists, consumer launched.")
        
    print(f"[DONE] {crypto} {model} v3 restarted successfully")

if __name__ == "__main__":
    main()
