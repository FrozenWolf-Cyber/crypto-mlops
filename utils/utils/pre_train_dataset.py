import os
from ..artifact_control.s3_manager import S3Manager
manager = S3Manager()
import subprocess
from ..producer_consumer.consumer_utils import state_checker, if_state_exists, state_write, delete_state, STATE_DIR
from ..trainer.train_utils import download_s3_dataset


download_s3_dataset("BTCUSDT", trl_model=True)


manager = S3Manager()
RETENTION_MIN = 60*24*365  # 1 year
TEST_RETENTION_MIN = 60*24*30  # 90 days

if if_state_exists("ALL", "producer", "main"):
    print("Existing producer state file found with state:", state_checker("ALL", "producer", "main"))
    if state_checker("ALL", "producer", "main") == "running":
        print("Existing producer found, deleting it first...")
        state_write("ALL", "producer", "main", "delete")
        while state_checker("ALL", "producer", "main") != "deleted":
            print("Waiting for existing producer to delete...")
            time.sleep(1)

        print("Existing producer deleted.")
else:
    print("No existing producer state file found.")
    
data_path = "./data/prices"

cryptos = ["BTCUSDT"]

def create_producer(crypto: str, model: str, version: str):
    if os.path.exists(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")):
        print(f"[WARNING] State file for {crypto} {model} {version} already exists, removing it first.")
        os.remove(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json"))
    """Download datasets and launch consumer."""
    print(f"[CREATE] Preparing producer for {crypto} {model} {version}")

    cmd = ["bash", "-c", "PYTHONPATH=..:$PYTHONPATH python -m utils.producer_consumer.producer"]
    print("[CREATE] Launching:", " ".join(cmd))
    subprocess.Popen(
    cmd,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    stdin=subprocess.DEVNULL,
    close_fds=True,
    start_new_session=True
)

import pandas as pd

for coin in cryptos:
    print(f"Processing {coin}...")
    train_df = pd.read_csv(f"{data_path}/{coin}.csv")
    test_df = pd.read_csv(f"{data_path}/{coin}_test.csv")
    train_df["open_time"] = pd.to_datetime(train_df["open_time"])
    test_df["open_time"] = pd.to_datetime(test_df["open_time"])
    full_df = pd.concat([train_df, test_df])
    full_df = full_df.drop_duplicates(subset=["open_time"])
    full_df = full_df.sort_values(by=["open_time"])
    train_df, test_df = full_df[:-len(test_df)], full_df[-len(test_df):]
    if len(train_df) > RETENTION_MIN:
         test_df = test_df[-RETENTION_MIN:]
    print(f"Train size: {len(train_df)}, Test size: {len(test_df)}")
    print(f"Start date: {train_df.iloc[0]['open_time']}, End date: {test_df.iloc[-1]['open_time']}")
    
    train_df.to_csv(f"{data_path}/{coin}.csv", index=False)
    test_df.to_csv(f"{data_path}/{coin}_test.csv", index=False)
    manager.upload_df(f"{data_path}/{coin}.csv", bucket='mlops', key=f'prices/{coin}.parquet')
    manager.upload_df(f"{data_path}/{coin}_test.csv", bucket='mlops', key=f'prices/{coin}_test.parquet')
    

articles=f"./data/articles/articles.csv"
manager.upload_df(articles, bucket='mlops', key='articles/articles.parquet')


create_producer("ALL", "producer", "main")
import time
while state_checker("ALL", "producer", "main") != "running":
    time.sleep(1)
    print("Waiting for producer to start...")
    
print("Producer started!!!!.")
    