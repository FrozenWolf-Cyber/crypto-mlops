import os
import subprocess
import s3_manager
from s3_manager import S3Manager
from train_utils import download_s3_dataset
from consumer_utils import state_write, state_checker
import time
manager = S3Manager()

download_s3_dataset("BTCUSDT", trl_model=True)

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

cryptos = ["BTCUSDT", "ETHUSDT"]
models = ["lightgbm", "trl"]
versions = ["v1", "v2", "v3"]


procs = []
for crypto in cryptos:
    for model in models:
        # manager.download_available_predictions(crypto, model)
        versions_ = manager.get_existing_versions(crypto, model)
        print(f"[INFO] Existing versions for {crypto} {model}: {versions_}")
        for version in versions[:len(versions_)]:
            cmd = [
                "python", "consumer.py",
                "--crypto", crypto,
                "--model", model,
                "--version", version,
            ]
            print("Launching:", " ".join(cmd))
            procs.append(subprocess.Popen(cmd))

            while state_checker(crypto, model, version) == "unknown":
                time.sleep(0.5)
            print(f"State file for {crypto} {model} {version} exists, consumer launched.")

# Keep running until all finish
for p in procs:
    p.wait()
