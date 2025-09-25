import os
import subprocess
from ..artifact_control.s3_manager import S3Manager
from ..trainer.train_utils import download_s3_dataset
from .consumer_utils import state_write, state_checker, STATE_DIR, delete_all_states
import time
import logging
log = logging.getLogger(__name__)

manager = S3Manager()
print("Cleaning up old state files...")
print("Cleaning up old state files...")
delete_all_states()
print("Downloading initial datasets...")
download_s3_dataset("BTCUSDT", trl_model=True)

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

cryptos = ["BTCUSDT"]
models = ["lightgbm", "trl"]
versions = ["v1", "v2", "v3"]


def create_consumer(crypto: str, model: str, version: str):
    if os.path.exists(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")):
        print(f"[WARNING] State file for {crypto} {model} {version} already exists, removing it first.")
        os.remove(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json"))
    """Download datasets and launch consumer."""
    print(f"[CREATE] Preparing consumer for {crypto} {model} {version}")

    cmd = [
    "bash", "-c",
    f"PYTHONPATH=..:$PYTHONPATH python -m utils.producer_consumer.consumer "
    f"--crypto {crypto} --model {model} --version {version}"
    ]
    print("[CREATE] Launching:", " ".join(cmd))
    subprocess.Popen(
    cmd,
    stdout=None,
    stderr=None,
    stdin=subprocess.DEVNULL,
    close_fds=True,
    start_new_session=True
)
    

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
    stdout=None,
    stderr=None,
    stdin=subprocess.DEVNULL,
    close_fds=True,
    start_new_session=True
)

create_producer("ALL", "producer", "main")
import time
while state_checker("ALL", "producer", "main") != "running":
    time.sleep(1)
    print("Waiting for producer to start...")
    
print("Producer started!!!!.")
    
    
procs = []
for crypto in cryptos:
    for model in models:
        print(f"[INFO] Downloading available predictions for {crypto} {model}...")
        manager.download_available_predictions(crypto, model)
        versions_ = manager.get_existing_versions(crypto, model)
        print(f"[INFO] Existing versions for {crypto} {model}: {versions_}")
        for version in versions[:len(versions_)]:
            print(f"[INFO] Creating consumer for {crypto} {model} {version}...")
            create_consumer(crypto, model, version)

            while state_checker(crypto, model, version) == "unknown":
                time.sleep(0.5)
            print(f"!!!State file for {crypto} {model} {version} exists, consumer waiting.")
            print(f"[INFO] Setting state to 'start' for {crypto} {model} {version}...")
            state_write(crypto, model, version, "start")
            
            while state_checker(crypto, model, version) != "running":
                time.sleep(0.5)
            print(f"!!!Consumer for {crypto} {model} {version} is now running.")
