import os
import subprocess
from ..artifact_control.s3_manager import S3Manager
from ..trainer.train_utils import download_s3_dataset
from .consumer_utils import state_write, state_checker, STATE_DIR, delete_all_states, if_state_exists
import time
import logging
log = logging.getLogger(__name__)

manager = S3Manager()
log.info("Cleaning up old state files...")
log.info("Cleaning up old state files...")
delete_all_states()
log.info("Downloading initial datasets...")
download_s3_dataset("BTCUSDT", trl_model=True)

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

cryptos = ["BTCUSDT"]
models = ["lightgbm", "tst"]
versions = ["v1", "v2", "v3"]


available_comb = []
procs = []
for crypto in cryptos:
    for model in models:
        log.info(f"[INFO] Downloading available predictions for {crypto} {model}...")
        manager.download_available_predictions(crypto, model)
        versions_ = manager.get_existing_versions(crypto, model)
        log.info(f"[INFO] Existing versions for {crypto} {model}: {versions_}")
        for version in versions[:len(versions_)]:
            if if_state_exists(crypto, model, version):
                log.info(f"[INFO] Existing state file found for {crypto} {model} {version}, state: {state_checker(crypto, model, version)}")
                if state_checker(crypto, model, version) != "deleted":
                    log.info(f"[INFO] Existing consumer found for {crypto} {model} {version}, deleting it first...")
                    state_write(crypto, model, version, "delete")
                    available_comb.append((crypto, model, version))
                else:
                    ### just in case stiill exists
                    log.info(f"[INFO] Existing consumer for {crypto} {model} {version} is already deleted.")
                    state_write(crypto, model, version, "delete")
                    
            state_write(crypto, model, version, "delete")
            
log.info("Waiting for all existing consumers to die...")
time.sleep(360)  # give some time for states to be written  

check_producer = False
if if_state_exists("ALL", "producer", "main"):
    if state_checker("ALL", "producer", "main") != "deleted":
        state_write("ALL", "producer", "main", "delete")
        check_producer = True
        

for crypto, model, version in available_comb:
    while state_checker(crypto, model, version) != "deleted":
        log.info(f"Waiting for consumer {crypto} {model} {version} to die...")
        time.sleep(0.5)
    log.info(f"!!!✅✅✅✅✅✅Consumer for {crypto} {model} {version} is now deleted.")


delete_all_states()

os.makedirs(STATE_DIR, exist_ok=True)
