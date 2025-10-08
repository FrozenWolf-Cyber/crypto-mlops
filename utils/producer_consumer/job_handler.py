from .consumer_utils import state_checker, if_state_exists, state_write, delete_state, STATE_DIR, PRODUCER_CONSUMER_JOB_DIR
import logging
import time
log = logging.getLogger(__name__)
import os
import subprocess

def create_consumer(crypto: str, model: str, version: str):
    if os.path.exists(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")):
        print(f"[WARNING] State file for {crypto} {model} {version} already exists, removing it first.")
        os.remove(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json"))
    """Download datasets and launch consumer."""
    print(f"[CREATE] Preparing consumer for {crypto} {model} {version}")

    cmd = [
    "bash", "-c",
    f"python -m consumer "
    f"--crypto {crypto} --model {model} --version {version}"
    ]
    print("[CREATE] Launching:", " ".join(cmd))
    subprocess.Popen(
    cmd,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
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

    cmd = ["bash", "-c", "python -m producer"]
    print("[CREATE] Launching:", " ".join(cmd))
    subprocess.Popen(
    cmd,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    stdin=subprocess.DEVNULL,
    close_fds=True,
    start_new_session=True
)

print(f"[INFO] Watching job directory {PRODUCER_CONSUMER_JOB_DIR} for new jobs...")
while True:
    jobs = os.listdir(PRODUCER_CONSUMER_JOB_DIR)
    if not jobs:
        time.sleep(1)
        continue
    for job in jobs:
        # file_name = f"{crypto}_{model}_{version}.sh"
        if ".sh" not in job:
            print(f"[SKIP] Ignoring non-sh file {job}")
            continue
        job = job.replace(".sh", "")
        crypto, model, version = tuple(job.split("_"))
        print(f"[JOB] Found job for {crypto} {model} {version}")
        if model == "producer":
            create_producer(crypto, model, version)
            
        else:
            create_consumer(crypto, model, version)
            
        print(f"[JOB] Launched job for {crypto} {model} {version}, removing job file...")
        os.remove(os.path.join(PRODUCER_CONSUMER_JOB_DIR, f"{job}.sh"))
         