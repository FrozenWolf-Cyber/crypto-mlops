import os
import json
import time
import logging

log = logging.getLogger(__name__)

STATE_DIR = "/opt/airflow/custom_persistent_shared/consumer_states"
PRODUCER_CONSUMER_JOB_DIR = "/opt/airflow/custom_persistent_shared/jobs"
# Ensure the state directory exists
if not os.path.exists(STATE_DIR):
    os.makedirs(STATE_DIR, exist_ok=True)
    
if not os.path.exists(PRODUCER_CONSUMER_JOB_DIR):
    os.makedirs(PRODUCER_CONSUMER_JOB_DIR, exist_ok=True)

def state_write(crypto: str, model: str, version: str, state: str, error_msg: str = ""):
    """
    Write the current state of a consumer to a JSON file.
    state: e.g., "running", "paused", "deleted"
    """
    state_file = os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")
    data = {
        "crypto": crypto,
        "model": model,
        "version": version,
        "state": state,
        "error_msg": error_msg,
    }
    with open(state_file, "w") as f:
        json.dump(data, f)
    print(f"[STATE] {crypto} {model} {version} -> {state}")

def state_checker(crypto: str, model: str, version: str, timeout=120) -> str:
    """
    Check the current state of a consumer.
    Returns the state string if exists, otherwise "unknown".
    Any error (file missing, invalid JSON, etc.) falls back to "unknown".
    """
    state_file = os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")
    print(f"[STATE CHECK] Checking state for {crypto} {model} {version}...")
    start = time.time()
    while True:
        try:
            if not os.path.exists(state_file):
                if time.time() - start > timeout:
                    print(f"[STATE CHECK] State file {state_file} not found after 120 seconds, returning 'unknown'.")
                    return "unknown"
                log.warning("State file %s not found, retrying...", state_file)
                time.sleep(1)  # avoid busy looping
                continue

            with open(state_file, "r") as f:
                data = json.load(f)
                print(data.get("state", "unknown"))
                return data.get("state", "unknown")
        except Exception as e:
            log.error("Error reading state file %s: %s. Retrying...", state_file, e)
            time.sleep(1)  # backoff before retrying

def if_state_exists(crypto: str, model: str, version: str) -> bool:
    if os.path.exists(os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")):
        return True
    return False


def delete_state(crypto: str, model: str, version: str):
    """
    Delete the state file for a specific consumer.
    """
    state_file = os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")
    if os.path.exists(state_file):
        os.remove(state_file)
        print(f"[STATE] Deleted state file for {crypto} {model} {version}")
    else:
        print(f"[STATE] No state file to delete for {crypto} {model} {version}")
        
def delete_all_states():
    """
    Delete all state files in the STATE_DIR.
    """
    for filename in os.listdir(STATE_DIR):
        if filename.endswith(".json"):
            file_path = os.path.join(STATE_DIR, filename)
            os.remove(file_path)
            print(f"[STATE] Deleted state file {file_path}")