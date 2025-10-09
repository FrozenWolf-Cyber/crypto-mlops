import time
import os
import subprocess
import logging

STATE_DIR = "/opt/airflow/custom_persistent_shared/consumer_states"
PRODUCER_CONSUMER_JOB_DIR = "/opt/airflow/custom_persistent_shared/jobs"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def safe_makedirs(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logging.info(f"Created directory: {path}")
        else:
            logging.info(f"Directory already exists: {path}")
    except Exception as e:
        logging.error(f"Failed to create directory {path}: {e}", exc_info=True)

safe_makedirs(STATE_DIR)
safe_makedirs(PRODUCER_CONSUMER_JOB_DIR)

def remove_state_file(crypto, model, version):
    state_file = os.path.join(STATE_DIR, f"{crypto}_{model}_{version}.json")
    try:
        if os.path.exists(state_file):
            logging.warning(f"State file for {crypto} {model} {version} already exists, removing it first.")
            os.remove(state_file)
            logging.info(f"Removed existing state file: {state_file}")
    except Exception as e:
        logging.error(f"Error removing state file {state_file}: {e}", exc_info=True)

def launch_process(cmd, process_type):
    try:
        logging.info(f"[CREATE] Launching {process_type}: {' '.join(cmd)}")
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True
        )
        logging.info(f"[CREATE] {process_type} launched successfully.")
    except Exception as e:
        logging.error(f"[ERROR] Failed to launch {process_type}: {e}", exc_info=True)

def create_consumer(crypto: str, model: str, version: str):
    remove_state_file(crypto, model, version)
    logging.info(f"[CREATE] Preparing consumer for {crypto} {model} {version}")
    cmd = [
        "bash", "-c",
        f"python -m utils.producer_consumer.consumer "
        f"--crypto {crypto} --model {model} --version {version}"
    ]
    launch_process(cmd, "consumer")

def create_producer(crypto: str, model: str, version: str):
    remove_state_file(crypto, model, version)
    logging.info(f"[CREATE] Preparing producer for {crypto} {model} {version}")
    cmd = ["bash", "-c", "python -m utils.producer_consumer.producer"]
    launch_process(cmd, "producer")

logging.info(f"[INFO] Watching job directory {PRODUCER_CONSUMER_JOB_DIR} for new jobs...")

while True:
    try:
        jobs = os.listdir(PRODUCER_CONSUMER_JOB_DIR)
        if not jobs:
            time.sleep(1)
            continue

        for job in jobs:
            if ".sh" not in job:
                logging.info(f"[SKIP] Ignoring non-sh file {job}")
                continue

            job_name = job.replace(".sh", "")
            try:
                crypto, model, version = tuple(job_name.split("_"))
                logging.info(f"[JOB] Found job for {crypto} {model} {version}")

                if model == "producer":
                    create_producer(crypto, model, version)
                else:
                    create_consumer(crypto, model, version)

                job_path = os.path.join(PRODUCER_CONSUMER_JOB_DIR, job)
                os.remove(job_path)
                logging.info(f"[JOB] Launched job and removed job file: {job_path}")

            except Exception as e:
                logging.error(f"[ERROR] Failed to process job {job}: {e}", exc_info=True)

    except Exception as e:
        logging.error(f"[ERROR] Watching job directory failed: {e}", exc_info=True)
        time.sleep(5)  # Avoid tight loop if directory reading fails
