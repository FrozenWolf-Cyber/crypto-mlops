# start_training.py
import subprocess
import os
from .train_utils import save_start_time

save_start_time()

COINS = ["BTCUSDT"]  # your coins list
full_path = os.path.dirname(os.path.abspath(__file__))  # repo root
logs_path = os.path.join(full_path, "logs")
os.makedirs(logs_path, exist_ok=True)

print(full_path)
processes = []
max_time = int(55*60) ###TODO: DEBUG 55 minutes max for each training

# trl_train
trl_cmd = [
    "python", "-m",  "utils.trainer.trl_train",
    "--epochs", "20",
    "--batch_size", "12",
    "--max_time", str(max_time)
]
with open(os.path.join(logs_path, "trl.log"), "w") as trl_log:
    processes.append(
        subprocess.Popen(trl_cmd, stdout=trl_log, stderr=subprocess.STDOUT)
    )

# per-coin trainings
for coin in COINS:
    print("Environment variable", os.getenv("AIRFLOW_DB"))
    # tst_train
    tst_cmd = [
        "python", "-m",  "utils.trainer.tst_train",
        "--coin", coin,
        "--epochs", "30",
        "--batch_size", "96",
        "--seq_len", "30",
        "--max_time", str(max_time)
    ]
    with open(os.path.join(logs_path, f"{coin}_tst.log"), "w") as tst_log:
        processes.append(
            subprocess.Popen(tst_cmd, stdout=tst_log, stderr=subprocess.STDOUT)
        )

    # lgb_train (fixed missing comma)
    lgb_cmd = [
         "python", "-m",  "utils.trainer.lgb_train",
        "--coin", coin,
        "--epochs", "500",
        "--max_time", str(max_time)
    ]
    with open(os.path.join(logs_path, f"{coin}_lgbm.log"), "w") as lgb_log:
        processes.append(
            subprocess.Popen(lgb_cmd, stdout=lgb_log, stderr=subprocess.STDOUT)
        )
        
        

print("All training scripts started in parallel and detached.")

for p in processes:
    p.wait()