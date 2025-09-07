# start_training.py
import subprocess
import os
from train_utils import save_start_time

save_start_time()

COINS = ["BTCUSDT", "ETHUSDT"]  # your coins list
full_path = os.path.dirname(os.path.abspath(__file__))  # repo root
logs_path = os.path.join(full_path, "logs")
os.makedirs(logs_path, exist_ok=True)

processes = []
max_time = 5 * 60

# trl_train
trl_cmd = [
    "python", os.path.join(full_path, "trl_train.py"),
    "--epochs", "10",
    "--batch_size", "12",
    "--max_time", str(max_time)
]
with open(os.path.join(logs_path, "trl.log"), "w") as trl_log:
    processes.append(
        subprocess.Popen(trl_cmd, stdout=trl_log, stderr=subprocess.STDOUT)
    )

# per-coin trainings
for coin in COINS:
    # tst_train
    tst_cmd = [
        "python", os.path.join(full_path, "tst_train.py"),
        "--coin", coin,
        "--epochs", "10",
        "--batch_size", "200",
        "--seq_len", "30",
        "--max_time", str(max_time)
    ]
    with open(os.path.join(logs_path, f"{coin}_tst.log"), "w") as tst_log:
        processes.append(
            subprocess.Popen(tst_cmd, stdout=tst_log, stderr=subprocess.STDOUT)
        )

    # lgb_train (fixed missing comma)
    lgb_cmd = [
        "python", os.path.join(full_path, "lgb_train.py"),
        "--coin", coin,
        "--epochs", "200",
        "--max_time", str(max_time)
    ]
    with open(os.path.join(logs_path, f"{coin}_lgbm.log"), "w") as lgb_log:
        processes.append(
            subprocess.Popen(lgb_cmd, stdout=lgb_log, stderr=subprocess.STDOUT)
        )

print("All training scripts started in parallel and detached.")
