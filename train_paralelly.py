# start_training.py
import subprocess
import os

COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # your coins list
full_path = os.path.dirname(os.path.abspath(__file__))  # repo root
logs_path = os.path.join(full_path, "logs")
os.makedirs(logs_path, exist_ok=True)

processes = []
max_time = 50*60

# trl_train
trl_cmd = [
    "python", os.path.join(full_path, "trl_train.py"),
    "--epochs", "10",
    "--batch_size", "12",
    "--max_time", str(max_time)  
]
trl_log = open(os.path.join(logs_path, "trl.log"), "w")
p = subprocess.Popen(trl_cmd, stdout=trl_log, stderr=subprocess.STDOUT)
processes.append(p)


#  python --epochs 10 --batch_size 12 --max_time 3000
# for coin in COINS:
#     # tst_train
#     tst_cmd = [
#         "python", os.path.join(full_path, "tst_train.py"),
#         "--coin", coin,
#         "--epochs", "10",
#         "--batch_size", "200",
#         "--seq_len", "30",
#         "--max_time", str(max_time) 
#     ]
#     tst_log = open(os.path.join(logs_path, f"{coin}_tst.log"), "w")
#     p = subprocess.Popen(tst_cmd, stdout=tst_log, stderr=subprocess.STDOUT)
#     processes.append(p)

#     # lgb_train
#     lgb_cmd = [
#         "python", os.path.join(full_path, "lgb_train.py"),
#         "--coin", coin,
#         "--epochs", "200"
#     ]
#     lgb_log = open(os.path.join(logs_path, f"{coin}_lgbm.log"), "w")
#     p = subprocess.Popen(lgb_cmd, stdout=lgb_log, stderr=subprocess.STDOUT)
#     processes.append(p)


# # Optional: wait for all to finish (or just leave them running in parallel)
# for p in processes:
#     p.wait()

print("All training scripts started in parallel.")
