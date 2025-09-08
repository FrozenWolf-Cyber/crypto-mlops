import subprocess
import json
import time
import os
import subprocess

def calculate_full_pod_cost(pod: dict, hours: float = 1, 
                             extra_storage_tb: float = 0.15,
                             internet_up_tb: float = 0.005,
                             internet_down_tb: float = 0.01) -> dict:
    """
    Calculate the full rental cost for a Vast.ai pod.

    Parameters:
    - pod: dict of Vast.ai pod info (API response).
    - hours: number of hours to rent.
    - extra_storage_tb: additional storage allocated beyond included (in TB).
    - internet_up_tb: data uploaded to internet (in TB).
    - internet_down_tb: data downloaded from internet (in TB).

    Returns:
    - dict with breakdown and total cost.
    """

    # Core hourly costs (from Vast.ai pricing API)
    dph_total = pod.get("dph_total", 0)  # $/hour
    # discounted_dph_total = pod.get("discounted_dph_total", dph_total)
    base_hourly_cost = dph_total * hours

    # Storage: pod includes `storage_total_cost` already in dph_total.
    # If user adds extra storage, charge prorated hourly per TB.
    storage_monthly_cost_per_tb = pod.get("storage_cost", 0)
    storage_hourly_per_tb = storage_monthly_cost_per_tb / (30 * 24)
    extra_storage_cost = storage_hourly_per_tb * extra_storage_tb * hours

    # Network transfer costs (charged per TB, not hourly)
    inet_up_transfer_cost = pod.get("internet_up_cost_per_tb", 0) * internet_up_tb
    inet_down_transfer_cost = pod.get("internet_down_cost_per_tb", 0) * internet_down_tb

    total_cost = base_hourly_cost + extra_storage_cost + inet_up_transfer_cost + inet_down_transfer_cost

    # return {
    #     "base_hourly_cost": base_hourly_cost,
    #     "extra_storage_cost": extra_storage_cost,
    #     "inet_up_transfer_cost": inet_up_transfer_cost,
    #     "inet_down_transfer_cost": inet_down_transfer_cost,
    #     "total_cost": total_cost
    # }

    return total_cost





def get_offers():
    cmd = [
        "vastai", "search", "offers",
        "gpu_total_ram>=11 disk_space>=30 verified=True datacenter=True",
        "--on-demand",
        "-o", "dph",
        "--raw"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)  # <-- already a list


while True:
    offers = get_offers()
    print(f"Fetched {len(offers)} offers")

    filtered_offers = [pod for pod in offers if calculate_full_pod_cost(pod) <= 0.15]
    filtered_offers.sort(key=calculate_full_pod_cost)

    print(f"Filtered {len(filtered_offers)} offers under cap")

    for i, pod in enumerate(filtered_offers[:5]):
        total_cost = calculate_full_pod_cost(pod)
        print(f"Pod {i+1}: ID {pod['id']}, GPU RAM {pod['gpu_ram']} MB, Disk {pod['disk_space']} GB, GPU {pod['gpu_name']}")
        print(f"  Hourly cost: ${pod['dph_total']:.4f}")
        print(f"  Total cost: ${total_cost:.4f}")

    if not filtered_offers:
        print("No pods under cap with required GPU RAM and disk. Retrying in 30 seconds...\n")
        time.sleep(30)
        continue

    best_pod = filtered_offers[0]
    print("="*40)
    print("Chosen Pod ID:", best_pod['id'], "Total price:", calculate_full_pod_cost(best_pod))
    POD_ID = best_pod['id']
    print("Choosing pod ID:", POD_ID)
    break




POD_ID = str(best_pod["id"])  # from your search loop
# POD_ID = "13293128"
# Collect environment variables (ignore None values)
env_map = {
    "MLFLOW_S3_ENDPOINT_URL": os.getenv("MLFLOW_S3_ENDPOINT_URL"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "MLFLOW_SQLALCHEMY_POOL_SIZE": os.getenv("MLFLOW_SQLALCHEMY_POOL_SIZE"),
    "MLFLOW_SQLALCHEMY_MAX_OVERFLOW": os.getenv("MLFLOW_SQLALCHEMY_MAX_OVERFLOW"),
    "S3_URL": os.getenv("S3_URL"),
    "DATABASE_URL": os.getenv("DATABASE_URL"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
    "MLFLOW_URI": os.getenv("MLFLOW_URI"),
    "MLFLOW_TRACKING_USERNAME": "admin",
    "MLFLOW_TRACKING_PASSWORD": os.getenv("MLFLOW_ADMIN_PASSWORD"),
    "AIRFLOW_DB": os.getenv("AIRFLOW_DB"),
}

export_string = "; ".join(
    f"export {k}='{v}'" for k, v in env_map.items() if v is not None
)

print("Using env vars:\n", export_string)


# Onstart command
# Onstart command using train_paralelly.py
onstart_cmd = (
    f"{export_string} && cd /app && "
    f"git clone https://github.com/FrozenWolf-Cyber/crypto-mlops && pip install wandb && wandb login 283c41dda88b658ba85c2d8ee7d37230f3341d8c &&"
    f"cd crypto-mlops/dags && "
    f"python -m trainer.train_paralelly"
)

print(onstart_cmd)

# Build Vast.ai command
cmd = [
    "vastai", "create", "instance", POD_ID,
    "--image", "frozenwolf2003/mlops:latest",
    "--onstart-cmd", onstart_cmd,
    "--disk", "30",
    "--ssh",
]


# Run
result = subprocess.run(cmd, capture_output=True, text=True, check=True)

print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
