import subprocess
import json
import time
import os
import pickle
from pathlib import Path

key = os.getenv("VASTAI_API_KEY")
if not key:
    raise ValueError("VASTAI_API_KEY environment variable not set.")

subprocess.run(["vastai", "set", "api-key", key], check=True)
print("Vast.ai API key set successfully: ", key[:4] + "****")


from .kill_vast_ai_instances import kill_all_vastai_instances
kill_all_vastai_instances()
BLACKLIST_FILE = Path("/opt/airflow/custom_persistent_shared/blacklisted_machines.pkl")


def load_blacklist():
    if BLACKLIST_FILE.exists():
        with open(BLACKLIST_FILE, "rb") as f:
            return pickle.load(f)
    return set()


def save_blacklist(blacklist):
    with open(BLACKLIST_FILE, "wb") as f:
        pickle.dump(blacklist, f)


def calculate_full_pod_cost(pod: dict, hours: float = 1,
                             extra_storage_tb: float = 0,
                             internet_up_tb: float = 0.005,
                             internet_down_tb: float = 0.01) -> float:
    dph_total = pod.get("dph_total", 0)  # $/hour
    base_hourly_cost = dph_total * hours

    storage_monthly_cost_per_tb = pod.get("storage_cost", 0)
    storage_hourly_per_tb = storage_monthly_cost_per_tb / (30 * 24)
    extra_storage_cost = storage_hourly_per_tb * extra_storage_tb * hours

    inet_up_transfer_cost = pod.get("internet_up_cost_per_tb", 0) * internet_up_tb
    inet_down_transfer_cost = pod.get("internet_down_cost_per_tb", 0) * internet_down_tb

    total_cost = base_hourly_cost + extra_storage_cost + inet_up_transfer_cost + inet_down_transfer_cost
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


def wait_for_pod(instance_id: str, timeout: int = 600) -> bool:
    """
    Poll `vastai show instances --raw` until the given instance_id is running
    or timeout (default 600s) is reached.
    """
    start = time.time()
    time.sleep(5) # initial wait before first poll
    while time.time() - start < timeout:
        cmd = ["vastai", "show", "instances", "--raw"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        if result.returncode != 0:
            print("Error checking instance:", result.stderr)
            kill_all_vastai_instances()
            return False

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            print("Failed to parse JSON:", result.stdout)
            time.sleep(30)
            continue

        print("Number of instances:", len(data))
        instance = None
        for i in data:
            print(f"ID: {i['id']}, Status: {i['actual_status']}")
            if int(i["id"]) == int(instance_id):
                instance = i
                print("Found instance:", instance)
                break
        
        print("Full instance data:", data)
        if not instance:
            print(f"Instance {instance_id} not found")
            time.sleep(30)
            continue

        state = instance.get("actual_status")
        print(f"Instance {instance_id} is {state}")

        if state == "running":
            print("SSH available at:", instance.get("ssh_host"), ":", instance.get("ssh_port"))
            return True

        time.sleep(30)  # poll every 30s

    print(f"Timeout reached: instance {instance_id} did not start in {timeout}s")
    return False


def create_instance(DEBUG=False):
    BUDGET = 0.2
    FIND_POD_SLEEP = 30  # seconds
    MAX_POD_WAIT = 600  # seconds
    MAX_TIME_RETRY = 1200 # seconds
    start = time.time()
    # ------------------ Main loop -------------------
    blacklist = load_blacklist()
    print(f"Loaded blacklist with {len(blacklist)} machine IDs")
    print("Blacklisted machine IDs:", blacklist)
    while True:
        if time.time() - start > MAX_TIME_RETRY:
            print("Exceeded maximum retry time. Exiting.")
            return
        offers = get_offers()
        print(f"Fetched {len(offers)} offers")

        # filter by cost and blacklist
        filtered_offers = [
            pod for pod in offers
            if calculate_full_pod_cost(pod) <=BUDGET  and pod["machine_id"] not in blacklist
        ]
        filtered_offers.sort(key=calculate_full_pod_cost)

        print(f"Filtered {len(filtered_offers)} offers under cap")

        if not filtered_offers:
            print("No pods available under cap and not blacklisted. Retrying in 30s...\n")
            time.sleep(FIND_POD_SLEEP)
            continue

        best_pod = filtered_offers[0]
        POD_ID = str(best_pod["id"])
        MACHINE_ID = best_pod["machine_id"]

        for possible_pods in filtered_offers:
            print(f"Pod ID: {possible_pods['id']}, Machine ID: {possible_pods['machine_id']}, Total price: {calculate_full_pod_cost(possible_pods):.4f}")
        print("="*40)
        print(f"Chosen Pod ID: {POD_ID}, Machine ID: {MACHINE_ID}, Total price: {calculate_full_pod_cost(best_pod):.4f}")

        # Collect environment variables
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

        onstart_cmd = (
            f"{export_string} && cd /app && "
            "git clone https://github.com/FrozenWolf-Cyber/crypto-mlops && "
            "pip install wandb && "
            "wandb login 283c41dda88b658ba85c2d8ee7d37230f3341d8c && "
            "cd crypto-mlops/utils && "
            "python -m trainer.train_paralelly"
        )

        cmd = [
            "vastai", "create", "instance", POD_ID,
            "--image", "frozenwolf2003/mlops:latest",
            "--onstart-cmd", onstart_cmd,
            "--disk", "30",
            "--ssh",
        ]

        if DEBUG:
            print("\n\n")
            time.sleep(FIND_POD_SLEEP)  # sleep to simulate long wait
            continue  # skip actual creation in debug mode
        break


    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    import ast
    output = result.stdout.strip()
    # Extract the part after "Started. "
    data_str = output.split("Started. ", 1)[1]
    # Convert string to Python dict safely
    data = ast.literal_eval(data_str)
    instance_id = data["new_contract"]
    print("Contract ID:", instance_id)
    print(f"Waiting for pod {instance_id} to start...")
    if wait_for_pod(instance_id, timeout=MAX_POD_WAIT): ## wait up to 10 minutes
        print("Pod is running! ✅") 
    else:
        print("Pod did not start in 10 minutes. Killing instances and blacklisting machine.")
        kill_all_vastai_instances()
        blacklist.add(MACHINE_ID)
        save_blacklist(blacklist)
        raise RuntimeError("Pod failed to start in time. Machine blacklisted.")

    
if __name__ == "__main__":
    create_instance(DEBUG=False)  # set DEBUG=False to actually create instance