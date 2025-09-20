import subprocess
import json

def kill_all_vastai_instances():
    try:
        # Get list of instances (JSON output)
        result = subprocess.run(
            ["vastai", "show", "instances", "--raw"],
            capture_output=True, text=True, check=True
        )

        # Parse JSON and extract IDs
        instances = json.loads(result.stdout)
        ids = [str(inst["id"]) for inst in instances]

        if not ids:
            print("No Vast.ai instances running.")
            return

        # Destroy each instance
        for inst_id in ids:
            print(f"Destroying Vast.ai instance {inst_id}...")
            subprocess.run(["vastai", "destroy", "instance", inst_id], check=True)

        print("✅ All Vast.ai instances destroyed.")

    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON from Vast.ai:", e)

kill_all_vastai_instances()
