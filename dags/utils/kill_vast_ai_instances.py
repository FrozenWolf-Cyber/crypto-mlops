import subprocess

def kill_all_vastai_instances():
    try:
        # Get list of instances (JSON output)
        result = subprocess.run(
            ["vastai", "show", "instances", "--raw"],
            capture_output=True, text=True, check=True
        )

        # Extract IDs with jq
        ids = subprocess.run(
            ["jq", "-r", ".[].id"],
            input=result.stdout,
            capture_output=True, text=True, check=True
        ).stdout.splitlines()

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



kill_all_vastai_instances()
