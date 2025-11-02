import time
import requests

# Base URL of your FastAPI backend
BASE_URL = "http://crypto-backend.gokuladethya.uk"   # change to your actual domain if needed

def trigger_apis():
    try:
        # 1️⃣ Trigger airflow batch_status
        airflow_resp = requests.get(f"{BASE_URL}/airflow/batch_status")
        print(f"[{time.ctime()}] ✅ /airflow/batch_status triggered | Status: {airflow_resp.status_code}")

        # 2️⃣ Trigger status events
        status_resp = requests.get(f"{BASE_URL}/status/events")
        print(f"[{time.ctime()}] ✅ /status/events triggered | Status: {status_resp.status_code}")

        # /keep_alive
        keep_alive_resp = requests.get(f"{BASE_URL}/keep_alive")
        print(f"[{time.ctime()}] ✅ /keep_alive triggered | Status: {keep_alive_resp}")

    except Exception as e:
        print(f"[{time.ctime()}] ❌ Error triggering APIs: {e}")

def main():
    while True:
        trigger_apis()
        print(f"⏳ Sleeping for 1 hour...\n")
        time.sleep(3600)  # 1 hour = 3600 seconds

if __name__ == "__main__":
    main()
