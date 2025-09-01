import time
from quixstreams import Application

KAFKA_BROKER = "localhost:9092"
CONTROL_TOPIC = "control_topic"

# Create the application
app = Application(broker_address=KAFKA_BROKER)

# Define control topic with JSON serializer
control_topic = app.topic(name=CONTROL_TOPIC, value_serializer="json")


def send_control_command(action: str, crypto: str, model: str, version: str):
    """
    Send a control command to pause, resume, or delete a consumer.
    action: 'pause' | 'resume' | 'delete'
    crypto: str, e.g. "BTCUSDT"
    model: str, e.g. "lightgbm"
    version: str, e.g. "v1"
    """
    if action not in {"pause", "resume", "delete"}:
        raise ValueError("Action must be 'pause', 'resume', or 'delete'")

    command = {"command": f"{action} {crypto} {model} {version}"}
    key = f"{crypto}_{model}_{version}"

    print(f"[CONTROL] Sending command: {command}")

    with app.get_producer() as producer:
        kafka_msg = control_topic.serialize(key=key, value=command)

        producer.produce(
            topic=control_topic.name,
            key=kafka_msg.key,
            value=kafka_msg.value,
        )


# --- Example test case ---
if __name__ == "__main__":
    send_control_command("resume", "BTCUSDT", "lightgbm", "v1")
    time.sleep(2)

    send_control_command("resume", "BTCUSDT", "tst", "v1")
    time.sleep(2)
	
