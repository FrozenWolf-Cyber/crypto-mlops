# crypto_batch_db.py

from sqlalchemy import create_engine, text
from datetime import datetime


class BatchStatusDB:
    """
    Simple DB wrapper to track model/coin training runs.
    - Stores state (PENDING, RUNNING, SUCCESS, FAILED)
    - Training scripts call set_state() as they progress
    - Airflow polls with get_status() to check when jobs are done
    """

    def __init__(self, db_url: str):
        # Small worker pool for lightweight status tracking
        self.engine = create_engine(
            db_url,
            pool_size=2,        # small pool size
            max_overflow=0,
            pool_pre_ping=True,
        )
        self._create_table()

    def _create_table(self):
        """Create batch_status table if not exists."""
        query = """
        CREATE TABLE IF NOT EXISTS batch_status (
            id SERIAL PRIMARY KEY,
            model TEXT NOT NULL,
            coin TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'PENDING',
            updated_at TIMESTAMP DEFAULT now(),
               error_message TEXT DEFAULT NULL
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def flush(self):
        """Reset table at DAG start or before a new cycle."""
        with self.engine.begin() as conn:
            conn.execute(text("TRUNCATE batch_status RESTART IDENTITY;"))

    def init_entries(self, coins):
        """Insert initial rows with PENDING state."""
        with self.engine.begin() as conn:
            for model in ["lightgbm", "tst"]:
                for coin in coins:
                    conn.execute(
                        text("""
                        INSERT INTO batch_status (model, coin, state)
                        VALUES (:m, :c, 'PENDING')
                        """),
                        {"m": model, "c": coin},
                    )
            conn.execute(text("INSERT INTO batch_status (model, coin, state) VALUES ('trl', 'ALL', 'PENDING')"))

    def set_state(self, model, coin, state, error_message=None):
        """
        Update state for a given model/coin.
        state = 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED'
        error_message = optional string with Python exception
        """
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                UPDATE batch_status
                SET state = :s, error_message = :e
                WHERE model = :m AND coin = :c
                """),
                {
                    "s": state,
                    "e": error_message,
                    "m": model,
                    "c": coin,
                },
            )


    def get_status(self):
        """Return all job states as a list of dicts."""
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT model, coin, state, error_message FROM batch_status")
            ).fetchall()
            return [
                {"model": r[0], "coin": r[1], "state": r[2], "error_message": r[3]}
                for r in rows
            ]


import os
AIRFLOW_DB = os.getenv("AIRFLOW_DB")

db = BatchStatusDB(AIRFLOW_DB)
# -------------------------------------------------------------------
# Usage Examples
# -------------------------------------------------------------------
#
# db_url = "........."
# db = BatchStatusDB(db_url)
#
# --- In Airflow DAG start (reset + init jobs) ---
# db.flush()
# db.init_entries(models=["tst", "lightgbm", "trl"], coins=["BTCUSDT", "ETHUSDT"])
#
# --- In training script ---
# db.set_state("tst", "BTCUSDT", "RUNNING")
# ... training code ...
# db.set_state("tst", "BTCUSDT", "SUCCESS")
#
# --- In Airflow sensor ---
# states = db.get_status()
# if all(s["state"] in ("SUCCESS", "FAILED") for s in states):
#     # All jobs finished, continue DAG
