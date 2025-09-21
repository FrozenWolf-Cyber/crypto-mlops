# crypto_batch_db.py

from sqlalchemy import create_engine, text
from datetime import datetime


class CryptoBatchDB:
    def __init__(self, db_url: str):
        """
        Tracks both events (append-only, 1 year retention)
        and current status (snapshot).
        """
        self.engine = create_engine(
            db_url,
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
        )
        self._create_tables()

    def _create_tables(self):
        """Create event log and status tables if not exists."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS crypto_batch_events (
                id SERIAL PRIMARY KEY,
                dag_name VARCHAR(255),
                task_name VARCHAR(255),
                model_name VARCHAR(255),
                run_id VARCHAR(255),
                event_type VARCHAR(50),        -- e.g. START, RETRY, SUCCESS, FAILED, INFO
                status VARCHAR(20),            -- PENDING, RUNNING, SUCCESS, FAILED
                message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS crypto_batch_status (
                id SERIAL PRIMARY KEY,
                dag_name VARCHAR(255),
                task_name VARCHAR(255),
                model_name VARCHAR(255),
                run_id VARCHAR(255),
                status VARCHAR(20),            -- latest status
                retries INT DEFAULT 0,
                last_message TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(dag_name, task_name, model_name, run_id)
            );
            """
        ]
        with self.engine.begin() as conn:
            for q in queries:
                conn.execute(text(q))

    # ---------------------------
    # Event logging
    # ---------------------------
    def log_event(self, dag_name: str, task_name: str, model_name: str,
                  run_id: str, event_type: str, status: str = None, message: str = None):
        """Append event and also update snapshot table."""
        now = datetime.utcnow()

        # Insert into event log
        insert_event = text("""
            INSERT INTO crypto_batch_events 
                (dag_name, task_name, model_name, run_id, event_type, status, message, created_at)
            VALUES (:dag, :task, :model, :run, :etype, :status, :msg, :now);
        """)
        with self.engine.begin() as conn:
            conn.execute(insert_event, {
                "dag": dag_name,
                "task": task_name,
                "model": model_name,
                "run": run_id,
                "etype": event_type,
                "status": status,
                "msg": message,
                "now": now
            })

        # Update snapshot
        upsert_status = text("""
            INSERT INTO crypto_batch_status 
                (dag_name, task_name, model_name, run_id, status, retries, last_message, start_time, end_time, updated_at)
            VALUES (:dag, :task, :model, :run, :status, 0, :msg,
                    CASE WHEN :status = 'RUNNING' THEN :now ELSE NULL END,
                    CASE WHEN :status IN ('SUCCESS','FAILED') THEN :now ELSE NULL END,
                    :now)
            ON CONFLICT (dag_name, task_name, model_name, run_id) DO UPDATE
            SET status = EXCLUDED.status,
                retries = CASE 
                            WHEN EXCLUDED.status = 'RETRY' 
                            THEN crypto_batch_status.retries + 1 
                            ELSE crypto_batch_status.retries 
                          END,
                last_message = EXCLUDED.last_message,
                start_time = COALESCE(crypto_batch_status.start_time, EXCLUDED.start_time),
                end_time = CASE 
                              WHEN EXCLUDED.status IN ('SUCCESS','FAILED') 
                              THEN EXCLUDED.end_time 
                              ELSE crypto_batch_status.end_time 
                           END,
                updated_at = :now;
        """)
        with self.engine.begin() as conn:
            conn.execute(upsert_status, {
                "dag": dag_name,
                "task": task_name,
                "model": model_name,
                "run": run_id,
                "status": status,
                "msg": message,
                "now": now
            })

    # ---------------------------
    # Queries
    # ---------------------------
    def get_status(self, dag_name: str, run_id: str = None):
        """Fetch current snapshot (optionally filter by run_id)."""
        query = """
            SELECT * FROM crypto_batch_status WHERE dag_name = :dag
        """
        params = {"dag": dag_name}
        if run_id:
            query += " AND run_id = :run"
            params["run"] = run_id

        query += " ORDER BY updated_at DESC;"
        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
        return [dict(row) for row in rows]

    def get_events(self, dag_name: str, run_id: str = None, limit: int = 200):
        """Fetch recent events for timeline/flowmap view."""
        query = """
            SELECT * FROM crypto_batch_events WHERE dag_name = :dag
        """
        params = {"dag": dag_name}
        if run_id:
            query += " AND run_id = :run"
            params["run"] = run_id

        query += " ORDER BY created_at DESC LIMIT :limit;"
        params["limit"] = limit

        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
        return [dict(row) for row in rows]

    # ---------------------------
    # Maintenance
    # ---------------------------
    def cleanup_old_events(self):
        """Delete events older than 365 days."""
        query = text("""
            DELETE FROM crypto_batch_events
            WHERE created_at < NOW() - INTERVAL '365 days';
        """)
        with self.engine.begin() as conn:
            conn.execute(query)
    

import os
db_url = os.getenv("STATUS_DB")
status_db = CryptoBatchDB(db_url)