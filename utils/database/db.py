from sqlalchemy import create_engine, text
import pandas as pd
from datetime import timedelta
from tqdm import tqdm
import os
import json
import numpy as np

def normalize_pred(x):
    if isinstance(x, np.ndarray):
        return x.tolist()
    if isinstance(x, (tuple, list)):
        return list(x)
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                # try JSON first
                return json.loads(s.replace(" ", ","))
            except Exception:
                # fallback: NumPy parse space-separated
                return np.fromstring(s.strip("[]"), sep=" ").tolist()
        return [float(s)]
    return [float(x)]

required_cols = ["open_time", "open", "high", "low", "close", "volume"]

class CryptoDB:
    def __init__(self, engine, coins, data_path="./data/prices", wanted_columns=None):
        self.engine = engine
        self.coins = coins
        self.data_path = data_path
        self.wanted_columns = wanted_columns or ["open_time", "open", "high", "low", "close", "volume"]

        # Initialize tables and update with CSV
        for coin in self.coins:
            self._create_table_if_not_exists(coin)
            self.update_from_csv(coin)
        
        self.create_TRL_tables()

    def _create_table_if_not_exists(self, coin):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{coin.lower()}" (
            open_time TIMESTAMP NOT NULL PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            tst_1 FLOAT[] DEFAULT NULL,
            tst_2 FLOAT[] DEFAULT NULL,
            tst_3 FLOAT[] DEFAULT NULL,
            lightgbm_1 FLOAT[] DEFAULT NULL,
            lightgbm_2 FLOAT[] DEFAULT NULL,
            lightgbm_3 FLOAT[] DEFAULT NULL
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_table_query))
            
    def create_TRL_tables(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "trl" (
            title TEXT,
            link TEXT NOT NULL PRIMARY KEY,
            date TIMESTAMP NOT NULL,
            "trl_1" FLOAT[] DEFAULT NULL,
            "trl_2" FLOAT[] DEFAULT NULL,
            "trl_3" FLOAT[] DEFAULT NULL,
            price_change FLOAT DEFAULT NULL,
            label INT DEFAULT NULL
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_table_query))

    def reset_trl_version(self, version: int):
        """
        Sets the entire trl_{version} column to NULL in the 'trl' table.
        """
        trl_column = f"trl_{version}"
        query = f"""
            UPDATE trl
            SET {trl_column} = NULL
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))

        print(f"All values in column '{trl_column}' have been set to NULL.")


    def upsert_trl_full(self, df, version=1):
        """
        Full upsert into 'trl' table:
        - Updates existing rows for the given trl_version column
        - Inserts missing rows
        - Stores 'pred' as FLOAT[] in the specified trl column
        - Uses temp table if >100 rows
        - 'label' and 'price_change' can be None
        - Uses 'link' as the unique identifier
        """
        table_name = "trl"
        trl_column = f"trl_{version}"
    
        # Step 0: Convert pred string to list of floats
        if df['pred'].dtype == object:
            if isinstance(df['pred'].iloc[0], str):
                df['pred_list'] = df['pred'].apply(
                    lambda x: [float(i) for i in x.strip('[]').split()]
                )
            else:
                df['pred_list'] = df['pred']
        else:
            df['pred_list'] = df['pred']
    
        # Ensure optional columns exist (default None if missing)
        if "label" not in df.columns:
            df["label"] = None
        if "price_change" not in df.columns:
            df["price_change"] = None
    
        df_to_upsert = df.copy()
    
        print(f"Upserting {len(df_to_upsert)} rows into {table_name}...")
    
        # Step 2: Use temp table for bulk operations if >100 rows
        if len(df_to_upsert) > 100:
            print("Using temp table for bulk upsert...")
            temp_table = "trl_temp"
            df_temp = df_to_upsert.copy()
            df_temp[trl_column] = df_temp['pred_list']
            df_temp.to_sql(temp_table, self.engine, if_exists='replace', index=False)
    
            # Update existing rows (join on link)
            update_query = f"""
            UPDATE {table_name} AS t
            SET {trl_column} = tmp.{trl_column}::double precision[],
                label = tmp.label,
                price_change = tmp.price_change,
                title = tmp.title,
                date = tmp.date
            FROM {temp_table} AS tmp
            WHERE t.link = tmp.link;
            """
    
            # Insert missing rows
            insert_query = f"""
            INSERT INTO {table_name} (title, link, date, {trl_column}, price_change, label)
            SELECT tmp.title, tmp.link, tmp.date, tmp.{trl_column}::double precision[],
                   tmp.price_change, tmp.label
            FROM {temp_table} AS tmp
            WHERE NOT EXISTS (SELECT 1 FROM {table_name} t2 WHERE t2.link = tmp.link);
            """
            with self.engine.begin() as conn:
                conn.execute(text(update_query))
                conn.execute(text(insert_query))
    
            print(f"Upserted {len(df_to_upsert)} rows using temp table.")
    
        else:
            # Small dataset, row by row
            for _, row in df_to_upsert.iterrows():
                kwargs = {
                    'title': row['title'],
                    'link': row['link'],
                    'date': row['date'],
                    trl_column: row['pred_list'],
                    'price_change': row.get('price_change', None),
                    'label': None if pd.isna(row.get('label', None)) else int(row['label'])
                }
                upsert_query = f"""
                INSERT INTO {table_name} (title, link, date, {trl_column}, price_change, label)
                VALUES (:title, :link, :date, :{trl_column}, :price_change, :label)
                ON CONFLICT (link) DO UPDATE
                SET {trl_column} = EXCLUDED.{trl_column},
                    title = EXCLUDED.title,
                    date = EXCLUDED.date,
                    price_change = EXCLUDED.price_change,
                    label = EXCLUDED.label;
                """
                with self.engine.begin() as conn:
                    conn.execute(text(upsert_query), kwargs)
            print(f"Upserted {len(df_to_upsert)} rows individually.")



    def insert_if_not_exists(self, df, table_name="trl"):
        """
        Insert rows into `table_name` only if the 'link' does not already exist.
        Assumes df has columns: title, link, date, pred (list of floats), price_change, label (int)
        """
        # Convert 'pred' to list of floats if needed
        if df['pred'].dtype == object and isinstance(df['pred'].iloc[0], str):
            df['pred_list'] = df['pred'].apply(lambda x: [float(i) for i in x.strip('[]').split()])
        else:
            df['pred_list'] = df['pred']

        rows_inserted = 0
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (title, link, date, pred, price_change, label)
                SELECT :title, :link, :date, :pred_list::double precision[], :price_change, :label
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} WHERE link = :link
                );
                """
                kwargs = {
                    'title': row['title'],
                    'link': row['link'],
                    'date': row['date'],
                    'pred_list': row['pred_list'],
                    'price_change': row['price_change'],
                    'label': int(row['label'])
                }
                result = conn.execute(text(insert_query), kwargs)
                rows_inserted += result.rowcount

        print(f"Inserted {rows_inserted} new rows into {table_name}, skipped existing links.")

    def get_last_crypto_date(self):
        """
        Returns the latest date for a given crypto from the 'trl' table.
        If no rows exist, returns None.
        """
        query = """
            SELECT MAX(date) AS last_date
            FROM trl;
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            last_date = result[0] if result else None
    
        return pd.to_datetime(last_date) if last_date else None





    def update_from_csv(self, coin):
        print(f"Processing {coin}...")
    
        print(f"Reading CSV from {self.data_path}/{coin}.csv")
        df = pd.read_csv(f'{self.data_path}/{coin}.csv')[self.wanted_columns]
        df['open_time'] = pd.to_datetime(df['open_time'])
    
        query_last_date = text(f"SELECT MAX(open_time) as last_date FROM {coin.lower()};")
        with self.engine.begin() as conn:
            result = conn.execute(query_last_date).scalar()
        last_date = result if result else None
    
        print(f"Last date in DB for {coin}: {last_date}")
        print(f"Last date in CSV for {coin}: {df['open_time'].max()}")
    
        df_new = df[df['open_time'] > last_date] if last_date is not None else df
        print(f"New rows to insert for {coin}: {len(df_new)}")
    
        if not df_new.empty:
            self.bulk_insert_df(coin, df_new)
        else:
            print("No new rows to insert.")

    def bulk_insert_df(self, table_name, df):
        table_name = table_name.lower()
        df.to_sql(
            table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10000,
        )
        print(f"Bulk inserted {len(df)} rows into {table_name}.")


    def insert_row(self, table_name, **kwargs):
        table_name = table_name.lower()
        columns = ', '.join(kwargs.keys())
        placeholders = ', '.join([f":{col}" for col in kwargs.keys()])
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        with self.engine.begin() as conn:
            conn.execute(text(insert_query), kwargs)
        self._keep_last_365_days(table_name)
        print(f"Inserted row into {table_name} and trimmed old rows.")



    def insert_df_rows(self, table_name, df, bulk=False, chunk_size=1000):
        table_name = table_name.lower()

        if df.empty:
            print("No rows to insert.")
            return

        if (bulk) or (len(df) > 100):
            print(f"Bulk inserting {len(df)} rows into {table_name}...")
            temp_table = f"{table_name}_staging"

            # Stage data
            df[required_cols].to_sql(
                temp_table,
                con=self.engine,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=chunk_size
            )

            # Insert missing rows
            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(required_cols)})
                SELECT {", ".join("s."+c for c in required_cols)}
                FROM {temp_table} s
                LEFT JOIN {table_name} t
                ON s.open_time = t.open_time
                WHERE t.open_time IS NULL
            """
            with self.engine.begin() as conn:
                conn.execute(text(insert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

            print(f"Bulk inserted {len(df)} rows into {table_name}.")
        else:

            # filter only required columns
            df_filtered = df[[c for c in required_cols if c in df.columns]]

            if df_filtered.empty:
                print("No rows to insert.")
                return

            # prepare query
            columns = ", ".join(df_filtered.columns)
            placeholders = ", ".join([f":{col}" for col in df_filtered.columns])
            insert_query = text(f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (open_time) DO NOTHING
            """)

            total_rows = len(df_filtered)
            inserted_count = 0

            with self.engine.begin() as conn:
                for start in tqdm(range(0, total_rows, chunk_size)):
                    end = start + chunk_size
                    chunk = df_filtered.iloc[start:end]
                    rows = chunk.to_dict(orient="records")
                    conn.execute(insert_query, rows)
                    inserted_count += len(rows)

            # cleanup old rows
            self._keep_last_365_days(table_name)

            print(f"Inserted {inserted_count} rows into {table_name} in chunks of {chunk_size}.")


    def shift_predictions(self, table_name, model, from_version, to_version):
        table_name = table_name.lower()
        with self.engine.begin() as conn:
            shift_sql = text(f"""
                UPDATE {table_name}
                SET {model}_{to_version} = {model}_{from_version},
                    {model}_{from_version} = NULL;
            """)
            conn.execute(shift_sql)

    def bulk_update_predictions(self, table_name, model, version, pred_df):
        """
        Bulk update predictions for a given model/version into an existing table.
        Keeps only rows where open_time >= earliest open_time in the DB.

        Args:
            table_name (str): Target table name.
            model (str): Model name (e.g., 'trl', 'tst', 'lightgbm').
            version (int): Version number (e.g., 1, 2, 3).
            pred_df (pd.DataFrame): DataFrame with ['open_time', 'pred'].
        """
        if not {"open_time", "pred"}.issubset(pred_df.columns):
            raise ValueError("pred_df must contain 'open_time' and 'pred' columns")

        table_name = table_name.lower()
        model = model.lower()
        col_name = f"{model}_{version}"  # real column in target table
        temp_table = f"{table_name}_tmp_preds"

        # Ensure open_time is datetime
        pred_df["open_time"] = pd.to_datetime(pred_df["open_time"])

        # Step 0: find earliest timestamp in DB
        with self.engine.begin() as conn:
            result = conn.execute(text(f"SELECT MIN(open_time) FROM {table_name}"))
            first_time_in_db = result.scalar()

        print(f"Earliest timestamp in {table_name}: {first_time_in_db}")
        print(f"Earliest timestamp in input DF: {pred_df['open_time'].min()}")

        if first_time_in_db is None:
            print(f"No rows exist in {table_name}, skipping update.")
            return

        # Step 1: slice input DF so only rows >= earliest in DB
        pred_df = pred_df[pred_df["open_time"] >= first_time_in_db].copy()
        if pred_df.empty:
            print("No new rows to update after slicing by earliest DB timestamp.")
            return

        print(f"Updating {len(pred_df)} rows in {table_name}.{col_name}")

        # Step 2: create temporary table with column `pred`
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            conn.execute(
                text(f"CREATE TEMP TABLE {temp_table} (open_time TIMESTAMP PRIMARY KEY, pred FLOAT[])")
            )

        # Step 3: normalize predictions → plain Python lists
        import numpy as np
        pred_df["pred"] = pred_df["pred"].apply(normalize_pred)

        # Step 4: bulk insert
        self.bulk_insert_df(temp_table, pred_df)

        # Step 5: update real table
        with self.engine.begin() as conn:
            update_sql = text(f"""
                UPDATE {table_name} t
                SET {col_name} = tmp.pred
                FROM {temp_table} tmp
                WHERE t.open_time = tmp.open_time;
                DROP TABLE IF EXISTS {temp_table};
            """)
            conn.execute(update_sql)

        print(f"Successfully updated {len(pred_df)} rows in {table_name}.{col_name}")

    def get_total_pred_count(self, table_name, model, version):
        table_name = table_name.lower()
        model = model.lower()
        col_name = model if version is None else f"{model}_{version}"
        query = text(f"""
            SELECT COUNT({col_name}) AS total_count
            FROM {table_name}
            WHERE {col_name} IS NOT NULL
        """)
        with self.engine.begin() as conn:
            result = conn.execute(query).fetchone()
        return result.total_count if result else 0

    def get_first_missing_date(self, table_name, model, version):
        table_name = table_name.lower()
        model = model.lower()
        col_name = model if version is None else f"{model}_{version}"
        query = text(f"""
            SELECT MIN(open_time) AS first_missing
            FROM {table_name}
            WHERE {col_name} IS NULL
        """)
        with self.engine.begin() as conn:
            result = conn.execute(query).fetchone()
        return result.first_missing if result and result.first_missing else None

    def get_last_date(self, table_name):
        table_name = table_name.lower()
        query = f"SELECT MAX(open_time) as last_date FROM {table_name};"
        with self.engine.begin() as conn:
            result = conn.execute(text(query)).fetchone()
        return result.last_date if result else None

    def get_missing_prediction_times(self, table_name, model, version):
        table_name = table_name.lower()
        model = model.lower()
        col_name = model if version is None else f"{model}_{version}"
        query = text(f"""
            SELECT open_time 
            FROM {table_name}
            WHERE {col_name} IS NULL
            ORDER BY open_time ASC
        """)
        with self.engine.begin() as conn:
            result = conn.execute(query).fetchall()
        return [row[0] for row in result]

    def upsert_predictions(self, table_name, model, version, open_times, predictions, original_df, threshold=100):
        """
        Upsert predictions into the given table.
        - If rows <= threshold → row-by-row update, fallback insert.
        - If rows > threshold → bulk update first, then insert missing rows.
        """
        if len(predictions)==0:
            print("No predictions to upsert.")
            return
        if len(open_times) != len(predictions):
            raise ValueError("Number of predictions must match number of open_times")

        table_name = table_name.lower()
        model = model.lower()
        col_name = f"{model}_{version}"

        # If large batch → use bulk update pipeline
        if len(open_times) > threshold:
            pred_df = pd.DataFrame({
                "open_time": open_times,
                "pred": predictions
            })

            # 🔹 Drop nulls before bulk update
            before = len(pred_df)
            pred_df = pred_df.dropna(subset=["pred"])
            after = len(pred_df)
            print(f"Bulk update skipped {before - after} null predictions, proceeding with {after} updates.")

            if not pred_df.empty:
                self.bulk_update_predictions(table_name, model, version, pred_df)

            # 🔹 Now handle inserts for open_times that are missing in DB
            check_query = text(f"SELECT open_time FROM {table_name} WHERE open_time = ANY(:times)")

            # normalize datetime values
            times_param = open_times.tolist()


            with self.engine.begin() as conn:
                existing_times = {r[0] for r in conn.execute(check_query, {"times": times_param})}

            missing_times = list(set(open_times) - existing_times)

            if missing_times:
                print(f"Inserting {len(missing_times)} missing rows...")
                insert_df = original_df[original_df["open_time"].isin(missing_times)].copy()
                pred_map = dict(zip(open_times, predictions))
                insert_df = insert_df[required_cols]  # ensure only required columns
                insert_df[col_name] = insert_df["open_time"].map(pred_map)
                print("Columns in insert_df:", insert_df.columns.tolist())
                self.bulk_insert_df(table_name, insert_df)

            print(f"Upserted {len(open_times)} predictions into {table_name}.{col_name} (bulk + insert).")
            return

        # -----------------------
        # Small batch path (<= threshold)
        # -----------------------
        update_query = text(f"""
            UPDATE {table_name}
            SET {col_name} = :prediction
            WHERE open_time = :open_time
        """)

        rows = [{"open_time": ot, "prediction": preds} for ot, preds in zip(open_times, predictions)]
        with self.engine.begin() as conn:
            conn.execute(update_query, rows)

    # Check which rows are missing → insert them
     # 🔹 Now handle inserts for open_times that are missing in DB
        check_query = text(f"SELECT open_time FROM {table_name} WHERE open_time = ANY(:times)")
        
        # normalize datetime values
        # if isinstance(open_times, (pd.Series, pd.DatetimeIndex)):
        #     times_param = open_times.to_pydatetime().tolist()
        # else:
        #     times_param = list(open_times)
        times_param = open_times.tolist()
        
        
        with self.engine.begin() as conn:
            existing_times = {r[0] for r in conn.execute(check_query, {"times": times_param})}
        
        missing_times = list(set(open_times) - existing_times)

        if missing_times:
            insert_df = original_df[original_df["open_time"].isin(missing_times)].copy()
            pred_map = dict(zip(open_times, predictions))
            insert_df[col_name] = insert_df["open_time"].map(pred_map)
            self.bulk_insert_df(table_name, insert_df)

        print(f"Upserted {len(open_times)} predictions into {table_name}.{col_name} (row-wise).")


    def _keep_last_365_days(self, table_name, time='open_time'):
        latest_query = f"SELECT MAX({time}) AS latest FROM {table_name}"
        with self.engine.begin() as conn:
            latest_ts = conn.execute(text(latest_query)).scalar()
        if latest_ts is None:
            return
        cutoff_date = latest_ts - timedelta(days=365)
        delete_query = f"DELETE FROM {table_name} WHERE {time} < :cutoff"
        with self.engine.begin() as conn:
            conn.execute(text(delete_query), {"cutoff": cutoff_date})


# Usage example
db_url = os.getenv("DATABASE_URL")
coins = ["BTCUSDT"]
print("Connecting to database at ", db_url[:20] + "****...")
engine = create_engine(
    db_url,
    pool_size=3,
    max_overflow=0,
    pool_pre_ping=True,
)
print("----------------Connected to database at ", db_url[:20] + "****...")
crypto_db = CryptoDB(engine, coins)
