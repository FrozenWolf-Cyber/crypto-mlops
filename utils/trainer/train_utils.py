import pandas as pd
import numpy as np
import ta
from ..artifact_control.s3_manager import S3Manager

def preprocess_crypto(df, horizon=1, threshold=0.001, balanced=False):
    """
    Preprocess 1-min crypto OHLCV data for classification.
    
    Parameters:
        df : pd.DataFrame
            Must contain columns: open_time, open, high, low, close, volume,
            close_time, quote_asset_volume, trades, taker_base, taker_quote, aggregated_new_sentiment_24hrs
        horizon : int
            How many minutes ahead to predict
        threshold : float
            Threshold for Buy/Sell classification
        
    Returns:
        X : pd.DataFrame
            Feature matrix
        y : pd.Series
            Labels (0=Sell, 1=Hold, 2=Buy)
    """
    
    df = df.copy()
    
    # Drop redundant columns
    if "close_time" in df.columns:
        df = df.drop(columns=["close_time"])
    
    # --- Price features ---
    df["return"] = df["close"].pct_change()
    df["log_return"] = np.log1p(df["return"])
    df["high_low_range"] = (df["high"] - df["low"]) / df["open"]
    df["close_open_range"] = (df["close"] - df["open"]) / df["open"]
    df["rolling_volatility"] = df["log_return"].rolling(30).std()
    
    # Rolling statistics
    df["rolling_mean_5"] = df["close"].rolling(5).mean()
    df["rolling_mean_15"] = df["close"].rolling(15).mean()
    df["rolling_mean_30"] = df["close"].rolling(30).mean()
    df["rolling_std_15"] = df["close"].rolling(15).std()
    
    # --- Technical indicators ---
    df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()
    macd = ta.trend.MACD(df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()
    bb = ta.volatility.BollingerBands(df["close"])
    df["bb_high"] = bb.bollinger_hband()
    df["bb_low"] = bb.bollinger_lband()
    df["bb_percent"] = bb.bollinger_pband()
    
    # --- Microstructure features ---
    for col in ["volume", "trades", "taker_base", "taker_quote", "quote_asset_volume"]:
        if col in df.columns:
            df[f"log_{col}"] = np.log1p(df[col])
    
    # --- Lag features ---
    for lag in [1, 3, 5, 15]:
        df[f"lag_return_{lag}"] = df["log_return"].shift(lag)
        df[f"lag_volume_{lag}"] = df["log_volume"].shift(lag)
    
    # --- Label ---
    df["future_return"] = df["close"].shift(-horizon) / df["close"] - 1
    
    def label_func(x):
        if x > threshold:
            return 2  # Buy
        elif x < -threshold:
            return 0  # Sell
        else:
            return 1  # Hold
    
    df["label"] = df["future_return"].apply(label_func)
    
    if balanced:
        min_size = df["label"].value_counts().min()

        # Sample equally from each class
        df_balanced = (
            df.groupby("label", group_keys=False)
              .apply(lambda x: x.sample(min_size, random_state=42))
        )

        # Shuffle
        df_balanced = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)
        df = df_balanced
    
    # --- Cleanup ---
    
    ohlcv_cols = ["open", "high", "low", "close", "volume"]
    df[ohlcv_cols] = df[ohlcv_cols].ffill().bfill()

    # 2. Fill engineered features with 0 (neutral signal)
    df = df.fillna(0)

    
    # print("NaN counts by column:")
    # print(df.isna().sum()[df.isna().sum() > 0])
    df = df.dropna().reset_index(drop=True)
    
    # Features / target
    X = df.drop(columns=["open_time", "label", "future_return"])
    y = df["label"]
    
    return X, y

def annotate_news(df_prices, df_news, window_hours=12, threshold=0.005):
    import numpy as np
    import pandas as pd

    df_prices['open_time'] = pd.to_datetime(df_prices['open_time'], utc=True, format='mixed')
    df_news['date'] = pd.to_datetime(df_news['date'], utc=True, format="mixed")

    # sort news by time
    df_news = df_news.sort_values('date').reset_index(drop=True)

    annotations, price_change_l = [], []

    for idx, row in df_news.iterrows():
        news_time = row['date']
        window_end = news_time + pd.Timedelta(hours=window_hours)

        # price just before the news
        price_before = df_prices[df_prices['open_time'] <= news_time]['close']
        if price_before.empty:
            annotations.append('Neutral')
            price_change_l.append(0.0)
            continue
        open_price = price_before.iloc[-1]

        # prices after news within window
        price_after = df_prices[
            (df_prices['open_time'] > news_time) &
            (df_prices['open_time'] <= window_end)
        ]['close']

        if price_after.empty:
            close_price = df_prices['close'].iloc[-1]
        else:
            close_price = price_after.mean()   # average return over window

        # compute % change
        price_change = (close_price - open_price) / open_price
        price_change_l.append(price_change)

        # annotation based on threshold
        if price_change > threshold:
            annotations.append('Positive')
        elif price_change < -threshold:
            annotations.append('Negative')
        else:
            annotations.append('Neutral')

    # ensure news text formatting
    df_news['text'] = df_news['text'].apply(lambda x: x if isinstance(x, (str, list)) else "")
    df_news['text'] = df_news['text'].apply(lambda x: '\n'.join(x) if isinstance(x, list) else x)
    df_news['news_text'] = df_news['title'] + ":\n" + df_news['text']

    # add labels
    df_news['annotation'] = annotations
    df_news['price_change'] = price_change_l
    df_news['label'] = df_news['annotation'].map({'Positive': 0, 'Negative': 1, 'Neutral': 2})

    return df_news

def generate_random_news(df_prices, num_news=50):
    """
    Generate a synthetic news DataFrame with random timestamps within price data range.
    
    Parameters:
        df_prices: DataFrame with OHLCV data, must have 'open_time'
        num_news: Number of news entries to generate
        
    Returns:
        df_news: DataFrame with 'date', 'news_text', 'sentiment_value'
    """
    
    # Ensure datetime format
    if not np.issubdtype(df_prices['open_time'].dtype, np.datetime64):
        df_prices['open_time'] = pd.to_datetime(df_prices['open_time'], format='%Y-%m-%d %H:%M:%S')
    
    start_time = df_prices['open_time'].min()
    end_time = df_prices['open_time'].max()

    # Generate random timestamps within the range
    random_timestamps = pd.to_datetime(
        np.random.randint(start_time.value//10**9, end_time.value//10**9, num_news),
        unit='s'
    )

    # Generate random text and sentiment placeholder
    news_texts = [f"News headline {i+1}" for i in range(num_news)]

    df_news = pd.DataFrame({
        'date': random_timestamps,
        'news_text': news_texts,
    }).sort_values('date').reset_index(drop=True)
    
    return df_news

def download_s3_dataset(coin, trl_model=False):
    s3_manager = S3Manager(
   )
    
    coins = ["BTCUSDT"] if trl_model else [coin]
    if trl_model:
        article_path = f"/opt/airflow/custom_persistent_shared/data/articles/articles.csv" 
        s3_manager.download_df(article_path, bucket='mlops', key=f'articles/articles.parquet')
        
    for coin in coins:
        price_test_path = f"/opt/airflow/custom_persistent_shared/data/prices/{coin}_test.csv"
        s3_manager.download_df(price_test_path, bucket='mlops', key=f'prices/{coin}_test.parquet')
    
        prices_path = f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv"
        s3_manager.download_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')
        
            
def convert_to_onnx(model, type="lightgbm", tokenizer=None, sample_input=None):
    """
    Convert a model to an ONNX object in memory.
    Returns an ONNX model object suitable for mlflow.onnx.log_model.
    """
    if type == "transformers":
        from transformers.onnx import FeaturesManager, export as onnx_export
        from pathlib import Path

        feature = "sequence-classification"
        model_kind, onnx_config_class = FeaturesManager.check_supported_model_or_raise(
            model, feature=feature
        )
        onnx_config = onnx_config_class(model.config)

        # Export to a temporary path (required by export)
        output_path = Path("temp_transformers.onnx")
        onnx_inputs, onnx_outputs = onnx_export(
            preprocessor=tokenizer,
            model=model,
            config=onnx_config,
            opset=17,
            output=output_path,
        )
        # Load the ONNX model object in memory
        import onnx
        onnx_model = onnx.load(str(output_path))
        return onnx_model

    elif type == "pytorch":
        import torch
        import io
        import onnx

        f = io.BytesIO()
        torch.onnx.export(
            model,
            sample_input,
            f,
            input_names=["input"],
            output_names=["output"],
            opset_version=14,
            dynamic_axes={"input": {0: "batch_size"}, "output": {0: "batch_size"}},
        )
        f.seek(0)
        onnx_model = onnx.load_model(f)
        return onnx_model

    elif type == "lightgbm":
        import onnxmltools
        from onnxmltools.convert.common.data_types import FloatTensorType
        onnx_model = onnxmltools.convert_lightgbm(model, name="lgbm_model", initial_types=[("float_input", FloatTensorType([None, sample_input.shape[1]]))])
        return onnx_model


def log_classification_metrics(y_pred, y_true, name="val", step=None, class_labels=None):
    from sklearn.metrics import classification_report
    import mlflow
    import wandb
    report = classification_report(y_true, y_pred, output_dict=True)

    if class_labels is None:
        class_labels = [k for k in report.keys() if k not in ["accuracy", "macro avg", "weighted avg"]]

    dict = {}
    for cls in class_labels:
        mlflow.log_metric(f"{name}_f1_class_{cls}", report[cls]["f1-score"], step=step)
        dict[f"{name}_f1_class_{cls}"] = report[cls]["f1-score"]
    mlflow.log_metric(f"{name}_f1_macro", report["macro avg"]["f1-score"], step=step)
    mlflow.log_metric(f"{name}_accuracy", report["accuracy"], step=step)
    dict[f"{name}_f1_macro"] = report["macro avg"]["f1-score"]
    wandb.log(dict)
    return report


def preprocess_sequences(df, seq_len=30, horizon=1, threshold=0.001, return_first=False, inference=False):
    """
    Convert raw OHLCV + sentiment features into sequences suitable for LSTM/Transformer.
    Returns X_seq (tensor) and y_seq (tensor).
    """
    # Raw features per timestep
    
    features = ["open", "high", "low", "close", "volume",
                "taker_base", "taker_quote"]
    
    X_raw = df[features].values.astype(float)
    
    # Label for classification: Buy(2), Hold(1), Sell(0)
    df["future_return"] = df["close"].shift(-horizon) / df["close"] - 1
    def label_func(x):
        if x > threshold:
            return 2  # Buy
        elif x < -threshold:
            return 0  # Sell
        else:
            return 1  # Hold
    y_raw = df["future_return"].apply(label_func).values.astype(int)
    
    # Build sequences
    X_seq, y_seq = [], []
    if return_first:
        for i in range(seq_len):
            temp = [X_raw[0].tolist()]*(seq_len-i)+X_raw[:i].tolist()
            X_seq.append(temp)
            y_seq.append(y_raw[i])
            
    for i in range(len(X_raw) - seq_len):
        X_seq.append(X_raw[i:i+seq_len])
        y_seq.append(y_raw[i+seq_len])
    
    if not inference:
        import torch
        X_seq = torch.tensor(X_seq, dtype=torch.float32)
        y_seq = torch.tensor(y_seq, dtype=torch.long)
    
    return X_seq, y_seq


def preprocess_common(model, df, seq_len=30, horizon=1, threshold=0.00015, return_first=True, inference=True):
    if model == "tst":
        X, _ = preprocess_sequences(df, seq_len=seq_len, horizon=horizon, threshold=threshold, return_first=return_first, inference=inference)
        X = X[-1].tolist()
        
    elif model == "lightgbm":
       X, _ =  preprocess_crypto(df, horizon=horizon, threshold=threshold, balanced=False)
       print(f"Preprocessed {len(X)} rows for LightGBM")
       X = X[-1:].values.tolist()[0]

    return X

def preprocess_common_batch(model, df, seq_len=30, horizon=1, threshold=0.00015, return_first=True, inference=True):
    if model == "tst":
        X, _ = preprocess_sequences(df, seq_len=seq_len, horizon=horizon, threshold=threshold, return_first=return_first, inference=inference)

        
    elif model == "lightgbm":
        print(df.columns)
        X, _ =  preprocess_crypto(df, horizon=horizon, threshold=threshold, balanced=False)
        print(f"Preprocessed {len(X)} rows for LightGBM")
        X = X.values.tolist()

    return X


import time, os
START_FILE = "train_start_time.txt"


def save_start_time(path=START_FILE):
    """Save the current time to a file."""
    with open(path, "w") as f:
        f.write(str(time.time()))


def load_start_time(path=START_FILE):
    """Load start time from file, or create it if missing."""
    if not os.path.exists(path):
        save_start_time(path)
    with open(path, "r") as f:
        return float(f.read().strip())