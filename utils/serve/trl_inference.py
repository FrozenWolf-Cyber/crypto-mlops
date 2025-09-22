import argparse
import json
import pandas as pd
from ..database.db import crypto_db
from tokenizers import Tokenizer
import numpy as np
import onnxruntime as ort
from tqdm import tqdm
from ..trainer.train_utils import annotate_news


def get_label(price_change, threshold):
    if price_change > threshold:
        return 2
    elif price_change < -threshold:
        return 0
    else:
        return 1


def run_finbert_on_batch(texts, tokenizer_path="finbert_tokenizer/tokenizer.json",
                         model_path="finbert.onnx", max_len=512, batch_size=4):
    """
    Run FinBERT ONNX inference on a list of texts in batches with progress bar.
    """
    tokenizer = Tokenizer.from_file(tokenizer_path)
    session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
    
    all_logits = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Processing batches"):
        batch_texts = texts[i:i+batch_size]
        encoded_batch = [tokenizer.encode(t) for t in batch_texts]

        input_ids = np.array(
            [e.ids[:max_len] + [0]*(max_len - len(e.ids[:max_len])) for e in encoded_batch], 
            dtype=np.int64
        )
        attention_mask = np.array(
            [e.attention_mask[:max_len] + [0]*(max_len - len(e.attention_mask[:max_len])) for e in encoded_batch], 
            dtype=np.int64
        )
        token_type_ids = np.zeros_like(input_ids, dtype=np.int64)

        inputs = {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "token_type_ids": token_type_ids
        }

        logits = session.run(None, inputs)[0]
        all_logits.append(logits)

    return np.vstack(all_logits)


def main(args):
    with open(args.config_path, "r") as f:
        config = json.load(f)

    print("Loaded config:", config)

    coins = ["BTCUSDT"]

    df_combined = pd.read_csv(args.articles_path)
    
    df_combined['date'] = pd.to_datetime(df_combined['date'], utc=True, errors="coerce", format="mixed")
    df_combined = df_combined.drop_duplicates(subset=['link'])

    print(f"Total rows in df_combined: {len(df_combined)}")

    for version, model_path in config.items():
        print(f"\nProcessing version: {version}, model path: {model_path}")
        tokenizer = model_path.replace("finbert_", "finbert_tokenizer_").replace(".onnx", "/tokenizer.json")
        print("Tokenizer path:", tokenizer)

        session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])

        pred = pd.read_csv(f"{args.pred_dir}/v{version}.csv")
        pred['date'] = pd.to_datetime(pred['date']) 
        pred = pred.drop_duplicates(subset=['link'])
        
        new_rows = df_combined[~df_combined['link'].isin(pred['link'])]
        print(f"Rows already predicted: {len(pred)}")
        print(f"Rows to run inference on: {len(new_rows)}")
        
        if not new_rows.empty:
            texts = new_rows['text'].tolist()
            logits = run_finbert_on_batch(
                texts, 
                tokenizer_path=tokenizer, 
                model_path=model_path, 
                max_len=args.max_len,
                batch_size=args.batch_size
            )
            
            new_preds = new_rows.copy()
            new_preds['pred'] = logits.tolist()
            new_preds['price_change'] = None
            new_preds['label'] = None
            print(f"Rows newly inferred: {len(new_preds)}")
            
            
            crypto_db.upsert_trl_full(new_preds.copy(), version=int(version))
            
            combined_preds = pd.concat([pred, new_preds], ignore_index=True)

        else:
            combined_preds = pred.copy()
            print("No new rows to infer.")
        
        print(f"Total rows in final predictions: {len(combined_preds)}")
        
        latest_date = combined_preds['date'].max()
        cutoff_date = latest_date - pd.Timedelta(hours=args.window_hours)
        recent_preds = combined_preds[combined_preds['date'] >= cutoff_date].copy()
        
        print(f"Latest date in predictions: {latest_date}")
        print(f"Cutoff date for recent predictions: {cutoff_date}")
        print(f"Rows in recent predictions before adding new: {len(recent_preds)}")
        
        if not new_rows.empty:
            recent_preds = pd.concat([recent_preds, new_preds], ignore_index=True).drop_duplicates(subset=['link'])
            
        print(f"Rows in recent predictions (last {args.window_hours} hours): {len(recent_preds)}")
        
        
        price_changes = []
        for coin in coins:
            df_prices = pd.read_csv(f"{args.prices_dir}/{coin}.csv")
            df_news = annotate_news(
                df_prices, recent_preds.copy(), 
                window_hours=args.window_hours, 
                threshold=args.threshold
            )
            price_changes.append(df_news['price_change'].tolist())
            
        price_changes = np.array(price_changes)
        avg_price_changes = np.mean(price_changes, axis=0)
        recent_preds['price_change'] = avg_price_changes
        recent_preds['label'] = recent_preds['price_change'].apply(lambda x: get_label(x, args.threshold))
        
        recent_preds['date'] = pd.to_datetime(recent_preds['date'])
        
        ### push this  new labels, price_changes to db
        print("Updating recent predictions in DB...")
        crypto_db.upsert_trl_full(recent_preds.copy(), version=int(version))
        
        combined_preds.update(recent_preds)
        
        
        out_path = f"{args.pred_dir}/v{version}.csv"
        
        
        combined_preds.to_csv(out_path, index=False)
        
        print(f"Saved updated predictions for version {version} at {out_path}")


# if __name__ == "__main__":
parser = argparse.ArgumentParser(description="Run FinBERT TRL inference pipeline")
parser.add_argument("--config_path", type=str, default="/opt/airflow/custom_persistent_shared/trl_onnx_models/trl_onnx_config.json", help="Path to model config JSON")
parser.add_argument("--articles_path", type=str, default="/opt/airflow/custom_persistent_shared/data/articles/articles.csv", help="Path to articles CSV")
parser.add_argument("--pred_dir", type=str, default="/opt/airflow/custom_persistent_shared/data/predictions/preds/trl", help="Directory for saving predictions")
parser.add_argument("--prices_dir", type=str, default="/opt/airflow/custom_persistent_shared/data/prices", help="Directory with price CSVs")
parser.add_argument("--threshold", type=float, default=0.005, help="Price change threshold for labeling")
parser.add_argument("--window_hours", type=int, default=12, help="Lookback window in hours for price changes")
parser.add_argument("--max_len", type=int, default=512, help="Maximum token sequence length")
parser.add_argument("--batch_size", type=int, default=4, help="Batch size for inference")
args = parser.parse_args()
main(args)
