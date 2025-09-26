import os
import argparse
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset, random_split
from sklearn.metrics import classification_report
import mlflow
from tqdm import tqdm
from .train_utils import load_start_time, download_s3_dataset, convert_to_onnx, log_classification_metrics, preprocess_sequences
from ..artifact_control.model_manager import ModelManager
from ..artifact_control.s3_manager import S3Manager
from ..database.airflow_db import db
import wandb
import time
print("Starting TST training script...", flush=True)
# -------------------------
# TST Model
# -------------------------
class TSTBlock(nn.Module):
    def __init__(self, hidden_dim, num_heads, ff_dim, dropout=0.1):
        super().__init__()
        self.attn = nn.MultiheadAttention(hidden_dim, num_heads, batch_first=True)
        self.ff = nn.Sequential(
            nn.Linear(hidden_dim, ff_dim),
            nn.ReLU(),
            nn.Linear(ff_dim, hidden_dim)
        )
        self.norm1 = nn.LayerNorm(hidden_dim)
        self.norm2 = nn.LayerNorm(hidden_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        attn_out, _ = self.attn(x, x, x)
        x = self.norm1(x + self.dropout(attn_out))
        ff_out = self.ff(x)
        x = self.norm2(x + self.dropout(ff_out))
        return x

class TSTClassifier(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_heads, ff_dim, num_layers, num_classes):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        self.layers = nn.ModuleList([TSTBlock(hidden_dim, num_heads, ff_dim) for _ in range(num_layers)])
        self.classifier = nn.Linear(hidden_dim, num_classes)

    def forward(self, x):
        x = self.input_proj(x)
        for layer in self.layers:
            x = layer(x)
        out = self.classifier(x[:, -1, :])  # use last timestep
        return out

# -------------------------
# Training / Evaluation
# -------------------------
def evaluate_model(model, loader, device="cuda"):
    model.eval()
    preds, labels = [], []
    with torch.no_grad():
        for xb, yb in loader:
            xb, yb = xb.to(device), yb.to(device)
            out = model(xb)
            pred_class = out.argmax(dim=1)
            preds.extend(pred_class.cpu().numpy())
            labels.extend(yb.cpu().numpy())
    return labels, preds

def log_metrics(model, loader, name="val", step=None):
    labels, preds = evaluate_model(model, loader)
    report = log_classification_metrics(preds, labels, name=name, step=step)
    return report

def main(args):
    coin = args.coin
    thresh = args.threshold
    seq_len = args.seq_len
    batch_size = args.batch_size
    epochs = args.epochs
    hidden_dim = args.hidden_dim
    num_heads = args.num_heads
    ff_dim = args.ff_dim
    num_layers = args.num_layers
    lr = args.lr
    device = "cuda" if torch.cuda.is_available() else "cpu"

    download_s3_dataset(coin, trl_model=False)

    df_train = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv")
    df_train = df_train[-args.trainset_size:]
    X_seq, y_seq = preprocess_sequences(df_train, seq_len=seq_len, threshold=thresh)
    val_size = int(0.2 * len(X_seq))
    train_size = len(X_seq) - val_size
    train_dataset, val_dataset = random_split(TensorDataset(X_seq, y_seq), [train_size, val_size])

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
    mlflow.set_experiment(f"{coin.lower()}-tst")

    # Model
    mm = ModelManager(os.getenv("MLFLOW_URI"))
    model, metadata = mm.load_latest_model(f'{coin.lower()}_tst')
    if model is None:
        model = TSTClassifier(
        input_dim=X_seq.shape[2],
        hidden_dim=hidden_dim,
        num_heads=num_heads,
        ff_dim=ff_dim,
        num_layers=num_layers,
        num_classes=3
        )
        
    model = model.to(device)

    print("Trainable parameters:", sum(p.numel() for p in model.parameters() if p.requires_grad), flush=True)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)


    run = wandb.init(project='mlops', entity="frozenwolf", config=vars(args), notes=f"Training TST model on {coin}")
    with mlflow.start_run() as run:
        print("MLflow run ID:", run.info.run_id, flush=True)
        mlflow.log_params(vars(args))
        mlflow.log_param("num_params", sum(p.numel() for p in model.parameters() if p.requires_grad))
        db.set_state("tst", args.coin.upper(), "RUNNING")
        train_losses, val_losses = [], []

        start_time = load_start_time()
        for epoch in range(epochs):
            model.train()
            epoch_loss = 0
            if time.time() - start_time > args.max_time:
                print("Max time exceeded, stopping training.")
                break
            for xb, yb in tqdm(train_loader, desc=f"Epoch {epoch+1}/{epochs}"):
                if time.time() - start_time > args.max_time:
                    print("Max time exceeded, stopping training.")
                    break
                optimizer.zero_grad()
                xb, yb = xb.to(device), yb.to(device)
                out = model(xb)
                loss = criterion(out, yb)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
                wandb.log({"batch_loss": loss.item()})
            avg_train_loss = epoch_loss / len(train_loader)
            train_losses.append(avg_train_loss)
            mlflow.log_metric("train_loss", avg_train_loss, step=epoch)
            wandb.log({"train_loss": avg_train_loss})

            # Validation
            model.eval()
            val_loss = 0
            for xb, yb in val_loader:
                xb, yb = xb.to(device), yb.to(device)
                out = model(xb)
                loss = criterion(out, yb)
                val_loss += loss.item()
            avg_val_loss = val_loss / len(val_loader)
            val_losses.append(avg_val_loss)
            mlflow.log_metric("val_loss", avg_val_loss, step=epoch)

            # Log classification metrics
            log_metrics(model, val_loader, name="val", step=epoch)
            wandb.log({"val_loss": avg_val_loss, "epoch": epoch})
            
            print(f"Epoch {epoch+1}/{epochs} - Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}", flush=True)

        # -------------------------
        # Test evaluation
        # -------------------------
        X_test_seq, y_test_seq = preprocess_sequences(pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}_test.csv"), seq_len=seq_len, threshold=thresh)
        test_loader = DataLoader(TensorDataset(X_test_seq, y_test_seq), batch_size=batch_size)
        test_report = log_metrics(model, test_loader, name="test")

        # -------------------------
        # Save model
        # -------------------------
        df_train = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv")
        X_seq, y_seq = preprocess_sequences(df_train, seq_len=seq_len, threshold=thresh, return_first=True)
        dataloader = DataLoader(TensorDataset(X_seq, y_seq), batch_size=batch_size, shuffle=False)
        log_metrics(model, dataloader, name="past_perf")
        pred = []
        model.eval()
        with torch.no_grad():
            for xb, _ in dataloader:
                xb = xb.to(device)
                out = model(xb)
                pred+=out.cpu().numpy().tolist()
                
        df_train['pred'] = pred
        df_train = df_train[["open_time", "pred"]]
        s3 = S3Manager()
        s3.add_pred_s3(df_train, coin, 'tst')

        # Convert to ONNX
        model = model.to("cpu")
        onnx_model = convert_to_onnx(model, type="pytorch", sample_input=X_seq[:1])
        mm.save_model(type="tst", model=model, name=f"{coin.lower()}_tst", onnx_model=onnx_model)
        mm.enforce_retention(f"{coin.lower()}_tst", max_versions=5, delete_s3=True)
        mm.set_production(f"{coin.lower()}_tst", keep_latest_n=2)

        print("MLflow run completed:", run.info.run_id, flush=True)

    db.set_state("tst", args.coin.upper(), "SUCCESS")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train TST crypto model with MLflow logging")
    parser.add_argument("--coin", type=str, default="BTCUSDT")
    parser.add_argument("--threshold", type=float, default=0.00015)
    parser.add_argument("--max_time", type=int, default=60*20) ## seconds
    parser.add_argument("--seq_len", type=int, default=20)
    parser.add_argument("--batch_size", type=int, default=128)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--trainset_size", type=float, default=300000, help="Proportion of data to use for training")
    parser.add_argument("--hidden_dim", type=int, default=64)
    parser.add_argument("--num_heads", type=int, default=2)
    parser.add_argument("--ff_dim", type=int, default=128)
    parser.add_argument("--num_layers", type=int, default=2)
    parser.add_argument("--lr", type=float, default=1e-3)
    args = parser.parse_args()
    try:
        main(args)
    except Exception as e:
        db.set_state("tst", args.coin.upper(), "FAILED", error_message=str(e))
        raise e
