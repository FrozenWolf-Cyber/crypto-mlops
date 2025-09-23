import os
import argparse
import copy
import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader, ConcatDataset, random_split
from torch.optim import AdamW
from peft import PeftModel
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from peft import LoraConfig, get_peft_model
import mlflow
from .train_utils import annotate_news
from tqdm import tqdm
from ..artifact_control.model_manager import ModelManager
from .train_utils import load_start_time, download_s3_dataset, log_classification_metrics
from ..artifact_control.s3_manager import S3Manager
import time
import wandb

# ----------------------------
# Dataset wrapper
# ----------------------------
class NewsDataset(Dataset):
    def __init__(self, df):
        df['news_text'] = df['title'] + ":\n" + df['text']
        self.texts = df['news_text'].tolist()
        self.labels = df['label'].astype(int).tolist()
        self.price_changes = df['price_change'].astype(float).tolist()

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return {
            "text": self.texts[idx],
            "label": self.labels[idx],
            "price_change": self.price_changes[idx]
        }

# ----------------------------
# Utilities
# ----------------------------
def compute_normalizer(df, eps=1e-9):
    mu = np.mean(np.abs(df['price_change'].values))
    return max(mu, eps)

def log_probs_from_logits(logits, actions):
    logp_all = F.log_softmax(logits, dim=-1)
    return logp_all.gather(1, actions.unsqueeze(1)).squeeze(1)

def kl_divergence_from_logits(logits_p, logits_q):
    p = F.log_softmax(logits_p, dim=-1)
    q = F.log_softmax(logits_q, dim=-1)
    sp = torch.softmax(p, dim=-1)
    kl = torch.sum(sp * (p - q), dim=-1)
    return kl.mean()

@torch.no_grad()
def get_predictions(policy, tokenizer, dataloader, device):
    pred = []
    policy.eval()
    for batch in dataloader:
        texts = batch["text"]
        enc = tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(device)
        logits = policy(**enc).logits
        logits = logits.cpu().numpy()
        pred.extend(logits.tolist())
        
    return pred



# ----------------------------
# GRPO training loop
# ----------------------------

# ----------------------------
# Main script
# ----------------------------
from torch.utils.data import DataLoader
from ..database.airflow_db import db
# ----------------------------
# Main script (DataLoader version)
# ----------------------------
def main(args):
    download_s3_dataset("all", trl_model=True)
    run = wandb.init(project='mlops', entity="frozenwolf", config=vars(args), notes=f"Training TRL model on with GRPO")
    

    coin = args.coin
    device = "cuda" if torch.cuda.is_available() else "cpu"

    lora_config = LoraConfig(
    r=args.lora_rank,                    # rank of the low-rank matrices
    lora_alpha=32,           # scaling factor
    target_modules=["query", "key", "value"],  # layers to apply LoRA
    lora_dropout=0.1,
    bias="none",
    task_type="SEQ_CLS"      # sequence classification
    )

    # Load data and generate news
    coins = ["BTCUSDT"]
    ds_combined = []
    normalizer = 0
    df_combined = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/articles/articles.csv")
    df_combined = df_combined.drop_duplicates(subset=['link'])
    price_changes = []
    for coin in coins:
        df_prices = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv")
        df_news = annotate_news(df_prices, df_combined.copy(), window_hours=args.window_hours, threshold=args.threshold)
        price_changes.append(df_news['price_change'].tolist())
        # ds = NewsDataset(df_news)
        normalizer += compute_normalizer(df_news)
        
        # ds_combined.append(ds)  
        
    def get_label(price_change, threshold):
        if price_change > threshold:
            return 2
        elif price_change < -threshold:
            return 0
        else:
            return 1

        
    ### avg the price changes
    price_changes = np.array(price_changes)
    avg_price_changes = np.mean(price_changes, axis=0)
    df_combined['price_change'] = avg_price_changes
    df_combined['label'] = df_combined['price_change'].apply(lambda x: get_label(x, args.threshold))
    ### assign labels
    
    print(f"Total rows in df_combined before deduplication: {len(df_combined)}", flush=True)
    print(df_combined['label'].value_counts())
    
    ds = NewsDataset(df_combined)
    ### shuffle and create validation split
    val_frac = 0.1  # 10% for validation
    n_total = len(ds)
    n_val = int(n_total * val_frac)
    n_train = n_total - n_val
    
    train_dataset, val_dataset = random_split(ds, [n_train, n_val])

    
    print(f"Total combined dataset size: {len(ds)}", flush=True)
    dataloader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    print(f"DataLoader created with {len(dataloader)} batches of size {args.batch_size}", flush=True)
        
    normalizer /= len(coins)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
    mlflow.set_experiment(f"grpo-finbert")

    mm = ModelManager(os.getenv("MLFLOW_URI"))
    policy, latest_version = mm.load_latest_model(f'trl', model_type="trl")
    db.set_state("trl", "ALL", "RUNNING")
    with mlflow.start_run() as run:
        mlflow.log_params(vars(args))

        if not policy:
            policy = AutoModelForSequenceClassification.from_pretrained(args.model_name, num_labels=3)
            tokenizer = AutoTokenizer.from_pretrained(args.model_name)
            policy = get_peft_model(policy, lora_config)
        else:
            (policy, tokenizer) = policy
            
        policy.to(device)
        policy.enable_adapter_layers()
        policy.print_trainable_parameters()
        print(f"Number of trainable parameters: {sum(p.numel() for p in policy.parameters() if p.requires_grad)}", flush=True)
        
        reference = AutoModelForSequenceClassification.from_pretrained(args.model_name, num_labels=3).to(device)
        for p in reference.parameters():
            p.requires_grad = False
        reference.eval()

        theta_old = copy.deepcopy(policy).to(device)
        for p in theta_old.parameters():
            p.requires_grad = False
        theta_old.eval()

        optimizer = AdamW(policy.parameters(), lr=args.lr)
        epoch = 0
        start_time = load_start_time()
        for epoch in tqdm(range(1, args.epochs + 1)):
            
            val_preds = get_predictions(policy, tokenizer, DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False), device)
            val_labels = [item['label'] for item in val_dataset]
            val_preds = np.array(val_preds).argmax(axis=1).tolist()
            log_classification_metrics(val_labels, val_preds, 'val', epoch)
            
            epoch_loss = 0
            epoch_surrogate = 0
            epoch_kl = 0
            loss_accum = 0
            iters=0
            optimizer.zero_grad()
            for batch_idx, batch_items in tqdm(enumerate(dataloader), total=len(dataloader), desc=f"Epoch {epoch}/{args.epochs}"):
                iters += 1
                texts = batch_items["text"]
                true_labels = batch_items["label"].to(device)
                price_changes = batch_items["price_change"].numpy()

                # Encode with old policy (theta_old)
                with torch.no_grad():
                    enc_old = tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(device)
                    logits_old = theta_old(**enc_old).logits

                all_texts, all_actions, all_old_logps, all_rewards, all_group_ids = [], [], [], [], []
                probs_old = F.softmax(logits_old, dim=-1)

                for i in range(len(texts)):
                    dist = torch.distributions.Categorical(probs_old[i])
                    sampled_actions = dist.sample((args.group_size,))
                    logp_all = F.log_softmax(logits_old[i], dim=-1)
                    sampled_logps = logp_all[sampled_actions]

                    pc = price_changes[i]
                    for g in range(args.group_size):
                        action = int(sampled_actions[g].item())
                        old_logp = sampled_logps[g]
                        reward_mag = abs(pc) / normalizer
                        if args.reward_clip:
                            reward_mag = np.clip(reward_mag, -args.reward_clip, args.reward_clip)
                        reward = reward_mag if action == true_labels[i].item() else -reward_mag

                        all_texts.append(texts[i])
                        all_actions.append(action)
                        all_old_logps.append(old_logp)
                        all_rewards.append(float(reward))
                        all_group_ids.append(i)

                # Compute new logits and log_probs
                enc_new = tokenizer(all_texts, padding=True, truncation=True, return_tensors="pt").to(device)
                logits_new = policy(**enc_new).logits
                actions_tensor = torch.tensor(all_actions, dtype=torch.long, device=device)
                new_logps = log_probs_from_logits(logits_new, actions_tensor)
                old_logps_tensor = torch.stack(all_old_logps).to(device)

                rewards_np = np.array(all_rewards)
                group_ids = np.array(all_group_ids)
                adv = np.zeros_like(rewards_np)
                for gid in np.unique(group_ids):
                    mask = group_ids == gid
                    adv[mask] = rewards_np[mask] - rewards_np[mask].mean()
                advantages = torch.tensor(adv, dtype=torch.float32, device=device)

                logratio = new_logps - old_logps_tensor
                ratio = torch.exp(logratio)
                unclipped = ratio * advantages
                clipped = torch.clamp(ratio, 1.0 - args.clip_eps, 1.0 + args.clip_eps) * advantages
                surrogate = -torch.min(unclipped, clipped)
                loss_surrogate = surrogate.mean()

                kl_term = torch.tensor(0.0, device=device)
                if args.kl_coef > 0.0:
                    with torch.no_grad():
                        enc_ref = tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(device)
                        logits_ref = reference(**enc_ref).logits
                    logits_pol = policy(**enc_ref).logits
                    kl_term = kl_divergence_from_logits(logits_pol, logits_ref)

                loss = loss_surrogate + args.kl_coef * kl_term
                
                epoch_loss += loss.item()
                epoch_surrogate += loss_surrogate.item()
                epoch_kl += kl_term.item()
                
                (loss / args.grad_accum_steps).backward()
                if batch_idx % args.grad_accum_steps == 0:
                    optimizer.step()
                    optimizer.zero_grad()
                    
                    if args.update_old_every_iter:
                        theta_old.load_state_dict(policy.state_dict())
                        
                if time.time() - start_time > args.max_time:
                    print(f"Reached time limit of {args.max_time} seconds, stopping training.")
                    break
                
                mlflow.log_metric("epoch_loss", loss.item(), step=iters)
                mlflow.log_metric("epoch_surrogate", loss_surrogate.item(), step=iters)
                mlflow.log_metric("epoch_kl", kl_term.item() , step=iters)
                wandb.log({"loss": loss.item(), "surrogate": loss_surrogate.item(), "kl": kl_term.item()})
                        
            if batch_idx % args.grad_accum_steps != 0:
                optimizer.step()
                optimizer.zero_grad()


            mlflow.log_metric("epoch_loss", epoch_loss / len(dataloader), step=epoch)
            mlflow.log_metric("epoch_surrogate", epoch_surrogate / len(dataloader), step=epoch)
            mlflow.log_metric("epoch_kl", epoch_kl / len(dataloader), step=epoch)
            print(f"Epoch {epoch}: Loss={epoch_loss/len(dataloader):.4f}, Surrogate={epoch_surrogate/len(dataloader):.4f}, KL={epoch_kl/len(dataloader):.4f}", flush=True)
            wandb.log({"epoch_loss": epoch_loss / len(dataloader), "epoch_surrogate": epoch_surrogate / len(dataloader), "epoch_kl": epoch_kl / len(dataloader), "epoch": epoch})

            if time.time() - start_time > args.max_time:
                print(f"Reached time limit of {args.max_time} seconds, stopping training.")
                break

        val_preds = get_predictions(policy, tokenizer, DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False), device)
        val_labels = [item['label'] for item in val_dataset]
        val_preds = np.array(val_preds).argmax(axis=1).tolist()
        log_classification_metrics(val_labels, val_preds, 'val', epoch)
            

        ds = NewsDataset(df_combined)
        dataloader = DataLoader(ds, batch_size=args.batch_size, shuffle=False)
        all_preds = get_predictions(policy, tokenizer, dataloader, device)
        all_labels = [item['label'] for item in ds]
        log_classification_metrics(all_labels, np.array(all_preds).argmax(axis=1).tolist(), 'past_perf', args.epochs)
        df_combined['pred'] = all_preds
        s3 = S3Manager()
        s3.add_pred_s3(df_combined, "preds", 'trl')



        mm.save_model(type="trl", model=policy, name=f"trl", tokenizer=tokenizer)
        mm.enforce_retention(f"trl", max_versions=5, delete_s3=True)
        mm.set_production(f"trl", keep_latest_n=2)

        
        print("Training completed. MLflow run ID:", run.info.run_id)
        os.system("rm -rf trl_lora_weights") ## DEBUG
        
    

    db.set_state("trl", "ALL", "SUCCESS")
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train GRPO FinBERT model with MLflow")
    parser.add_argument("--coin", type=str, default="BTCUSDT")
    parser.add_argument("--lora_rank", type=int, default=4)
    parser.add_argument("--num_news", type=int, default=50)
    parser.add_argument("--window_hours", type=int, default=12)
    parser.add_argument("--threshold", type=float, default=0.005)
    parser.add_argument("--model_name", type=str, default="ProsusAI/finbert")
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch_size", type=int, default=4)
    parser.add_argument("--grad_accum_steps", type=int, default=1)
    parser.add_argument("--group_size", type=int, default=4)
    parser.add_argument("--clip_eps", type=float, default=0.2)
    parser.add_argument("--kl_coef", type=float, default=0.1)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--reward_clip", type=float, default=None)
    parser.add_argument("--update_old_every_iter", type=bool, default=True)
    parser.add_argument("--max_time", type=int, default=60*20) ## seconds
    args = parser.parse_args()
    try:
        main(args)
    except Exception as e:
        db.set_state("trl", args.coin.upper(), "FAILED", error_message=str(e))
        raise e