import boto3
import pandas as pd
from io import BytesIO
import hashlib
import os
from pathlib import Path
from urllib.parse import urlparse

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

### PRINT CURRENT PATH AND FROM WHERE IT IS RUNNING
print(f"Current working directory: {os.getcwd()}")
print(f"Script directory: {os.path.dirname(os.path.abspath(__file__))}")

create_dir("data/predictions")
create_dir("data/prices")
create_dir("data/articles")


class S3Manager:
    def __init__(self):
        self.s3 = boto3.client(
    service_name="s3",
     endpoint_url=os.getenv("S3_URL"),
     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
     region_name="apac"
        )

    # -------------------
    # Hash helpers
    # -------------------
    @staticmethod
    def compute_hash_bytes(data_bytes: bytes) -> str:
        return hashlib.sha256(data_bytes).hexdigest()

    def _get_s3_hash(self, bucket: str, hash_key: str) -> str:
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=hash_key)
            return obj['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Hash file {hash_key} not found in bucket {bucket}: {e}")
            return None


    def _delete_s3_prefix(self, s3_uri: str):
        parsed = urlparse(s3_uri, allow_fragments=False)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
    
        print(f"Deleting from bucket={bucket}, prefix={prefix}")
    
        paginator = self.s3.get_paginator("list_objects_v2")
    
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue
                
                objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
                while objects_to_delete:
                    batch = objects_to_delete[:1000]
                    del objects_to_delete[:1000]
    
                    self.s3.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": batch, "Quiet": True},
                    )
        except Exception as e:
            print(f"Error deleting prefix {prefix} from {bucket}: {e}")
    
    # -------------------
    # DataFrame upload/download with hashing
    # -------------------
    def upload_df(self, path, bucket: str, key: str):
        if isinstance(path, pd.DataFrame):
            df = path
        else:
            df = pd.read_csv(path)
        buffer = BytesIO()

        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        data_bytes = buffer.getvalue()
        file_hash = self.compute_hash_bytes(data_bytes)
        print(f"Computed hash for {key}: {file_hash}")
        hash_key = key.replace('.parquet', '.hash')
        existing_hash = self._get_s3_hash(bucket, hash_key)
        if existing_hash == file_hash:
            print(f"Skipping upload for {key}, hash matches S3.")
            return False
        
        # Upload file and hash
        self.s3.put_object(Bucket=bucket, Key=key, Body=data_bytes)
        self.s3.put_object(Bucket=bucket, Key=hash_key, Body=file_hash.encode('utf-8'))
        print(f"Uploaded {key} with hash {file_hash} to S3.")
        return True


    def get_existing_versions(self, coin, model_name):
        try:
            existing_versions = self.list_objects(bucket='mlops', prefix=f'predictions/{coin}/{model_name}')
        except Exception as e:
            print(f"Error listing existing versions: {e}")
            existing_versions = []
            
        existing_versions = [v for v in existing_versions if v.endswith('.parquet')]
        existing_versions = sorted(existing_versions, key=lambda x: int(x.split('/')[-1].split('.')[0][1:]))
        print(f"Existing versions for {coin}: {existing_versions}")
        return existing_versions
    
    def download_available_predictions(self, coin, model_name):
        existing_versions = self.get_existing_versions(coin, model_name)
        create_dir(f"data/predictions/{coin}")
        create_dir(f"data/predictions/{coin}/{model_name}")
        
        print(f"Existing versions for {coin}: {existing_versions}")
        for v in existing_versions:
            local_file = f"data/predictions/{coin}/{model_name}/v{v.split('/')[-1].split('.')[0][1:]}.csv"
            if not os.path.exists(local_file):
                self.download_df(local_file=local_file, bucket='mlops', key=v)
                print(f"Downloaded {v} to {local_file}")
            else:
                print(f"Local file {local_file} already exists. Skipping download.")
                


    def update_local_pred(self, coin, model_name, upload_s3=True):
        ### after running add_pred_s3, we need to download the latest csv
        existing_versions = self.get_existing_versions(coin, model_name)
        print(f"Existing versions for {coin}: {existing_versions}")
        ### get the last version (it will be the latest)
        chosen_version = None if not existing_versions else existing_versions[-1]
        print(f"Chosen version for download: {chosen_version}")
        ### Delete local file if exists
        if chosen_version is not None:
            local_file = f"data/predictions/{coin}/{model_name}/v{chosen_version.split('/')[-1].split('.')[0][1:]}.csv"
            if os.path.exists(local_file):
                os.remove(local_file)
                print(f"Deleted existing local file: {local_file}")
                
            self.download_df(local_file=local_file, bucket='mlops', key=chosen_version)
            print(f"Downloaded latest predictions to {local_file}")
            
        if upload_s3:
            print("Updating previous version predictions from local csv files to s3") ## backup
            for v in existing_versions[:-1]:
                local_file = f"data/predictions/{coin}/{model_name}/v{v.split('/')[-1].split('.')[0][1:]}.csv"
                if os.path.exists(local_file):
                    df = pd.read_csv(local_file)
                    self.upload_df(df, bucket='mlops', key=v)
                    print(f"Uploaded backup of previous version {v} from local file {local_file}")

    def add_pred_s3(self, df, coin, model_name):
        ### df is new version with predictions
        ### model/<model_name>/predictions/v1.parquet
        ### there can be max 3 versions v1, v2, v3. it can be empty too
        ### if this is the first time, just upload as v1
        
        existing_versions = self.get_existing_versions(coin, model_name)
        print(f"Existing versions for {coin}: {existing_versions}")
        l = len(existing_versions)
        self.upload_df(df, bucket='mlops', key=f'predictions/{coin}/{model_name}/v{l+1}.parquet')
        print(f"Uploaded new predictions as v{l+1} for {coin}")

        
        
    def reassign_pred_s3(self, coin, model_name):
        ### if there are existing versions, shift them down and upload new as v1. Delete v2 and put v3 as v2. BUT ALWAYS KEEP V1 as it IS
        existing_versions = self.get_existing_versions(coin, model_name)
        if len(existing_versions) <= 3:
            print(f"Not enough existing versions to reassign for {coin}. Need at least 4, found {len(existing_versions)}. Uploading new as next version.")
            return
        
        print(f"Existing versions for {coin}: {existing_versions}")
        ### deleting v2
        print(f"Deleting old version: {existing_versions[1]}")
        self.s3.delete_object(Bucket='mlops', Key=existing_versions[1])
        
        ## rename v3 to v2
        print(f"Renaming v3 to v2 for {coin}")
        self.s3.copy_object(Bucket='mlops', CopySource={'Bucket': 'mlops', 'Key': f'predictions/{coin}/{model_name}/v3.parquet'}, Key=f'predictions/{coin}/{model_name}/v2.parquet')
        
        ## rename v4 to v3
        print(f"Renaming v4 to v3 for {coin}")
        self.s3.copy_object(Bucket='mlops', CopySource={'Bucket': 'mlops', 'Key': f'predictions/{coin}/{model_name}/v4.parquet'}, Key=f'predictions/{coin}/{model_name}/v3.parquet')
        
        ### delete v4
        print(f"Deleting old version: {existing_versions[3]}")
        self.s3.delete_object(Bucket='mlops', Key=existing_versions[3])
            

                        

    def download_df(self, local_file: str, bucket: str, key: str):
        """
        Download a file from S3 and save it locally.
        Creates directories if they do not exist.
        Skips download if the file already exists locally.
        """
        
        if os.path.exists(local_file.replace('.parquet', '.csv')):
            # print(f"Local file {local_file} already exists, checking hash...")
            if os.path.exists(local_file.replace('.parquet', '.hash')):
                with open(local_file.replace('.csv', '.hash'), 'r') as f:
                    local_hash = f.read().strip()
                    print(f"Local hash: {local_hash}")
                s3_hash = self._get_s3_hash(bucket, key.replace('.parquet', '.hash'))
                # print(f"S3 hash: {s3_hash}")
                if local_hash == s3_hash:
                    print(f"Local file {local_file} is up-to-date with S3. Skipping download.")
                    return
                else:
                    print(f"Hash mismatch for {local_file}. Downloading new version.")

        # Ensure directory exists
        os.makedirs(os.path.dirname(local_file), exist_ok=True)

        # Download from S3
        buffer = BytesIO()
        self.s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)

        ### convert to DataFrame

        df = pd.read_parquet(buffer)
   
        # Save DataFrame to local file as csv
        df.to_csv(local_file.replace('.parquet', '.csv'), index=False)

        # DOwnload hash file too
        hash_key = key.replace('.parquet', '.hash')
        s3_hash = self._get_s3_hash(bucket, hash_key)
        if s3_hash:
            ## save hash locally
            s3_hash_file = local_file.replace('.csv', '.hash')
            with open(s3_hash_file, 'w') as f:
                f.write(s3_hash)
            
        print(f"Downloaded {key} to {local_file}, and hash to {s3_hash_file}")
        
        
    def _upload_file(self, local_file: str, bucket: str, key: str):
        self.s3.upload_file(local_file, bucket, key)

    def _download_file(self, local_file: str, bucket: str, key: str):
        self.s3.download_file(bucket, key, local_file)

    # -------------------
    # List objects
    # -------------------
    def list_objects(self, bucket: str, prefix: str = ''):
        response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]




    def _upload_dir(self, local_dir, bucket, s3_prefix):
        for root, _, files in os.walk(local_dir):
            for file in files:
                path = Path(root) / file
                rel_path = os.path.relpath(path, local_dir)
                self.s3.upload_file(str(path), bucket, f"{s3_prefix}{rel_path}")
                print(f"Uploaded {rel_path} → s3://{bucket}/{s3_prefix}{rel_path}")

# -------------------
# Example usage
# -------------------
if __name__ == "__main__":
    s3_manager = S3Manager(
  )

    # coins = ["BTCUSDT"]
    # article_path = f"data/articles/articles.csv"   
    # s3_manager.upload_df(article_path, bucket='mlops', key=f'articles/articles.parquet')
    # s3_manager.download_df(article_path, bucket='mlops', key=f'articles/articles.parquet')
        
    # for coin in coins:
            
    #     prices_path = f"data/prices/{coin}.csv"
    #     price_test_path = f"data/prices/{coin}_test.csv"
        
        
    #     s3_manager.upload_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')
    #     s3_manager.upload_df(price_test_path, bucket='mlops', key=f'prices/{coin}_test.parquet')
        

    #     s3_manager.download_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')
    #     s3_manager.download_df(price_test_path, bucket='mlops', key=f'prices/{coin}_test.parquet')
        
    coins = ["BTCUSDT"]
    # article_path = f"data/articles/articles.csv"   
    # s3_manager.upload_df(article_path, bucket='mlops', key=f'articles/articles.parquet')
    # s3_manager.download_df(article_path, bucket='mlops', key=f'articles/articles.parquet')
        
    for coin in coins:
            
        prices_path = f"/home/frozenwolf/Desktop/{coin}.csv"
        price_test_path = f"/home/frozenwolf/Desktop/{coin}_test.csv"
        
        
        s3_manager.upload_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')
        s3_manager.upload_df(price_test_path, bucket='mlops', key=f'prices/{coin}_test.parquet')
        

        # s3_manager.download_df(prices_path, bucket='mlops', key=f'prices/{coin}.parquet')
        # s3_manager.download_df(price_test_path, bucket='mlops', key=f'prices/{coin}_test.parquet')
        

# Project directory structure:
# s3://mlops/
# articles/
#    _articles.parquet

# └─ prices/
#    ├─ BTCUSDT.parquet
#    ├─ BTCUSDT_test.parquet
#    ├─ ETHUSDT.parquet
#    ├─ ETHUSDT_test.parquet
#    ├─ BNBUSDT.parquet
#    └─ BNBUSDT_test.parquet

#ml-flow
# └─ <experiment_id>/<run_id>/artifacts/
#    ├─ model/                          # MLflow model directory (MLmodel, model.onnx, conda.yaml, etc.)
#    └─ plots/ logs/ ...

# └─ predictions/
#    ├─ lightgbm.parquet
#    └─ tst.parquet
#    └─ trl.parquet