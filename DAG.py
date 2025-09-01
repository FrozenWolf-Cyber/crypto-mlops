### start kafka server
### python consumer_start.py (crypto, model, v1) -> 2x2x3 -> downloads dataset, predictions (trigger initially) from s3 if not present locally
### -> local csv writer 
### -> make sure no duplicates in either of the csv -> skip those while listening
### -> V1 writes on main csv raw data, while V1, V2, V3 write its predictions to separate csvs
### -> infer through fastapi
### -> psql writer (db.py session pool)
### resume all consumers based on available versions -> consumer_control.py

### python producer.py 
### python hourly_scrapper.py
### mlflow server start
### python fastapi inference.py  -> fastapi_app.py
### prometheus fastapi_instrumentator
### training
### -> create vast ai instance and install k8 and connect to our k8s cluster
### -> slice train data, update test data and upload training data to s3
### -> submit using dag's kube job operator on lightgbm_train.py, tst_train.py, trl_train.py for each crypto [assign max memory needed and handle dynaically during dag]
### -> train model -> (need ENV variables) mlflow versioning staging handled (model manager trigger at end of script) -> also uploads predictions to s3

### on training end of xth model version v:
### stop consumers for x-v-2  -> consumer start.py
### stop consumers for x-v-3  -> consumer start.py
### pull new fastapi models from s3 -> trigger fastapi_app.py /refresh endpoint
### renamed pred of x-v-3 locally to x-v-2 -> need to create a util
### start consumers for x-v-2 -> consumer start.py
### download pred of new x-v-3 from s3 to local as x-v-3 -> s3_manager.py util
### infer remaining test data (delta of what was pushed to s3 before training to till now in raw data) for x-v-3 and save pred locally as x-v-3 -> trigger the fastapi_app.py /predict endpoint util
### push complete pred of x-v-3 to psql -> db.py session pool util
### start consumers for x-v-3 (should be no lag since new data filled in above step) -> consumer start.py
### upload new predictions to s3 (v2 and v3 since new data filled) (concurrently with above step) -> s3_manager.py util


### on crash:
	### upload datasets and predictions to s3 -> s3_manager.py util