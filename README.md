# ds2bq
Google Cloud Datastore ExportのデータをBigQueryにLoadするアプリケーション

## Setup

### Deploy

#### 別ProjectのDatastoreをExportする時の設定

example

```
# ds2bqをDeployするProjectで実行
gcloud iam service-accounts create gcpug-ds2bq --display-name gcpug-ds2bq

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com --role=roles/datastore.importExportAdmin
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.dataEditor
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.jobUser

gcloud beta run deploy gcpug-ds2bq --image=gcr.io/gcpug-container/ds2bq:v0.0.4 --service-account=gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com

gcloud beta tasks queues create gcpug-ds2bq-datastore-job-check --max-concurrent-dispatches=1 --max-dispatches-per-second=1 --min-backoff=300s 

gcloud beta run services update gcpug-ds2bq --set-env-vars=JOB_STATUS_CHECK_QUEUE_NAME=projects/gcpug-ds2bq-project/locations/asia-northeast1/queues/gcpug-ds2bq-datastore-job-check

gcloud iam service-accounts create scheduler --display-name scheduler

# exportするdatastoreのProjectで実行
gcloud beta run services add-iam-policy-binding gcpug-ds2bq --region us-central1 --member serviceAccount:scheduler@gcpug-ds2bq-project.iam.gserviceaccount.com --role roles/run.invoker
gcloud beta run services add-iam-policy-binding gcpug-ds2bq --region us-central1 --member serviceAccount:gcpug-ds2bq@$PROJECT_ID.iam.gserviceaccount.com --role roles/run.invoker 

# ds2bqをDeployするProjectで実行
gcloud scheduler jobs create http gcpug-ds2bq --schedule="2 2 * * *" --uri=https://gcpug-ds2bq-ed4d43qzla-uc.a.run.app/api/v1/datastore-export/ \
  --message-body='{"projectID": "datastore-project","outputGCSFilePath": "gs://datastore-project-ds2bq-test","allKinds":true, "bqLoadProjectId":"datastore-project", "bqLoadDatasetId":"ds2bq_test"}' \
  --oidc-service-account-email=scheduler@gcpug-ds2bq-project.iam.gserviceaccount.com
```
