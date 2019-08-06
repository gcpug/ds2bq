# ds2bq
Google Cloud Datastore ExportのデータをBigQueryにLoadするアプリケーション

## Setup

### Deploy

#### 別ProjectのDatastoreをExportする時の設定

example

```
# ds2bqをDeployするProjectで実行

gcloud iam service-accounts create gcpug-ds2bq --display-name gcpug-ds2bq

gcloud projects add-iam-policy-binding $DS2BQ_PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/datastore.user
gcloud projects add-iam-policy-binding $DS2BQ_PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/cloudtasks.enqueuer
gcloud projects add-iam-policy-binding $DS2BQ_PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/iam.serviceAccountUser
gcloud projects add-iam-policy-binding $DS2BQ_PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.jobUser

gcloud beta run deploy gcpug-ds2bq --image=gcr.io/gcpug-container/ds2bq:v0.1.1 --service-account=gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com

gcloud beta tasks queues create gcpug-ds2bq-datastore-job-check --max-concurrent-dispatches=1 --max-dispatches-per-second=1 --min-backoff=300s 

gcloud beta run services update gcpug-ds2bq --set-env-vars=JOB_STATUS_CHECK_QUEUE_NAME=projects/$DS2BQ_PROJECT_ID/locations/asia-northeast1/queues/gcpug-ds2bq-datastore-job-check

gcloud iam service-accounts create scheduler --display-name scheduler

gcloud beta run services add-iam-policy-binding gcpug-ds2bq --member serviceAccount:scheduler@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role roles/run.invoker
gcloud beta run services add-iam-policy-binding gcpug-ds2bq --member serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role roles/run.invoker

gcloud scheduler jobs create http gcpug-ds2bq --schedule="16 16 * * *" --uri=https://{YOUR_DS2BQ_CLOUD_RUN_URI}/api/v1/datastore-export/ \
  --message-body='{"projectID": "datastore-project","outputGCSFilePath": "gs://datastore-project-ds2bq-test","allKinds":true, "bqLoadProjectId":"datastore-project", "bqLoadDatasetId":"ds2bq_test"}' \
  --oidc-service-account-email=scheduler@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com

# exportするdatastoreのProjectで実行

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/datastore.importExportAdmin
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.dataEditor
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:gcpug-ds2bq@$DS2BQ_PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.jobUser
```
