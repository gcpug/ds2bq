# ds2bq
Google Cloud Datastore ExportのデータをBigQueryにLoadするアプリケーション

## Setup

### BigQuery Loadをするための設定

#### Cloud StorageのRegistering object changesをPubSubに通知する設定

// TODO 変数値を分かりやすくする

```
gsutil notification create -t gcpug-ds2bq-ds-export-object-change -f json gs://datastore-backup-gcpugjp-dev
```

```
gcloud pubsub subscriptions create gcpug-ds2bq-ds-export-object-change --topic gcpug-ds2bq-ds-export-object-change
export STORAGE_CHANGE_NOTIFY_SUBSCRIPTION=gcpug-ds2bq-ds-export-object-change
```