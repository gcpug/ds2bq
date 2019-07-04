# ds2bq
Google Cloud Datastore ExportのデータをBigQueryにLoadするアプリケーション

## Setup

### BigQuery Loadをするための設定

#### Cloud StorageのRegistering object changesをPubSubに通知する設定

```
# Objectの更新情報を通知するPubSub Topicを作成
gsutil notification create -t {TOPIC_NAME} -f json gs://{DATASTORE_EXPORT_BUCKET}

# PubSub TopicをPullするSubscriberを作成
gcloud pubsub subscriptions create {SUBSCRIPTION_NAME} --topic {TOPIC_NAME}

# Subscriberの名前を設定
gcloud beta run services update ds2bq --update-env-vars STORAGE_CHANGE_NOTIFY_SUBSCRIPTION={SUBSCRIPTION_NAME}
```

example

```
gsutil notification create -t gcpug-ds2bq-ds-export-object-change -f json gs://datastore-backup-gcpugjp-dev

gcloud pubsub subscriptions create gcpug-ds2bq-ds-export-object-change --topic gcpug-ds2bq-ds-export-object-change

gcloud beta run services update ds2bq --update-env-vars STORAGE_CHANGE_NOTIFY_SUBSCRIPTION=gcpug-ds2bq-ds-export-object-change
```