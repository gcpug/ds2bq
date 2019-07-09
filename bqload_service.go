package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/gcpug/ds2bq/bigquery"
	"go.mercari.io/datastore"
)

type BQLoadService struct {
	storageChangeNotifySubscription string
	pubsub                          *pubsub.Client
	bqLoadJobStore                  *BQLoadJobStore
}

func NewBQLoadService(storageChangeNotifySubscription string, pubsub *pubsub.Client, bqLoadJobStore *BQLoadJobStore) *BQLoadService {
	return &BQLoadService{
		storageChangeNotifySubscription,
		pubsub,
		bqLoadJobStore,
	}
}

func (s *BQLoadService) InsertBigQueryLoadJob(ctx context.Context, jobID string) error {
	loadJobs, err := s.bqLoadJobStore.List(ctx, jobID)
	if err != nil {
		return err
	}
	if len(loadJobs) < 1 {
		// BigQueryにLoadする対象Kindがない場合は終了
		return nil
	}

	return s.ReceiveStorageChangeNotify(ctx, jobID)
}

func (s *BQLoadService) ReceiveStorageChangeNotify(ctx context.Context, jobID string) error {
	return s.pubsub.Subscription(s.storageChangeNotifySubscription).Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		gcsObject, err := EncodePayloadPull(message.Data)
		if err != nil {
			log.Printf("failed EncodePayload MessageID=%v,err=%v\n", message.ID, err)
			message.Nack()
			return
		}

		fmt.Printf("running %s\n", gcsObject.Name)
		kind, ok := SearchKindName(gcsObject.Name)
		if !ok {
			log.Printf("%s is SearchKindName not hit.", gcsObject.Name)
			message.Ack()
			return
		}

		lj, err := s.bqLoadJobStore.Get(ctx, jobID, kind)
		if err != nil {
			if err == datastore.ErrNoSuchEntity {
				// BQ Load対象外はAckを返して終了
				log.Printf("%s is bq load not target kind.", kind)
				message.Ack()
				return
			}
		}

		bqLoadJobId, err := bigquery.Load(ctx, lj.BQLoadProjectID, fmt.Sprintf("gs://%s/%s", gcsObject.Bucket, gcsObject.Name), lj.BQLoadDatasetID, kind)
		if err != nil {
			log.Printf("failed bigquery.Load() message.ID=%v,GCSObjectID=%v,err=%v\n", message.ID, gcsObject.Name, err)
			message.Nack()
			return
		}
		fmt.Printf("bq insert job. ds2bqJobID=%v,kind=%v,gcs=%v,bqLoadJobID=%v\n", jobID, kind, gcsObject.Name, bqLoadJobId)

		_, err = s.bqLoadJobStore.Update(ctx, jobID, kind, BQLoadJobStatusDone)
		if err != nil {
			log.Printf("failed BQLoadJobStore.Update() message.ID=%v,GCSObjectID=%v,err=%v\n", message.ID, gcsObject.Name, err)
			message.Nack()
			return
		}

		message.Ack()
	})
}
