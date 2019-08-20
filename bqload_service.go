package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gcpug/ds2bq/bigquery"
)

type BQLoadService struct {
	bqLoadJobStore      *BQLoadJobStore
	bqLoadJobCheckQueue *BQLoadJobCheckQueue
}

func NewBQLoadService(bqLoadJobStore *BQLoadJobStore, bqLoadJobCheckQueue *BQLoadJobCheckQueue) *BQLoadService {
	return &BQLoadService{
		bqLoadJobStore,
		bqLoadJobCheckQueue,
	}
}

func (s *BQLoadService) InsertBigQueryLoadJob(ctx context.Context, ds2bqJobID string, outputURLPrefix string) error {
	loadJobs, err := s.bqLoadJobStore.List(ctx, ds2bqJobID)
	if err != nil {
		return err
	}
	for _, loadJob := range loadJobs {
		gcsPath := fmt.Sprintf("%s/all_namespaces/kind_%s/all_namespaces_kind_%s.export_metadata", outputURLPrefix, loadJob.Kind, loadJob.Kind)

		bqLoadJobId, err := bigquery.Load(ctx, loadJob.BQLoadProjectID, gcsPath, loadJob.BQLoadDatasetID, loadJob.Kind)
		if err != nil {
			log.Printf("failed bigquery.Load() DS2BQJobID=%v,GCSObjectID=%v,err=%v\n", ds2bqJobID, gcsPath, err)
			return err
		}
		fmt.Printf("bq insert job. ds2bqJobID=%v,kind=%v,gcs=%v,bqLoadJobID=%v\n", ds2bqJobID, loadJob.Kind, gcsPath, bqLoadJobId)

		_, err = s.bqLoadJobStore.StartLoadJob(ctx, ds2bqJobID, loadJob.Kind, bqLoadJobId)
		if err != nil {
			log.Printf("failed BQLoadJobStore.Update() DS2BQJobID=%v,GCSObjectID=%v,err=%v\n", ds2bqJobID, gcsPath, err)
			return err
		}

		if err := s.bqLoadJobCheckQueue.AddTask(ctx, &BQLoadJobCheckRequest{
			DS2BQJobID:        ds2bqJobID,
			BQLoadProjectID:   loadJob.BQLoadProjectID,
			BQLoadKind:        loadJob.Kind,
			BigQueryLoadJobID: bqLoadJobId,
		}); err != nil {
			log.Printf("failed BQLoadJobCheckQueue.AddTask(). DS2BQJobID=%v,Kind=%v,BigQueryLoadJobID=%v\n", ds2bqJobID, loadJob.Kind, bqLoadJobId)
			return err
		}
	}

	return nil
}
