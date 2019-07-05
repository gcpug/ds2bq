package main

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func TestBQLoadService_InsertBigQueryLoadJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		t.Fatal(err)
	}
	jobID := "hoge"
	_, err = store.Put(ctx, &BQLoadJobPutForm{
		JobID: jobID,
		Kind:  "PugEvent",
	})
	if err != nil {
		t.Fatal(err)
	}

	pc, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		t.Fatal(err)
	}

	bqs := NewBQLoadService("gcpug-ds2bq-ds-export-object-change", pc, store)
	if err := bqs.InsertBigQueryLoadJob(ctx, jobID); err != nil {
		t.Fatal(err)
	}
}
