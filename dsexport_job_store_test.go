package main

import (
	"context"
	"encoding/json"
	"testing"

	cds "cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"go.mercari.io/datastore/clouddatastore"
)

func TestDSExportJobStore_Lifecycle(t *testing.T) {
	ctx := context.Background()

	cdsc, err := cds.NewClient(ctx, uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	ds, err := clouddatastore.FromClient(ctx, cdsc)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewDSExportJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	req := DatastoreExportRequest{
		ProjectID:         "gcpugjp-dev",
		OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
		Kinds:             []string{"PugEvent"},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	ds2bqJobID := s.NewDS2BQJobID(ctx)
	{
		job, err := s.Create(ctx, ds2bqJobID, string(body), []string{"PugEvent"})
		if err != nil {
			t.Fatal(err)
		}

		if e, g := DSExportJobStatusDefault, job.Status; e != g {
			t.Fatalf("want Status is %v but got %v", e, g)
		}
		if e, g := string(body), job.JobRequestBody; e != g {
			t.Fatalf("want JobRequestBody is %v but got %v", e, g)
		}
	}

	{
		const dsExportJobID = "dummyDatastoreExportJobID"
		job, err := s.StartExportJob(ctx, ds2bqJobID, dsExportJobID)
		if err != nil {
			t.Fatal(err)
		}

		if e, g := DSExportJobStatusRunning, job.Status; e != g {
			t.Fatalf("want Status is %v but got %v", e, g)
		}
		if e, g := dsExportJobID, job.DSExportJobID; e != g {
			t.Fatalf("want DSExportJobID is %v but got %v", e, g)
		}
	}

	{
		const msg = "failed operation..."
		job, err := s.FinishExportJob(ctx, ds2bqJobID, DSExportJobStatusFailed, msg)
		if err != nil {
			t.Fatal(err)
		}

		if e, g := DSExportJobStatusFailed, job.Status; e != g {
			t.Fatalf("want Status is %v but got %v", e, g)
		}
		if e, g := msg, job.DSExportResponseMessage; e != g {
			t.Fatalf("want Message is %v but got %v", e, g)
		}
	}
}

func TestDSExportJobStore_IncrementJobStatusCheckCount(t *testing.T) {
	ctx := context.Background()

	cdsc, err := cds.NewClient(ctx, uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	ds, err := clouddatastore.FromClient(ctx, cdsc)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewDSExportJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	ds2bqJobID := s.NewDS2BQJobID(ctx)
	_, err = s.Create(ctx, ds2bqJobID, "", []string{})
	if err != nil {
		t.Fatal(err)
	}

	job, err := s.IncrementJobStatusCheckCount(ctx, ds2bqJobID)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := 1, job.StatusCheckCount; e != g {
		t.Errorf("want StatusCheckCount is %v but got %v", e, g)
	}
}
