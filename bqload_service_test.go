package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

func TestBQLoadService_InsertBigQueryLoadJob(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	const ds2bqJobID = "helloJob"
	{
		form := &BQLoadJobPutForm{
			JobID:           ds2bqJobID,
			Kind:            "PugEvent",
			BQLoadProjectID: "gcpugjp-dev",
			BQLoadDatasetID: "datastore",
		}
		_, err = s.Put(ctx, form)
		if err != nil {
			t.Fatal(err)
		}
	}

	ls := NewBQLoadService(s)
	if err := ls.InsertBigQueryLoadJob(ctx, ds2bqJobID, "gs://datastore-backup-gcpugjp-dev/2019-07-25T10:35:08_16520"); err != nil {
		t.Fatal(err)
	}

}
