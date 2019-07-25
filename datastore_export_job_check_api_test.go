package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

func TestHandleDatastoreExportJobCheckAPI(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	const ds2bqJobID = "helloJob"

	ctx := context.Background()
	{
		ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
		if err != nil {
			t.Fatal(err)
		}

		s, err := NewBQLoadJobStore(ctx, ds)
		if err != nil {
			t.Fatal(err)
		}

		form := &BQLoadJobPutForm{
			JobID:           ds2bqJobID,
			Kind:            "SampleKind",
			BQLoadProjectID: "hoge",
			BQLoadDatasetID: "fuga",
		}
		_, err = s.Put(ctx, form)
		if err != nil {
			t.Fatal(err)
		}
	}

	hf := http.HandlerFunc(HandleDatastoreExportJobCheckAPI)
	server := httptest.NewServer(hf)
	defer server.Close()

	form := DatastoreExportJobCheckRequest{
		DS2BQJobID:           ds2bqJobID,
		DatastoreExportJobID: "projects/gcpugjp-dev/operations/ASA4NjAwMjExOTIJGnRsdWFmZWQHEjF0c2FlaHRyb24tYWlzYS1zYm9qLW5pbWRhGgoyEg",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(server.URL, "application/json; charset=utf8", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if e, g := http.StatusOK, resp.StatusCode; e != g {
		t.Errorf("StatusCode expected %v; got %v", e, g)
	}
}
