package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleDatastoreExportAPI(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	hf := http.HandlerFunc(HandleDatastoreExportAPI)
	server := httptest.NewServer(hf)
	defer server.Close()

	form := DatastoreExportRequest{
		ProjectID:         "gcpugjp-dev",
		OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
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
