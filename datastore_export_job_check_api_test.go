package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleDatastoreExportJobCheckAPI(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	hf := http.HandlerFunc(HandleDatastoreExportJobCheckAPI)
	server := httptest.NewServer(hf)
	defer server.Close()

	form := DatastoreExportJobCheckRequest{
		DatastoreExportJobID: "projects/gcpugjp-dev/operations/ASAxODAwMjIwODIJGnRsdWFmZWQHEjF0c2FlaHRyb24tYWlzYS1zYm9qLW5pbWRhGgoyEg",
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
