package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gcpug/ds2bq/datastore"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestBuildEntityFilter(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	cases := []struct {
		name string
		form *DatastoreExportRequest
		want *datastore.EntityFilter
	}{
		{"Specified Kinds", &DatastoreExportRequest{
			ProjectID:         "gcpugjp-dev",
			Kinds:             []string{"Hoge", "Fuga"},
			NamespaceIDs:      []string{""},
			OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
		}, &datastore.EntityFilter{
			Kinds:        []string{"Hoge", "Fuga"},
			NamespaceIds: []string{""},
		}},
		{"All Kinds", &DatastoreExportRequest{
			ProjectID:         "gcpugjp-dev",
			AllKinds:          true,
			NamespaceIDs:      []string{""},
			OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
		}, &datastore.EntityFilter{
			Kinds:        []string{"DatastoreSample", "Organization", "PugEvent", "SpannerAccount"},
			NamespaceIds: []string{""},
		}},
		{"Ignore Kinds", &DatastoreExportRequest{
			ProjectID:         "gcpugjp-dev",
			AllKinds:          true,
			IgnoreKinds:       []string{"DatastoreSample"},
			NamespaceIDs:      []string{""},
			OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
		}, &datastore.EntityFilter{
			Kinds:        []string{"Organization", "PugEvent", "SpannerAccount"},
			NamespaceIds: []string{""},
		}},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildEntityFilter(context.Background(), tt.form)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(tt.want.Kinds, got.Kinds) == false {
				t.Errorf("want Kinds %v but got %v", tt.want.Kinds, got.Kinds)
			}
			if reflect.DeepEqual(tt.want.NamespaceIds, got.NamespaceIds) == false {
				t.Errorf("want NamespaceIds %v but got %v", tt.want.NamespaceIds, got.NamespaceIds)
			}
		})
	}
}
