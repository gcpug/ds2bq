package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gcpug/ds2bq/datastore"
	mds "go.mercari.io/datastore"
)

func TestHandleDatastoreExportAPI(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	ctx := context.Background()
	store, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		t.Fatal(err)
	}

	hf := http.HandlerFunc(HandleDatastoreExportAPI)
	server := httptest.NewServer(hf)
	defer server.Close()

	cases := []struct {
		name                string
		form                DatastoreExportRequest
		wantBQLoadProjectID string
		wantBQLoadDatasetID string
	}{
		{"default value",
			DatastoreExportRequest{
				ProjectID:         "gcpugjp-dev",
				OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
				Kinds:             []string{"PugEvent"},
			}, "gcpugjp-dev", "datastore"},
		{"explicit value",
			DatastoreExportRequest{
				ProjectID:         "gcpugjp-dev",
				OutputGCSFilePath: "gs://datastore-backup-gcpugjp-dev",
				Kinds:             []string{"PugEvent"},
				BQLoadProjectID:   "gcpugjp",
				BQLoadDatasetID:   "ds2bqtest",
			}, "gcpugjp", "ds2bqtest"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			b, err := json.Marshal(tt.form)
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
			var respBody DatastoreExportResponse
			if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
				t.Fatal(err)
			}
			job, err := store.Get(ctx, respBody.DS2BQJobID, "PugEvent")
			if err != nil {
				if err == mds.ErrNoSuchEntity {
					t.Errorf("BQLoadJobStore ErrNoSuchEntity JobID=%s,Kind=%s", respBody.DS2BQJobID, "PugEvent")
					return
				}
				t.Fatal(err)
			}
			if e, g := tt.wantBQLoadProjectID, job.BQLoadProjectID; e != g {
				t.Errorf("want BQLoadProjectID %s but got %s", e, g)
			}
			if e, g := tt.wantBQLoadDatasetID, job.BQLoadDatasetID; e != g {
				t.Errorf("want BQLoadDatasetID %s but got %s", e, g)
			}
		})
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

func TestBuildBQLoadKinds(t *testing.T) {
	cases := []struct {
		name        string
		ef          *datastore.EntityFilter
		ignoreKinds []string
		want        []string
	}{
		{"ignore empty", &datastore.EntityFilter{
			Kinds: []string{"Hoge", "Fuga"},
		},
			[]string{},
			[]string{"Hoge", "Fuga"},
		},
		{"exits ignore", &datastore.EntityFilter{
			Kinds: []string{"Hoge", "Fuga"},
		},
			[]string{"Hoge"},
			[]string{"Fuga"},
		},
		{"exits ignore", &datastore.EntityFilter{
			Kinds: []string{"Hoge", "Fuga", "Duga"},
		},
			[]string{"Hoge", "Fuga", "Moge"},
			[]string{"Duga"},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := BuildBQLoadKinds(tt.ef, tt.ignoreKinds)
			if reflect.DeepEqual(tt.want, got) == false {
				t.Errorf("want Kinds %v but got %v", tt.want, got)
			}
		})
	}
}
