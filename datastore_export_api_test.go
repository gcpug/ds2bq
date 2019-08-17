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
	ctx := context.Background()
	dsexportjobStore, err := NewDSExportJobStore(ctx, DatastoreClient)
	if err != nil {
		t.Fatal(err)
	}
	bqLoadJobStore, err := NewBQLoadJobStore(ctx, DatastoreClient)
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
				ProjectID:         "gcpug-ds2bq-dev",
				OutputGCSFilePath: "gs://datastore-export-gcpug-ds2bq-dev",
				Kinds:             []string{"Hoge"},
			}, "gcpug-ds2bq-dev", "datastore"},
		{"explicit value",
			DatastoreExportRequest{
				ProjectID:         "gcpug-ds2bq-dev",
				OutputGCSFilePath: "gs://datastore-export-gcpug-ds2bq-dev",
				Kinds:             []string{"Hoge"},
				BQLoadProjectID:   "gcpug-ds2bq-dev",
				BQLoadDatasetID:   "ds2bqtest",
			}, "gcpug-ds2bq-dev", "ds2bqtest"},
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
			_, err = dsexportjobStore.Get(ctx, respBody.DS2BQJobID)
			if err != nil {
				if err == mds.ErrNoSuchEntity {
					t.Errorf("DSExportjobStore ErrNoSuchEntity JobID=%s", respBody.DS2BQJobID)
					return
				}
				t.Fatal(err)
			}
			job, err := bqLoadJobStore.Get(ctx, respBody.DS2BQJobID, "Hoge")
			if err != nil {
				if err == mds.ErrNoSuchEntity {
					t.Errorf("BQLoadJobStore ErrNoSuchEntity JobID=%s,Kind=%s", respBody.DS2BQJobID, "Hoge")
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

func TestGetDatastoreKinds(t *testing.T) {
	cases := []struct {
		name string
		form *DatastoreExportRequest
		want []string
	}{
		{"Specified Kinds",
			&DatastoreExportRequest{
				ProjectID:         "gcpug-ds2bq-dev",
				Kinds:             []string{"Hoge", "Fuga"},
				NamespaceIDs:      []string{""},
				OutputGCSFilePath: "gs://datastore-export-gcpug-ds2bq-dev",
			},
			[]string{"Hoge", "Fuga"},
		},
		{"All Kinds",
			&DatastoreExportRequest{
				ProjectID:         "gcpug-ds2bq-dev",
				AllKinds:          true,
				NamespaceIDs:      []string{""},
				OutputGCSFilePath: "gs://datastore-export-gcpug-ds2bq-dev",
			},
			[]string{"BQLoadJob", "DSExportJob", "Fuga", "Hoge", "Moge"},
		},
		{"Ignore Kinds",
			&DatastoreExportRequest{
				ProjectID:         "gcpug-ds2bq-dev",
				AllKinds:          true,
				IgnoreKinds:       []string{"BQLoadJob", "DSExportJob"},
				NamespaceIDs:      []string{""},
				OutputGCSFilePath: "gs://datastore-export-gcpug-ds2bq-dev",
			},
			[]string{"Fuga", "Hoge", "Moge"},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := GetDatastoreKinds(ctx, tt.form)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := len(tt.want), len(got); e != g {
				body, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				t.Errorf("want Kinds.length %+v but got %+v. got body is %+v", e, g, string(body))
				return
			}
			for i := 0; i < len(tt.want); i++ {
				if reflect.DeepEqual(tt.want[i], got[i]) == false {
					t.Errorf("want Kinds %+v but got %+v", tt.want[i], got[i])
				}
			}
		})
	}
}

func TestBuildEntityFilter(t *testing.T) {
	cases := []struct {
		name  string
		kinds []string
		want  []*datastore.EntityFilter
	}{
		{"hoge",
			[]string{"K1", "K2", "K3"},
			[]*datastore.EntityFilter{
				&datastore.EntityFilter{
					Kinds: []string{"K1", "K2"},
				},
				&datastore.EntityFilter{
					Kinds: []string{"K3"},
				},
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := BuildEntityFilter(ctx, []string{}, tt.kinds, 2)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := len(tt.want), len(got); e != g {
				body, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				t.Errorf("want EntityFilter.length %+v but got %+v. got body is %+v", e, g, string(body))
				return
			}
			for i := 0; i < len(tt.want); i++ {
				if reflect.DeepEqual(tt.want[i], got[i]) == false {
					t.Errorf("want EntityFilter %+v but got %+v", tt.want[i], got[i])
				}
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
