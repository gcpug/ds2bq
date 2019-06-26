package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

func TestBQLoadJobStore_NewKey(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	key := s.NewKey(ctx, "helloJob", "SampleKind")
	if e, g := "helloJob-_-SampleKind", key.Name(); e != g {
		t.Errorf("expected %v; got %v", e, g)
	}
}

func TestBQLoadJobStore_Put(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Put(ctx, "helloJob", "SampleKind")
	if err != nil {
		t.Fatal(err)
	}
}

func TestBQLoadJobStore_PutMulti(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	var kinds []string
	for i := 0; i < 10; i++ {
		kinds = append(kinds, fmt.Sprintf("SampleKind%d", i))
	}
	_, err = s.PutMulti(ctx, "helloJob", kinds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBQLoadJobStore_Get(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	const jobID = "helloJob"
	const kind = "SampleKind"
	_, err = s.Put(ctx, jobID, kind)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		jobID string
		kind  string
		want  error
	}{
		{"exists", jobID, kind, nil},
		{"not found", "hoge", "fuga", datastore.ErrNoSuchEntity},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Get(ctx, tt.jobID, tt.kind)
			if err != tt.want {
				t.Errorf("want %v but got %v", tt.want, err)
			}
			if err != nil {
				return
			}

			if e, g := fmt.Sprintf("%s-_-%s", jobID, kind), got.ID; e != g {
				t.Errorf("ID want %v but got %v", e, g)
			}
			if e, g := jobID, got.JobID; e != g {
				t.Errorf("JobID want %v but got %v", e, g)
			}
			if e, g := kind, got.Kind; e != g {
				t.Errorf("Kind want %v but got %v", e, g)
			}
			if e, g := BQLoadJobStatusDefault, got.Status; e != g {
				t.Errorf("Status want %v but got %v", e, g)
			}
			if got.ChangeStatusAt.IsZero() {
				t.Error("ChangeStatusAt is Zero")
			}
			if got.CreatedAt.IsZero() {
				t.Error("CreatedAt is Zero")
			}
			if got.UpdatedAt.IsZero() {
				t.Error("UpdatedAt is Zero")
			}
			if e, g := 1, got.SchemaVersion; e != g {
				t.Errorf("SchemaVersion want %v but got %v", e, g)
			}

		})
	}
}

func TestBQLoadJobStore_Update(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	const jobID = "helloJob"
	const kind = "SampleKind"
	_, err = s.Put(ctx, jobID, kind)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		jobID string
		kind  string
		want  error
	}{
		{"exists", jobID, kind, nil},
		{"not found", "hoge", "fuga", datastore.ErrNoSuchEntity},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Update(ctx, tt.jobID, tt.kind, BQLoadJobStatusFailed)
			if err != tt.want {
				t.Errorf("want %v but got %v", tt.want, err)
			}
			if err != nil {
				return
			}

			if e, g := fmt.Sprintf("%s-_-%s", jobID, kind), got.ID; e != g {
				t.Errorf("ID want %v but got %v", e, g)
			}
			if e, g := jobID, got.JobID; e != g {
				t.Errorf("JobID want %v but got %v", e, g)
			}
			if e, g := kind, got.Kind; e != g {
				t.Errorf("Kind want %v but got %v", e, g)
			}
			if e, g := BQLoadJobStatusFailed, got.Status; e != g {
				t.Errorf("Status want %v but got %v", e, g)
			}
			if got.ChangeStatusAt.IsZero() {
				t.Error("ChangeStatusAt is Zero")
			}
			if got.CreatedAt.IsZero() {
				t.Error("CreatedAt is Zero")
			}
			if got.UpdatedAt.IsZero() {
				t.Error("UpdatedAt is Zero")
			}
			if e, g := 1, got.SchemaVersion; e != g {
				t.Errorf("SchemaVersion want %v but got %v", e, g)
			}

		})
	}
}

func TestBQLoadJobStore_List(t *testing.T) {
	ctx := context.Background()

	ds, err := clouddatastore.FromContext(ctx, datastore.WithProjectID(uuid.New().String()))
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBQLoadJobStore(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}

	const jobID = "helloJob"
	const kind = "SampleKind"
	for i := 0; i < 10; i++ {
		_, err = s.Put(ctx, jobID, fmt.Sprintf("%s%d", kind, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name      string
		jobID     string
		wantCount int
	}{
		{"10entities", jobID, 10},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.List(ctx, tt.jobID)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.wantCount, len(got); e != g {
				t.Errorf("get length want %v but got %v", e, g)
			}

			model := got[0]
			if model.ID == "" {
				t.Error("ID is empty")
			}
			if e, g := jobID, model.JobID; e != g {
				t.Errorf("JobID want %v but got %v", e, g)
			}
			if model.Kind == "" {
				t.Error("Kind is empty")
			}
			if e, g := BQLoadJobStatusDefault, model.Status; e != g {
				t.Errorf("Status want %v but got %v", e, g)
			}
			if model.ChangeStatusAt.IsZero() {
				t.Error("ChangeStatusAt is Zero")
			}
			if model.CreatedAt.IsZero() {
				t.Error("CreatedAt is Zero")
			}
			if model.UpdatedAt.IsZero() {
				t.Error("UpdatedAt is Zero")
			}
			if e, g := 1, model.SchemaVersion; e != g {
				t.Errorf("SchemaVersion want %v but got %v", e, g)
			}

		})
	}
}
