package datastore_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/gcpug/ds2bq/datastore"
)

func TestExport(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	ctx := context.Background()

	ope, err := datastore.Export(ctx, "gcpugjp-dev", "gs://datastore-backup-gcpugjp-dev", &datastore.EntityFilter{})
	if err != nil {
		t.Fatalf("failed datastore.Export(). err=%+v", err)
	}
	if e, g := http.StatusOK, ope.ServerResponse.HTTPStatusCode; e != g {
		t.Errorf("Export API Response Code expected %v; got %v", e, g)
	}
}
