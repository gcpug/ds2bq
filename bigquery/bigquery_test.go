package bigquery

import (
	"context"
	"fmt"
	"testing"
)

func TestLoad(t *testing.T) {
	t.SkipNow() // 実際にBQ Loadするので、普段はSkipする

	ctx := context.Background()

	jobID, err := Load(ctx, "gcpugjp-dev", "gs://datastore-backup-gcpugjp-dev/2019-06-28T03:42:15_18632/all_namespaces/kind_PugEvent/all_namespaces_kind_PugEvent.export_metadata", "datastore", "PugEvent")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(jobID)
}
