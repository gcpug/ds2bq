package datastore_test

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	cds "cloud.google.com/go/datastore"
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
	fmt.Printf("ope name is %s\n", ope.Name)
}

func TestCheckJobStatus(t *testing.T) {
	t.SkipNow() // 実際にDatastore APIを実行するので、普段はSkipする

	ctx := context.Background()

	jobName := "projects/gcpugjp-dev/operations/ASA4NjAwMjExOTIJGnRsdWFmZWQHEjF0c2FlaHRyb24tYWlzYS1zYm9qLW5pbWRhGgoyEg"

	res, err := datastore.CheckJobStatus(ctx, jobName)
	if err != nil {
		t.Fatalf("failed datastore.CheckJobStatus. err=%v", err)
	}
	fmt.Printf("%+v\n", res.Metadata)

	switch res.Status {
	case datastore.Running:
		fmt.Printf("%v JobStatus is %v\n", time.Now(), res.Status)
	case datastore.Fail:
		t.Fatalf("fail : %d:%s", res.ErrCode, res.ErrMessage)
	default:
		fmt.Printf("DONE!!!")
	}
}

func TestGetAllKinds(t *testing.T) {
	const projectID = "gcpug-ds2bq-dev"
	ctx := context.Background()

	// Test Data 投入
	{
		ds, err := cds.NewClient(ctx, projectID)
		if err != nil {
			t.Fatal(err)
		}
		kl := []*cds.Key{
			cds.NameKey("Fuga", "Sample", nil),
			cds.NameKey("Hoge", "Sample", nil),
			cds.NameKey("Moge", "Sample", nil),
		}

		el := []struct {
			Text string
		}{
			{"Fuga"},
			{"Hoge"},
			{"Moge"},
		}
		_, err = ds.PutMulti(ctx, kl, el)
		if err != nil {
			t.Fatal(err)
		}
	}

	kinds, err := datastore.GetAllKinds(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"BQLoadJob", "DSExportJob", "Fuga", "Hoge", "Moge"}
	if !reflect.DeepEqual(want, kinds) {
		t.Errorf("want %+v but got %+v", want, kinds)
	}
}
