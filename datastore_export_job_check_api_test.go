package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/morikuni/failure"
	"github.com/sinmetal/silverdog/dogtime"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestWaitBQLoadJobStatusChecker(t *testing.T) {
	bljs, err := NewBQLoadJobStore(context.Background(), DatastoreClient)
	if err != nil {
		t.Fatal(err)
	}

	const kind1 = "Hoge"
	const kind2 = "Fuga"

	var ErrTestCode failure.StringCode = "Test"
	var ErrTest = failure.New(ErrTestCode)
	cases := []struct {
		name          string
		kinds         []string
		sendError     error
		wantError     bool
		wantErrorCode failure.StringCode
	}{
		{"done", []string{kind1, kind2}, nil, false, ""},
		{"halfway", []string{kind1}, nil, true, ErrTimeout},
		{"send error", []string{}, ErrTest, true, ErrTestCode},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			errch := make(chan error, 1)

			ds2bqJobID := bljs.NewJobID(ctx)
			_, err = bljs.PutMulti(ctx, &BQLoadJobPutMultiForm{
				JobID: ds2bqJobID,
				Kinds: []string{kind1, kind2},
			})
			if err != nil {
				t.Fatal(err)
			}

			for _, kind := range tt.kinds {
				_, err = bljs.Update(ctx, ds2bqJobID, kind, BQLoadJobStatusDone)
				if err != nil {
					t.Fatal(err)
				}
			}

			mtc := dogtime.NewManualTickerCreator()
			dogtime.SetMockTickerCreator(mtc)

			result := make(chan error, 1)
			go func() {
				result <- WaitBQLoadJobStatusChecker(ctx, 1*time.Second, bljs, ds2bqJobID, errch)
			}()

			if tt.sendError != nil {
				errch <- tt.sendError
			} else {
				for {
					m1, err := mtc.GetMockTicker(0)
					if err != nil {
						continue
					}
					m1.Fire()
					break
				}
			}

			err = <-result
			if (err != nil) != tt.wantError {
				if tt.wantError == false {
					t.Errorf("want noerror but got %v", err)
				} else {
					if failure.Is(err, tt.wantErrorCode) == false {
						t.Errorf("want ErrorCode %v but got %v", tt.wantErrorCode, err)
					}
				}
			}
		})
	}
}
