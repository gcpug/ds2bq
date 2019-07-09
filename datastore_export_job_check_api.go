package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gcpug/ds2bq/datastore"
	"github.com/morikuni/failure"
	"github.com/sinmetal/silverdog/dogtime"
	slog "github.com/sinmetal/slog/v2"
)

var ErrTimeout failure.StringCode = "Timeout"

type DatastoreExportJobCheckRequest struct {
	DS2BQJobID           string
	DatastoreExportJobID string
}

func HandleDatastoreExportJobCheckAPI(w http.ResponseWriter, r *http.Request) {
	ctx := slog.WithValue(r.Context())
	defer slog.Flush(ctx)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("failed ioutil.Read(request.Body).err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	form := &DatastoreExportJobCheckRequest{}
	if err := json.Unmarshal(b, form); err != nil {
		msg := fmt.Sprintf("failed json.Unmarshal(request.Body).err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	log.Printf("%s\n", string(b))

	res, err := datastore.CheckJobStatus(r.Context(), form.DatastoreExportJobID)
	if err != nil {
		msg := fmt.Sprintf("failed datastore.CheckJobStatus.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	switch res.Status {
	case datastore.Running:
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("%s is Running...\n", form.DatastoreExportJobID)})
		w.WriteHeader(http.StatusConflict)
	case datastore.Fail:
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("%s is Fail. ErrCode=%v,ErrMessage=%v\n", form.DatastoreExportJobID, res.ErrCode, res.ErrMessage)})
		w.WriteHeader(http.StatusOK)
	case datastore.Done:
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("%s is Done...\n", form.DatastoreExportJobID)})
		if err := ReceiveStorageChangeNotify(ctx, form.DS2BQJobID); err != nil {
			slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("failed ReceiveStorageChangeNotify. err=%v\n", err)})
		}
		w.WriteHeader(http.StatusOK)
	default:
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("%v is Unsupported Status\n", res.Status)})
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func ReceiveStorageChangeNotify(ctx context.Context, ds2bqJobID string) error {
	sub := os.Getenv("STORAGE_CHANGE_NOTIFY_SUBSCRIPTION")
	if sub == "" {
		log.Printf("STORAGE_CHANGE_NOTIFY_SUBSCRIPTION is empty")
		return nil
	}
	ps, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		return failure.Wrap(err, failure.Message("failed pubsub.NewClient"))
	}
	bljs, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		return failure.Wrap(err, failure.Message("failed NewBQLoadJobStore"))
	}
	ls := NewBQLoadService(sub, ps, bljs)

	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	errch := make(chan error, 1)
	go func() {
		if err := ls.ReceiveStorageChangeNotify(cctx, ds2bqJobID); err != nil {
			slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("failed ReceiveStorageChangeNotify. jobID=%s\n", ds2bqJobID)})
			errch <- err
		}
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("finish! ReceiveStorageChangeNotify. jobID=%s\n", ds2bqJobID)})
	}()

	return WaitBQLoadJobStatusChecker(cctx, 60*time.Second, bljs, ds2bqJobID, errch)
}

func WaitBQLoadJobStatusChecker(ctx context.Context, d time.Duration, bljs *BQLoadJobStore, ds2bqJobID string, errch chan error) error {
	t := dogtime.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-t.Chan():
			allDone := IsBQLoadJobStatusAllDone(ctx, bljs, ds2bqJobID)
			if allDone {
				ctx.Done()
				return nil
			}
		case <-ctx.Done():
			return failure.New(ErrTimeout)
		case err := <-errch:
			allDone := IsBQLoadJobStatusAllDone(ctx, bljs, ds2bqJobID)
			if allDone {
				return nil
			}
			return err
		}
	}
}

func IsBQLoadJobStatusAllDone(ctx context.Context, bljs *BQLoadJobStore, ds2bqJobID string) bool {
	jobs, err := bljs.List(ctx, ds2bqJobID)
	if err != nil {
		return false
	}
	allDone := true
	for _, job := range jobs {
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("BQLoadJobStatusCheck. jobID=%v,kind=%v,status=%v\n", job.JobID, job.Kind, job.Status)})
		if job.Status != BQLoadJobStatusDone {
			allDone = false
		}
	}
	return allDone
}
