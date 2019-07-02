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
)

type DatastoreExportJobCheckRequest struct {
	DS2BQJobID           string
	DatastoreExportJobID string
}

func HandleDatastoreExportJobCheckAPI(w http.ResponseWriter, r *http.Request) {
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
		log.Printf("%s is Running...\n", form.DatastoreExportJobID)
		w.WriteHeader(http.StatusConflict)
	case datastore.Fail:
		log.Printf("%s is Fail. ErrCode=%v,ErrMessage=%v\n", form.DatastoreExportJobID, res.ErrCode, res.ErrMessage)
		w.WriteHeader(http.StatusOK)
	case datastore.Done:
		log.Printf("%s is Done...\n", form.DatastoreExportJobID)
		if err := ReceiveStorageChangeNotify(r.Context(), form.DS2BQJobID); err != nil {
			log.Printf("failed ReceiveStorageChangeNotify. err=%v\n", err)
		}
		w.WriteHeader(http.StatusOK)
	default:
		log.Printf("%v is Unsupported Status\n", res.Status)
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
			errch <- err
		}
	}()

	t := time.NewTicker(time.Second * 60)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			jobs, err := bljs.List(cctx, ds2bqJobID)
			if err != nil {
				continue
			}
			allDone := true
			for _, job := range jobs {
				log.Printf("BQLoadJobStatusCheck. kind=%v,status=%v\n", job.Kind, job.Status)
				if job.Status != BQLoadJobStatusDone {
					allDone = false
				}
			}
			log.Printf("BQLoadJobStatusChech. %v\n", allDone)
			if allDone {
				cctx.Done()
				cancel()
				return nil
			}
		case <-cctx.Done():
			return failure.Unexpected("timeout")
		case err := <-errch:
			cancel()
			return err

		}
	}
}
