package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gcpug/ds2bq/datastore"
	"github.com/morikuni/failure"
	"io/ioutil"
	"log"
	"net/http"
)

var ErrTimeout failure.StringCode = "Timeout"

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

	dseJobStore, err := NewDSExportJobStore(r.Context(), DatastoreClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewDSExportJobStore.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

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

		_, err := dseJobStore.FinishExportJob(r.Context(), form.DS2BQJobID, DSExportJobStatusFailed, fmt.Sprintf("Code=%v,MSG=%v,BODY=%+v", res.ErrCode, res.ErrMessage))
		if err != nil {
			log.Printf("failed DSExportJobStore.FinishExportJob. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	case datastore.Done:
		log.Printf("%s is Done...\n", form.DatastoreExportJobID)

		_, err := dseJobStore.FinishExportJob(r.Context(), form.DS2BQJobID, DSExportJobStatusDone, "")
		if err != nil {
			log.Printf("failed DSExportJobStore.FinishExportJob. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := InsertBQLoadJobs(r.Context(), form.DS2BQJobID, res.Metadata.OutputURLPrefix); err != nil {
			log.Printf("failed InsertBQLoadJobs. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		log.Printf("%v is Unsupported Status\n", res.Status)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func InsertBQLoadJobs(ctx context.Context, ds2bqJobID string, outputURLPrefix string) error {
	bljs, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		return failure.Wrap(err, failure.Message("failed NewBQLoadJobStore"))
	}
	ls := NewBQLoadService(bljs)

	if err := ls.InsertBigQueryLoadJob(ctx, ds2bqJobID, outputURLPrefix); err != nil {
		return failure.Wrap(err, failure.Message("failed BQLoadService.InsertBigQueryLoadJob"))
	}

	return nil
}
