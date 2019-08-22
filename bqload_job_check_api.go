package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gcpug/ds2bq/bigquery"
)

type BQLoadJobCheckRequest struct {
	DS2BQJobID        string
	BQLoadProjectID   string
	BQLoadKind        string
	BigQueryLoadJobID string
}

func HandleBQLoadJobCheckAPI(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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

	var form BQLoadJobCheckRequest
	if err := json.Unmarshal(b, &form); err != nil {
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

	bqloadJobStore, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewBQLoadJobStore.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	res, err := bigquery.CheckJobStatus(ctx, form.BQLoadProjectID, form.BigQueryLoadJobID)
	if err != nil {
		msg := fmt.Sprintf("failed bigquery.CheckJobStatus.ProjectID=%v,JobID=%v,err=%+v", form.BQLoadProjectID, form.BigQueryLoadJobID, err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	switch res.Status {
	case bigquery.Running:
		_, err := bqloadJobStore.IncrementJobStatusCheckCount(ctx, form.DS2BQJobID, form.BQLoadKind)
		if err != nil {
			log.Printf("failed DSExportJobStore.IncrementJobStatusCheckCount. DS2BQJobID=%v,BQLoadKind=%v,err=%v\n", form.DS2BQJobID, form.BQLoadKind, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusConflict)
	case bigquery.Fail:
		_, err := bqloadJobStore.FinishExportJob(ctx, form.DS2BQJobID, form.BQLoadKind, BQLoadJobStatusFailed, fmt.Sprintf("MSG=%v", res.ErrMessage))
		if err != nil {
			log.Printf("failed BQLOadJobStore.FinishExportJob. DS2BQJobID=%v,BQLoadKind=%v,err=%v\n", form.DS2BQJobID, form.BQLoadKind, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	case bigquery.Done:
		_, err := bqloadJobStore.FinishExportJob(ctx, form.DS2BQJobID, form.BQLoadKind, BQLoadJobStatusDone, "")
		if err != nil {
			log.Printf("failed BQLOadJobStore.FinishExportJob. DS2BQJobID=%v,BQLoadKind=%v,err=%v\n", form.DS2BQJobID, form.BQLoadKind, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		log.Printf("%v is Unsupported Status\n", res.Status)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
