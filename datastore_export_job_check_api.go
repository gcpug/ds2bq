package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gcpug/ds2bq/datastore"
)

type DatastoreExportJobCheckRequest struct {
	JobID string
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

	res, err := datastore.CheckJobStatus(r.Context(), form.JobID)
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
		log.Printf("%s is Running...\n", form.JobID)
		w.WriteHeader(http.StatusConflict)
	case datastore.Fail:
		log.Printf("%s is Fail. ErrCode=%v,ErrMessage=%v\n", form.JobID, res.ErrCode, res.ErrMessage)
		w.WriteHeader(http.StatusOK)
	case datastore.Done:
		w.WriteHeader(http.StatusOK)
	default:
		log.Printf("%v is Unsupported Status\n", res.Status)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
