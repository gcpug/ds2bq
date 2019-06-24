package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gcpug/ds2bq/datastore"
	"github.com/morikuni/failure"
)

type DatastoreExportRequest struct {
	ProjectID         string   `json:"projectId"`
	AllKinds          bool     `json:"allKinds"`
	Kinds             []string `json:"kinds"`
	NamespaceIDs      []string `json:"namespaceIds"`
	IgnoreKinds       []string `json:"ignoreKinds"`
	IgnoreBQLoadKinds []string `json:"ignoreBQLoadKinds"`
	OutputGCSFilePath string   `json:"outputGCSFilePath"`
}

func HandleDatastoreExportAPI(w http.ResponseWriter, r *http.Request) {
	queue, err := NewJobStatusCheckQueue(r.Host, TasksClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewJobStatusCheckQueue.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

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

	form := &DatastoreExportRequest{}
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

	ef, err := BuildEntityFilter(r.Context(), form)
	if err != nil {
		msg := fmt.Sprintf("failed BuildEntityFilter form=%+v.err=%+v", form, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	ope, err := datastore.Export(r.Context(), form.ProjectID, form.OutputGCSFilePath, ef)
	if err != nil {
		msg := fmt.Sprintf("failed datastore.Export() form=%+v.err=%+v", form, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	switch ope.HTTPStatusCode {
	case http.StatusOK:
		log.Printf("%+v", ope)

		if err := queue.AddTask(r.Context(), &DatastoreExportJobCheckRequest{
			JobID: ope.Name,
		}); err != nil {
			msg := fmt.Sprintf("failed queue.AddTask. jobName=%s.err=%+v", ope.Name, err)
			log.Println(msg)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write([]byte(msg))
			if err != nil {
				log.Println(err)
			}
			return
		}
		w.WriteHeader(ope.HTTPStatusCode)
	default:
		msg := fmt.Sprintf("datastore export API Response Code is not OK. form=%+v.ope=%+v", form, ope)
		log.Println(msg)
		w.WriteHeader(ope.HTTPStatusCode)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
}

func BuildEntityFilter(ctx context.Context, form *DatastoreExportRequest) (*datastore.EntityFilter, error) {
	var err error
	kinds := form.Kinds
	ns := form.NamespaceIDs
	if form.AllKinds {
		kinds, err = datastore.GetAllKinds(ctx, form.ProjectID)
		if err != nil {
			return nil, failure.Wrap(err)
		}
		kinds = kinds
	}
	if len(form.IgnoreKinds) > 0 {
		var nks []string
		m := map[string]string{}
		for _, v := range form.IgnoreKinds {
			m[v] = v
		}

		for _, v := range kinds {
			if _, ok := m[v]; ok {
				continue
			}
			nks = append(nks, v)
		}
		kinds = nks
	}

	return &datastore.EntityFilter{
		Kinds:        kinds,
		NamespaceIds: ns,
	}, nil
}
