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
	BQLoadProjectID   string   `json:"bqLoadProjectId"`
	BQLoadDatasetID   string   `json:"bqLoadDatasetId"`
}

type DatastoreExportResponse struct {
	DS2BQJobID           string `json:"ds2bqJobId"`
	DatastoreExportJobID string `json:"datastoreExportJobId"`
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

	bqLoadKinds := BuildBQLoadKinds(ef, form.IgnoreBQLoadKinds)
	dsexportJobStore, err := NewDSExportJobStore(r.Context(), DatastoreClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewDSExportJobStore() form=%+v.err=%+v", form, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	bqloadJobStore, err := NewBQLoadJobStore(r.Context(), DatastoreClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewBQLoadJobStore() form=%+v.err=%+v", form, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	ds2bqJobID := dsexportJobStore.NewDS2BQJobID(r.Context())
	_, err = dsexportJobStore.Create(r.Context(), ds2bqJobID, string(b))
	if err != nil {
		msg := fmt.Sprintf("failed DSExportJobStore.Create() ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	_, err = bqloadJobStore.PutMulti(r.Context(), BuildBQLoadJobPutMultiForm(ds2bqJobID, bqLoadKinds, form))
	if err != nil {
		msg := fmt.Sprintf("failed BQLoadJobStore.PutMulti() ds2bqJobID=%v,bqLoadKinds=%+v.err=%+v", ds2bqJobID, bqLoadKinds, err)
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

		if _, err := dsexportJobStore.StartExportJob(r.Context(), ds2bqJobID, ope.Name); err != nil {
			msg := fmt.Sprintf("failed DSExportJobStore.StartExportJob. ds2bqJobID=%v,jobName=%s.err=%+v", ds2bqJobID, ope.Name, err)
			log.Println(msg)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(msg))
			if err != nil {
				log.Println(err)
			}
			return
		}

		if err := queue.AddTask(r.Context(), &DatastoreExportJobCheckRequest{
			DS2BQJobID:           ds2bqJobID,
			DatastoreExportJobID: ope.Name,
		}); err != nil {
			msg := fmt.Sprintf("failed queue.AddTask. jobName=%s.err=%+v", ope.Name, err)
			log.Println(msg)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(msg))
			if err != nil {
				log.Println(err)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(ope.HTTPStatusCode)
		res := DatastoreExportResponse{
			DS2BQJobID:           ds2bqJobID,
			DatastoreExportJobID: ope.Name,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			log.Printf("failed write response. %+v, err=%v", res, err)
		}
	default:
		msg := fmt.Sprintf("failed DatastoreExportJob.INSERT(). form=%+v.ope.Error=%+v", form, ope.Error)
		log.Println(msg)

		if _, err := dsexportJobStore.FinishExportJob(r.Context(), ds2bqJobID, DSExportJobStatusFailed, fmt.Sprintf("failed DatastoreExportJob.INSERT(). Code=%v,Message=%v", ope.Error.Code, ope.Error.Message)); err != nil {
			msg := fmt.Sprintf("failed DSExportJobStore.FinishExportJob. ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
			log.Println(msg)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(msg))
			if err != nil {
				log.Println(err)
			}
			return
		}
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

func BuildBQLoadKinds(ef *datastore.EntityFilter, ignoreKinds []string) []string {
	ignore := map[string]bool{}
	if len(ignoreKinds) > 0 {
		for _, v := range ignoreKinds {
			ignore[v] = true
		}
	}
	var kinds []string
	for _, kind := range ef.Kinds {
		if ignore[kind] {
			continue
		}
		kinds = append(kinds, kind)
	}

	return kinds
}

func BuildBQLoadJobPutMultiForm(jobID string, kinds []string, form *DatastoreExportRequest) *BQLoadJobPutMultiForm {
	result := BQLoadJobPutMultiForm{
		JobID:           jobID,
		Kinds:           kinds,
		BQLoadProjectID: form.BQLoadProjectID,
		BQLoadDatasetID: form.BQLoadDatasetID,
	}

	if result.BQLoadProjectID == "" {
		result.BQLoadProjectID = ProjectID
	}
	if result.BQLoadDatasetID == "" {
		result.BQLoadDatasetID = "datastore"
	}
	return &result
}
