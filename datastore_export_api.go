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
	IDs []*DS2BQJobIDWithDatastoreExportJobID `json:"ids"`
}

type DS2BQJobIDWithDatastoreExportJobID struct {
	DS2BQJobID           string `json:"ds2bqJobId"`
	DatastoreExportJobID string `json:"datastoreExportJobId"`
}

func HandleDatastoreExportAPI(w http.ResponseWriter, r *http.Request) {
	queue, err := NewDatastoreExportJobCheckQueue(r.Host, TasksClient)
	if err != nil {
		msg := fmt.Sprintf("failed NewDatastoreExportJobCheckQueue.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	body, err := ioutil.ReadAll(r.Body)
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
	if err := json.Unmarshal(body, form); err != nil {
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

	log.Printf("%s\n", string(body))

	kinds, err := GetDatastoreKinds(r.Context(), form)
	if err != nil {
		msg := fmt.Sprintf("failed GetDatastoreKinds form=%+v.err=%+v", form, err)
		log.Println(msg)
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	efs, err := BuildEntityFilter(r.Context(), form.NamespaceIDs, kinds, 30)
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

	res := &DatastoreExportResponse{
		[]*DS2BQJobIDWithDatastoreExportJobID{},
	}
	for _, ef := range efs {
		var dsExportJobID string
		ds2bqJobID := dsexportJobStore.NewDS2BQJobID(r.Context())
		bqLoadKinds := BuildBQLoadKinds(ef, form.IgnoreBQLoadKinds)
		dsExportJobID, err := CreateDatastoreExportJob(r.Context(), dsexportJobStore, bqloadJobStore, queue, ds2bqJobID, string(body), form, bqLoadKinds, ef)
		if err != nil {
			msg := fmt.Sprintf("failed CreateDatastoreExportJob ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
			log.Println(msg)
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(msg))
			if err != nil {
				log.Println(err)
			}
			return
		}
		res.IDs = append(res.IDs, &DS2BQJobIDWithDatastoreExportJobID{
			DS2BQJobID:           ds2bqJobID,
			DatastoreExportJobID: dsExportJobID,
		})
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		log.Println(err)
	}
}

func CreateDatastoreExportJob(ctx context.Context, dsexportJobStore *DSExportJobStore, bqloadJobStore *BQLoadJobStore, queue *DatastoreExportJobCheckQueue, ds2bqJobID string, body string, form *DatastoreExportRequest, kinds []string, ef *datastore.EntityFilter) (string, error) {
	_, err := dsexportJobStore.Create(ctx, ds2bqJobID, body, kinds)
	if err != nil {
		return "", fmt.Errorf("failed DSExportJobStore.Create() ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
	}

	_, err = bqloadJobStore.PutMulti(ctx, BuildBQLoadJobPutMultiForm(ds2bqJobID, kinds, form))
	if err != nil {
		return "", fmt.Errorf("failed BQLoadJobStore.PutMulti() ds2bqJobID=%v,bqLoadKinds=%+v.err=%+v", ds2bqJobID, kinds, err)
	}

	ope, err := datastore.Export(ctx, form.ProjectID, form.OutputGCSFilePath, ef)
	if err != nil {
		return "", fmt.Errorf("failed datastore.Export() form=%+v.err=%+v", form, err)
	}
	switch ope.HTTPStatusCode {
	case http.StatusOK:
		log.Printf("%+v", ope)

		if _, err := dsexportJobStore.StartExportJob(ctx, ds2bqJobID, ope.Name); err != nil {
			return "", fmt.Errorf("failed DSExportJobStore.StartExportJob. ds2bqJobID=%v,jobName=%s.err=%+v", ds2bqJobID, ope.Name, err)
		}

		if err := queue.AddTask(ctx, &DatastoreExportJobCheckRequest{
			DS2BQJobID:           ds2bqJobID,
			DatastoreExportJobID: ope.Name,
		}); err != nil {
			return "", fmt.Errorf("failed queue.AddTask. jobName=%s.err=%+v", ope.Name, err)
		}
		return ope.Name, nil
	default:
		if _, err := dsexportJobStore.FinishExportJob(ctx, ds2bqJobID, DSExportJobStatusFailed, fmt.Sprintf("failed DatastoreExportJob.INSERT(). Code=%v,Message=%v", ope.Error.Code, ope.Error.Message)); err != nil {
			return "", fmt.Errorf("failed DSExportJobStore.FinishExportJob. ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
		}
		return "", fmt.Errorf("failed DatastoreExportJob.INSERT(). form=%+v.ope.Error=%+v", form, ope.Error)
	}
}

func GetDatastoreKinds(ctx context.Context, form *DatastoreExportRequest) ([]string, error) {
	var err error
	kinds := form.Kinds
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

	return kinds, nil
}

func BuildEntityFilter(ctx context.Context, namespaceIDs []string, kinds []string, size int) ([]*datastore.EntityFilter, error) {
	work := kinds
	var result []*datastore.EntityFilter
	for {
		if len(work) < 1 {
			break
		}
		end := size
		if len(work) <= end {
			end = len(work)
		}
		result = append(result, &datastore.EntityFilter{
			Kinds:        work[:end],
			NamespaceIds: namespaceIDs,
		})
		work = work[end:]
	}
	return result, nil
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
