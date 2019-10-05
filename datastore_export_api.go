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

const DefaultSeparateKindCount = 30

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

type DatastoreExportAPI struct {
	DatastoreExportJobCheckQueue *DatastoreExportJobCheckQueue
	DSExportJobStore             *DSExportJobStore
	BQLoadJobStore               *BQLoadJobStore
}

func NewDatastoreExportAPI(queue *DatastoreExportJobCheckQueue, dseJS *DSExportJobStore, bqlJS *BQLoadJobStore) *DatastoreExportAPI {
	return &DatastoreExportAPI{
		queue, dseJS, bqlJS,
	}
}

func HandleDatastoreExportAPI(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed ioutil.Read(request.Body)", err)
		return
	}

	form := &DatastoreExportRequest{}
	if err := json.Unmarshal(body, form); err != nil {
		WriteError(w, http.StatusBadRequest, fmt.Sprintf("failed json.Unmarshal(request.Body) body=%v", string(body)), err)
		return
	}

	log.Printf("%s\n", string(body))

	kinds, err := GetDatastoreKinds(r.Context(), form)
	if err != nil {
		WriteError(w, http.StatusBadRequest, fmt.Sprintf("failed GetDatastoreKinds form=%+v", form), err)
		return
	}
	efs, err := BuildEntityFilter(r.Context(), form.NamespaceIDs, kinds, DefaultSeparateKindCount)
	if err != nil {
		WriteError(w, http.StatusBadRequest, fmt.Sprintf("failed BuildEntityFilter form=%+v", form), err)
		return
	}

	queue, err := NewDatastoreExportJobCheckQueue(r.Host, TasksClient)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed NewDatastoreExportJobCheckQueue", err)
		return
	}

	dsexportJobStore, err := NewDSExportJobStore(r.Context(), DatastoreClient)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed NewDSExportJobStore() form=%+v", form), err)
		return
	}

	bqloadJobStore, err := NewBQLoadJobStore(r.Context(), DatastoreClient)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed NewBQLoadJobStore() form=%+v", form), err)
		return
	}
	api := NewDatastoreExportAPI(queue, dsexportJobStore, bqloadJobStore)

	res := &DatastoreExportResponse{
		[]*DS2BQJobIDWithDatastoreExportJobID{},
	}
	for _, ef := range efs {
		var dsExportJobID string
		ds2bqJobID := dsexportJobStore.NewDS2BQJobID(r.Context())
		bqLoadKinds := BuildBQLoadKinds(ef, form.IgnoreBQLoadKinds)
		dsExportJobID, err := api.StartDS2BQJob(r.Context(), ds2bqJobID, string(body), form, form.NamespaceIDs, bqLoadKinds, ef)
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

func (api *DatastoreExportAPI) StartDS2BQJob(ctx context.Context, ds2bqJobID string, body string, form *DatastoreExportRequest, namespaceIDs []string, kinds []string, ef *datastore.EntityFilter) (string, error) {
	_, err := api.DSExportJobStore.Create(ctx, ds2bqJobID, body, form.ProjectID, namespaceIDs, kinds)
	if err != nil {
		return "", fmt.Errorf("failed DSExportJobStore.Create() ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
	}

	_, err = api.BQLoadJobStore.PutMulti(ctx, BuildBQLoadJobPutMultiForm(ds2bqJobID, kinds, form))
	if err != nil {
		return "", fmt.Errorf("failed BQLoadJobStore.PutMulti() ds2bqJobID=%v,bqLoadKinds=%+v.err=%+v", ds2bqJobID, kinds, err)
	}

	return api.CreateDatastoreExportJob(ctx, ds2bqJobID, form.ProjectID, form.OutputGCSFilePath, ef)
}

func (api *DatastoreExportAPI) CreateDatastoreExportJob(ctx context.Context, ds2bqJobID string, projectID string, outputGCSFilePath string, ef *datastore.EntityFilter) (string, error) {
	ope, err := datastore.Export(ctx, projectID, outputGCSFilePath, ef)
	if err != nil {
		return "", fmt.Errorf("failed datastore.Export() err=%+v", err)
	}
	switch ope.HTTPStatusCode {
	case http.StatusOK:
		log.Printf("%+v", ope)

		if _, err := api.DSExportJobStore.StartExportJob(ctx, ds2bqJobID, ope.Name, 0); err != nil {
			return "", fmt.Errorf("failed DSExportJobStore.StartExportJob. ds2bqJobID=%v,jobName=%s.err=%+v", ds2bqJobID, ope.Name, err)
		}

		if err := api.DatastoreExportJobCheckQueue.AddTask(ctx, &DatastoreExportJobCheckRequest{
			DS2BQJobID:           ds2bqJobID,
			DatastoreExportJobID: ope.Name,
		}); err != nil {
			return "", fmt.Errorf("failed queue.AddTask. jobName=%s.err=%+v", ope.Name, err)
		}
		return ope.Name, nil
	default:
		if _, err := api.DSExportJobStore.FinishExportJob(ctx, ds2bqJobID, DSExportJobStatusFailed, "", fmt.Sprintf("failed DatastoreExportJob.INSERT(). Code=%v,Message=%v", ope.Error.Code, ope.Error.Message)); err != nil {
			return "", fmt.Errorf("failed DSExportJobStore.FinishExportJob. ds2bqJobID=%v.err=%+v", ds2bqJobID, err)
		}
		return "", fmt.Errorf("failed DatastoreExportJob.INSERT(). ds2bqJobID=%v,ope.Error=%+v", ds2bqJobID, ope.Error)
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
