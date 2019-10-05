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

type DatastoreExportJobCheckRequest struct {
	DS2BQJobID           string
	DatastoreExportJobID string
}

type DatastoreExportJobCheckAPI struct {
	DatastoreExportJobCheckQueue *DatastoreExportJobCheckQueue
	DSExportJobStore             *DSExportJobStore
	BQLoadJobStore               *BQLoadJobStore
	BQLoadJobCheckQueue          *BQLoadJobCheckQueue
}

func NewDatastoreExportJobCheckAPI(queue *DatastoreExportJobCheckQueue, dseJS *DSExportJobStore, bqlJS *BQLoadJobStore, bqjcQ *BQLoadJobCheckQueue) *DatastoreExportJobCheckAPI {
	return &DatastoreExportJobCheckAPI{
		queue, dseJS, bqlJS, bqjcQ,
	}
}

func HandleDatastoreExportJobCheckAPI(w http.ResponseWriter, r *http.Request) {
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

	queue, err := NewDatastoreExportJobCheckQueue(r.Host, TasksClient)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed NewDatastoreExportJobCheckQueue", err)
		return
	}

	dsexportJobStore, err := NewDSExportJobStore(ctx, DatastoreClient)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed NewDSExportJobStore() form=%+v", form), err)
		return
	}

	bqloadJobStore, err := NewBQLoadJobStore(ctx, DatastoreClient)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed NewBQLoadJobStore() form=%+v", form), err)
		return
	}

	bqljcQ, err := NewBQLoadJobCheckQueue(r.Host, TasksClient)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed NewBQLoadJobCheckQueue() form=%+v", form), err)
		return
	}

	api := NewDatastoreExportJobCheckAPI(queue, dsexportJobStore, bqloadJobStore, bqljcQ)

	if err := api.Check(ctx, form); err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
}

func (api *DatastoreExportJobCheckAPI) Check(ctx context.Context, form *DatastoreExportJobCheckRequest) error {
	res, err := datastore.CheckJobStatus(ctx, form.DatastoreExportJobID)
	if err != nil {
		return failure.New(StatusInternalServerError, failure.Messagef("failed Datastore.CheckJobStatus.err=%+v", err))
	}
	switch res.Status {
	case datastore.Running:
		log.Printf("%s is Running...\n", form.DatastoreExportJobID)

		_, err := api.DSExportJobStore.IncrementJobStatusCheckCount(ctx, form.DS2BQJobID)
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed DSExportJobStore.IncrementJobStatusCheckCount. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		return failure.New(StatusConflict)
	case datastore.Fail:
		log.Printf("%s is Fail. ErrCode=%v,ErrMessage=%v\n", form.DatastoreExportJobID, res.ErrCode, res.ErrMessage)

		_, err := api.DSExportJobStore.FinishExportJob(ctx, form.DS2BQJobID, DSExportJobStatusFailed, form.DatastoreExportJobID, fmt.Sprintf("Code=%v,MSG=%v,META=%+v", res.ErrCode, res.ErrMessage, res.Metadata))
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed DSExportJobStore.FinishExportJob. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		job, err := api.DSExportJobStore.Get(ctx, form.DS2BQJobID)
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed DSExportJobStore.Get. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		job.RetryCount++
		if job.RetryCount > 3 { // TODO MaxRetryCountを設定できるようにする
			return nil
		}

		efs, err := BuildEntityFilter(ctx, job.ExportNamespaceIDs, job.ExportKinds, len(job.ExportKinds))
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed BuildEntityFilter. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}

		ds2bqJobID := api.DSExportJobStore.NewDS2BQJobID(ctx)

		dseAPI := NewDatastoreExportAPI(api.DatastoreExportJobCheckQueue, api.DSExportJobStore, api.BQLoadJobStore)
		var dseForm DatastoreExportRequest
		if err := json.Unmarshal([]byte(job.JobRequestBody), &dseForm); err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed json.Unmarshal.ds2bqJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		_, err = dseAPI.CreateDatastoreExportJob(ctx, ds2bqJobID, job.ExportProjectID, dseForm.OutputGCSFilePath, efs[0])
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed CreateDatastoreExportJob.ds2bqJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		return nil
	case datastore.Done:
		log.Printf("%s is Done...\n", form.DatastoreExportJobID)

		_, err := api.DSExportJobStore.FinishExportJob(ctx, form.DS2BQJobID, DSExportJobStatusDone, form.DatastoreExportJobID, "")
		if err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed DSExportJobStore.FinishExportJob. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}

		if err := api.InsertBQLoadJobs(ctx, form.DS2BQJobID, res.Metadata.OutputURLPrefix); err != nil {
			return failure.New(StatusInternalServerError, failure.Messagef("failed InsertBQLoadJobs. DS2BQJobID=%v,err=%v\n", form.DS2BQJobID, err))
		}
		return nil
	default:
		return failure.New(StatusInternalServerError, failure.Messagef("%v is Unspported Status", res.Status))
	}
}

func (api *DatastoreExportJobCheckAPI) InsertBQLoadJobs(ctx context.Context, ds2bqJobID string, outputURLPrefix string) error {
	ls := NewBQLoadService(api.BQLoadJobStore, api.BQLoadJobCheckQueue)

	if err := ls.InsertBigQueryLoadJob(ctx, ds2bqJobID, outputURLPrefix); err != nil {
		return failure.Wrap(err, failure.Message("failed BQLoadService.InsertBigQueryLoadJob"))
	}

	return nil
}
