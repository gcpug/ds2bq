//go:generate qbg -usedatastorewrapper -output model_query.go .

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/morikuni/failure"
	"go.mercari.io/datastore"
)

type BQLoadJobStore struct {
	ds datastore.Client
}

func NewBQLoadJobStore(ctx context.Context, client datastore.Client) (*BQLoadJobStore, error) {
	return &BQLoadJobStore{
		ds: client,
	}, nil
}

type BQLoadJobStatus int

const (
	BQLoadJobStatusDefault BQLoadJobStatus = iota
	BQLoadJobStatusRunning
	BQLoadJobStatusFailed
	BQLoadJobStatusDone
)

// +qbg
type BQLoadJob struct {
	ID                    string `datastore:"-"`
	JobID                 string
	Kind                  string
	BQLoadProjectID       string // BQ Loadする先のGCP ProjectID
	BQLoadDatasetID       string // BQ Loadする先のDatasetID
	BQLoadJobID           string // BQ Load InsertのJobID
	StatusCheckCount      int
	Status                BQLoadJobStatus
	ChangeStatusAt        time.Time
	BQLoadResponseMessage string `datastore:",noindex"`
	CreatedAt             time.Time
	UpdatedAt             time.Time
	SchemaVersion         int
}

var _ datastore.PropertyLoadSaver = &BQLoadJob{}
var _ datastore.KeyLoader = &BQLoadJob{}

// BQLoadJobPutForm is Put する時のRequest内容
type BQLoadJobPutForm struct {
	JobID           string
	Kind            string
	BQLoadProjectID string // BQ Loadする先のGCP ProjectID
	BQLoadDatasetID string // BQ Loadする先のDatasetID
}

// BQLoadJobPutMultiForm is Put する時のRequest内容
type BQLoadJobPutMultiForm struct {
	JobID           string
	Kinds           []string
	BQLoadProjectID string // BQ Loadする先のGCP ProjectID
	BQLoadDatasetID string // BQ Loadする先のDatasetID
}

// LoadKey is Entity Load時にKeyを設定する
func (e *BQLoadJob) LoadKey(ctx context.Context, k datastore.Key) error {
	e.ID = k.Name()

	return nil
}

// Load is Entity Load時に呼ばれる
func (e *BQLoadJob) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, e, ps)
	if err != nil {
		return err
	}

	return nil
}

// Save is Entity Save時に呼ばれる
func (e *BQLoadJob) Save(ctx context.Context) ([]datastore.Property, error) {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	e.UpdatedAt = time.Now()
	e.SchemaVersion = 1

	return datastore.SaveStruct(ctx, e)
}

func (store *BQLoadJobStore) NewKey(ctx context.Context, jobID string, kind string) datastore.Key {
	return store.ds.NameKey("BQLoadJob", fmt.Sprintf("%s-_-%s", jobID, kind), nil)
}

func (store *BQLoadJobStore) Put(ctx context.Context, form *BQLoadJobPutForm) (*BQLoadJob, error) {
	e := BQLoadJob{
		JobID:           form.JobID,
		Kind:            form.Kind,
		Status:          BQLoadJobStatusDefault,
		BQLoadProjectID: form.BQLoadProjectID,
		BQLoadDatasetID: form.BQLoadDatasetID,
		ChangeStatusAt:  time.Now(),
	}
	key, err := store.ds.Put(ctx, store.NewKey(ctx, e.JobID, e.Kind), &e)
	if err != nil {
		return nil, failure.Wrap(err)
	}
	e.ID = key.Name()
	return &e, nil
}

func (store *BQLoadJobStore) PutMulti(ctx context.Context, form *BQLoadJobPutMultiForm) ([]*BQLoadJob, error) {
	var keys []datastore.Key
	var entities []*BQLoadJob

	now := time.Now()
	for _, kind := range form.Kinds {
		k := store.NewKey(ctx, form.JobID, kind)
		e := BQLoadJob{
			ID:              k.Name(),
			JobID:           form.JobID,
			Kind:            kind,
			Status:          BQLoadJobStatusDefault,
			BQLoadProjectID: form.BQLoadProjectID,
			BQLoadDatasetID: form.BQLoadDatasetID,
			ChangeStatusAt:  now,
		}
		keys = append(keys, k)
		entities = append(entities, &e)
	}

	_, err := store.ds.PutMulti(ctx, keys, entities)
	if err != nil {
		return nil, failure.Wrap(err)
	}
	return entities, nil
}

func (store *BQLoadJobStore) Get(ctx context.Context, jobID string, kind string) (*BQLoadJob, error) {
	var e BQLoadJob
	err := store.ds.Get(ctx, store.NewKey(ctx, jobID, kind), &e)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.Get() jobID=%s,kind=%s", jobID, kind))
	}

	return &e, nil
}

func (store *BQLoadJobStore) StartLoadJob(ctx context.Context, ds2bqJobID string, kind string, bqLoadJobID string) (*BQLoadJob, error) {
	key := store.NewKey(ctx, ds2bqJobID, kind)
	var e BQLoadJob
	_, err := store.ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		if err := tx.Get(key, &e); err != nil {
			return err
		}

		e.BQLoadJobID = bqLoadJobID
		e.Status = BQLoadJobStatusRunning
		e.ChangeStatusAt = time.Now()

		_, err := tx.Put(key, &e)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v,kind=%v,bqLoadJobID=%v", ds2bqJobID, kind, bqLoadJobID))
	}
	return &e, nil
}

func (store *BQLoadJobStore) IncrementJobStatusCheckCount(ctx context.Context, ds2bqJobID string, kind string) (*BQLoadJob, error) {
	key := store.NewKey(ctx, ds2bqJobID, kind)
	var e BQLoadJob
	_, err := store.ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		if err := tx.Get(key, &e); err != nil {
			return err
		}
		e.StatusCheckCount++
		_, err := tx.Put(key, &e)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v, kind=%v", ds2bqJobID, kind))
	}
	return &e, nil
}

func (store *BQLoadJobStore) FinishExportJob(ctx context.Context, ds2bqJobID string, kind string, status BQLoadJobStatus, message string) (*BQLoadJob, error) {
	key := store.NewKey(ctx, ds2bqJobID, kind)
	var e BQLoadJob
	_, err := store.ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		if err := tx.Get(key, &e); err != nil {
			return err
		}
		e.Status = status
		e.ChangeStatusAt = time.Now()
		e.BQLoadResponseMessage = message
		_, err := tx.Put(key, &e)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v, kind=%v", ds2bqJobID, kind))
	}
	return &e, nil
}

func (store *BQLoadJobStore) List(ctx context.Context, jobID string) ([]*BQLoadJob, error) {
	b := NewBQLoadJobQueryBuilder(store.ds)
	b.JobID.Equal(jobID)

	var l []*BQLoadJob
	if _, err := store.ds.GetAll(ctx, b.Query(), &l); err != nil {
		_, ok := err.(datastore.MultiError)
		if ok {
			return l, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.GetAll() jobID=%v", jobID))
	}

	return l, nil
}
