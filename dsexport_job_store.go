//go:generate qbg -usedatastorewrapper -output model_query.go .

package main

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/morikuni/failure"
	"go.mercari.io/datastore"
)

type DSExportJobStore struct {
	ds datastore.Client
}

func NewDSExportJobStore(ctx context.Context, client datastore.Client) (*DSExportJobStore, error) {
	return &DSExportJobStore{
		ds: client,
	}, nil
}

type DSExportJobStatus int

const (
	DSExportJobStatusDefault DSExportJobStatus = iota
	DSExportJobStatusRunning
	DSExportJobStatusFailed
	DSExportJobStatusDone
)

type DSExportJob struct {
	ID                      string `datastore:"-"`
	DSExportJobID           string
	JobRequestBody          string   `datastore:",noindex"`
	ExportKinds             []string `datastore:",noindex"`
	StatusCheckCount        int
	Status                  DSExportJobStatus
	ChangeStatusAt          time.Time
	DSExportResponseMessage string `datastore:",noindex"`
	CreatedAt               time.Time
	UpdatedAt               time.Time
	SchemaVersion           int
}

var _ datastore.PropertyLoadSaver = &DSExportJob{}
var _ datastore.KeyLoader = &DSExportJob{}

// LoadKey is Entity Load時にKeyを設定する
func (e *DSExportJob) LoadKey(ctx context.Context, k datastore.Key) error {
	e.ID = k.Name()

	return nil
}

// Load is Entity Load時に呼ばれる
func (e *DSExportJob) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, e, ps)
	if err != nil {
		return err
	}

	return nil
}

// Save is Entity Save時に呼ばれる
func (e *DSExportJob) Save(ctx context.Context) ([]datastore.Property, error) {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	e.UpdatedAt = time.Now()
	e.SchemaVersion = 1

	return datastore.SaveStruct(ctx, e)
}

// NewJobID is JobIDを生成する
// JobIDは一度のDatastore Export, BQ Loadで一つ発行され、複数KindのExportが全て終わっているかを確認するためのID
func (store *DSExportJobStore) NewDS2BQJobID(ctx context.Context) string {
	return uuid.New().String()
}

func (store *DSExportJobStore) NewKey(ctx context.Context, ds2bqJobID string) datastore.Key {
	return store.ds.NameKey("DSExportJob", ds2bqJobID, nil)
}

func (store *DSExportJobStore) Create(ctx context.Context, ds2bqJobID string, body string, kinds []string) (*DSExportJob, error) {
	e := DSExportJob{
		ID:             ds2bqJobID,
		Status:         DSExportJobStatusDefault,
		JobRequestBody: body,
		ExportKinds:    kinds,
		ChangeStatusAt: time.Now(),
	}
	_, err := store.ds.Put(ctx, store.NewKey(ctx, ds2bqJobID), &e)
	if err != nil {
		return nil, failure.Wrap(err)
	}
	return &e, nil
}

func (store *DSExportJobStore) Get(ctx context.Context, ds2bqJobID string) (*DSExportJob, error) {
	var e DSExportJob
	err := store.ds.Get(ctx, store.NewKey(ctx, ds2bqJobID), &e)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, err
		}
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.Get() ds2bqJobID=%v", ds2bqJobID))
	}
	return &e, nil
}

func (store *DSExportJobStore) StartExportJob(ctx context.Context, ds2bqJobID string, dsExportJobID string) (*DSExportJob, error) {
	key := store.NewKey(ctx, ds2bqJobID)
	var e DSExportJob
	_, err := store.ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		if err := tx.Get(key, &e); err != nil {
			return err
		}
		e.DSExportJobID = dsExportJobID
		e.Status = DSExportJobStatusRunning
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
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v", ds2bqJobID))
	}
	return &e, nil
}

func (store *DSExportJobStore) IncrementJobStatusCheckCount(ctx context.Context, ds2bqJobID string) (*DSExportJob, error) {
	key := store.NewKey(ctx, ds2bqJobID)
	var e DSExportJob
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
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v", ds2bqJobID))
	}
	return &e, nil
}

func (store *DSExportJobStore) FinishExportJob(ctx context.Context, ds2bqJobID string, status DSExportJobStatus, message string) (*DSExportJob, error) {
	key := store.NewKey(ctx, ds2bqJobID)
	var e DSExportJob
	_, err := store.ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		if err := tx.Get(key, &e); err != nil {
			return err
		}
		e.Status = status
		e.ChangeStatusAt = time.Now()
		e.DSExportResponseMessage = message
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
		return nil, failure.Wrap(err, failure.Messagef("failed datastore.RunInTx() ds2bqJobID=%v", ds2bqJobID))
	}
	return &e, nil
}
