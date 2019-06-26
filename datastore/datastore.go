package datastore

import (
	"context"
	"strings"

	cds "cloud.google.com/go/datastore"
	"github.com/morikuni/failure"
	"google.golang.org/api/datastore/v1"
	"google.golang.org/api/iterator"
)

// https://cloud.google.com/datastore/docs/export-import-entities

// EntityFilter is Entity condition to export
type EntityFilter struct {
	Kinds           []string `json:"kinds,omitempty"`
	NamespaceIds    []string `json:"namespaceIds,omitempty"`
	ForceSendFields []string `json:"-"`
	NullFields      []string `json:"-"`
}

// Export is Datastore Export APIを実行する
func Export(ctx context.Context, projectID string, outputGCSPrefix string, entityFilter *EntityFilter) (*datastore.GoogleLongrunningOperation, error) {
	service, err := datastore.NewService(ctx)
	if err != nil {
		return nil, failure.Wrap(err, failure.Message("failed datastore.New()."))
	}

	ope, err := service.Projects.Export(projectID, &datastore.GoogleDatastoreAdminV1ExportEntitiesRequest{
		EntityFilter: &datastore.GoogleDatastoreAdminV1EntityFilter{
			Kinds:           entityFilter.Kinds,
			NamespaceIds:    entityFilter.NamespaceIds,
			ForceSendFields: entityFilter.ForceSendFields,
			NullFields:      entityFilter.NullFields,
		},
		OutputUrlPrefix: outputGCSPrefix,
	}).Do()
	if err != nil {
		return nil, failure.Wrap(err, failure.Message("failed Datastore Export API."))
	}
	return ope, nil
}

// JobStatus is Datastore Export Jobの状態
type JobStatus int

const (
	Running JobStatus = iota
	Fail
	Done
)

// JobStatusResponse is Datastore Export Jobの状態の取得結果を表すstruct
type JobStatusResponse struct {
	Status     JobStatus
	ErrCode    int64
	ErrMessage string
}

// CheckJobStatus is Datastore Export Jobの状態を取得する
func CheckJobStatus(ctx context.Context, jobID string) (*JobStatusResponse, error) {
	service, err := datastore.NewService(ctx)
	if err != nil {
		return nil, failure.Wrap(err, failure.Message("failed datastore.New()."))
	}

	ope, err := service.Projects.Operations.Get(jobID).Do()
	if err != nil {
		return nil, failure.Wrap(err, failure.Message("failed Operations.Get()."))
	}

	if err != nil {
		return nil, failure.Wrap(err, failure.Message("failed Datastore Export API."))
	}
	if ope.Done == false {
		return &JobStatusResponse{Running, 0, ""}, nil
	}
	if ope.Error != nil {
		return &JobStatusResponse{Fail, ope.Error.Code, ope.Error.Message}, nil
	}
	return &JobStatusResponse{Done, 0, ""}, nil
}

// GetAllKinds is Kind名一覧を返す
// ただし、 _ で始まるものは無視する
func GetAllKinds(ctx context.Context, projectID string) (kinds []string, rerr error) {
	client, err := cds.NewClient(ctx, projectID)
	if err != nil {
		return nil, failure.Wrap(err, failure.Messagef("failed Datastore.NewClient. projectID=%s", projectID))
	}
	defer func() {
		if err := client.Close(); err != nil {
			rerr = failure.Wrap(err, failure.Messagef("failed Datastore.Client.Close. projectID=%s", projectID))
		}
	}()

	q := cds.NewQuery("__kind__").KeysOnly()
	t := client.Run(ctx, q)
	for {
		key, err := t.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(key.Name, "_") {
			continue
		}
		kinds = append(kinds, key.Name)
	}
	return kinds, nil
}
