package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/morikuni/failure"
)

type JobStatus int

const (
	StateUnspecified JobStatus = iota
	Running
	Fail
	Done
)

type JobStatusResponse struct {
	Status     JobStatus
	ErrMessage string
}

func Load(ctx context.Context, projectID string, sourceGCSUri string, dstDataset string, dstTable string) (string, error) {
	bq, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return "", failure.Wrap(err, failure.Messagef("ProjectID:%v", projectID))
	}
	ref := bigquery.NewGCSReference(sourceGCSUri)
	ref.SourceFormat = bigquery.DatastoreBackup
	l := bq.Dataset(dstDataset).Table(dstTable).LoaderFrom(
		ref,
	)
	l.WriteDisposition = bigquery.WriteTruncate
	job, err := l.Run(ctx)
	if err != nil {
		return "", failure.Wrap(err, failure.Messagef("ProjectID:%v,SourceGCSUri:%v,Dataset:%v,Table:%v", projectID, sourceGCSUri, dstDataset, dstTable))
	}
	return job.ID(), nil
}

func CheckJobStatus(ctx context.Context, projectID string, bqloadJobID string) (res *JobStatusResponse, rerr error) {
	bq, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, failure.Wrap(err, failure.Messagef("ProjectID:%v", projectID))
	}
	defer func() {
		if err := bq.Close(); err != nil {
			rerr = failure.Wrap(err, failure.Messagef("failed bq.Client.Close. projectID=%s", projectID))
		}
	}()

	job, err := bq.JobFromID(ctx, bqloadJobID)
	if err != nil {
		return nil, failure.Wrap(err, failure.Messagef("BQLoadJobID=%s", bqloadJobID))
	}
	if !job.LastStatus().Done() {
		return &JobStatusResponse{Running, ""}, nil
	}
	switch job.LastStatus().State {
	case bigquery.Done:
		return &JobStatusResponse{Done, ""}, nil
	default:
		return &JobStatusResponse{Fail, fmt.Sprintf("%+v", job.LastStatus().Errors)}, nil
	}
}
