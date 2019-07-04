package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/morikuni/failure"
)

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
