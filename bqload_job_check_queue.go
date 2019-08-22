package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/cloudtasks/apiv2beta3"
	"github.com/morikuni/failure"
	"github.com/sinmetal/gcpmetadata"
	"go.opencensus.io/trace"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BQLoadJobCheckQueue struct {
	queueName string
	targetURL string
	tasks     *cloudtasks.Client
}

func NewBQLoadJobCheckQueue(host string, tasks *cloudtasks.Client) (*BQLoadJobCheckQueue, error) {
	qn := os.Getenv("BIGQUERY_LOAD_JOB_CHECK_QUEUE_NAME")
	if len(qn) < 1 {
		region, err := gcpmetadata.GetRegion()
		if err != nil {
			return nil, errors.New("failed get instance region")
		}

		qn = fmt.Sprintf("projects/%s/locations/%s/queues/gcpug-ds2bq-bigquery-job-check", ProjectID, region)
	}

	return &BQLoadJobCheckQueue{
		tasks:     tasks,
		queueName: qn,
		targetURL: fmt.Sprintf("https://%s/api/v1/bigquery-load-job-check/", host),
	}, nil
}

func (q *BQLoadJobCheckQueue) AddTask(ctx context.Context, body *BQLoadJobCheckRequest) error {
	// TODO いずれはMockを作ったりしたい
	if !gcpmetadata.OnGCP() {
		return nil
	}
	ctx, span := trace.StartSpan(ctx, "BQLoadJobCheckQueue.AddTask")
	defer span.End()

	message, err := json.Marshal(body)
	if err != nil {
		return failure.Wrap(err, failure.Messagef("failed json.Marshal. body=%+v\n", body))
	}

	req := &taskspb.CreateTaskRequest{
		Parent: q.queueName,
		Task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					HttpMethod: taskspb.HttpMethod_POST,
					Url:        q.targetURL,
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: ServiceAccountEmail,
						},
					},
				},
			},
		},
	}
	req.Task.GetHttpRequest().Body = []byte(message)

	var retryCount int
	for {
		_, err = q.tasks.CreateTask(ctx, req)
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				retryCount++
				if retryCount > 5 {
					return failure.Wrap(err, failure.Messagef("failed cloudtasks.CreateTask. body=%+v\n", body))
				}
				log.Printf("failed cloudtasks.CreateTask. body=%+v, retryCount=%v\n", body, retryCount)
				continue
			}
			return failure.Wrap(err, failure.Messagef("failed cloudtasks.CreateTask. body=%+v\n", body))
		}
		break
	}

	return nil
}
