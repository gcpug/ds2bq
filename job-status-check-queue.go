package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.opencensus.io/trace"
	"log"
	"os"

	"cloud.google.com/go/cloudtasks/apiv2beta3"
	"github.com/morikuni/failure"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobStatusCheckQueue struct {
	queueName string
	targetURL string
	tasks     *cloudtasks.Client
}

func NewJobStatusCheckQueue(host string, tasks *cloudtasks.Client) (*JobStatusCheckQueue, error) {
	// TODO Cloud RunのTokyo Regionが来たら、Runと同じProject, Locationにあるという前提にしてしまってもいいかも
	qn := os.Getenv("JOB_STATUS_CHECK_QUEUE_NAME")
	if len(qn) < 1 {
		return nil, errors.New("required JOB_STATUS_CHECK_QUEUE_NAME variable")
	}

	return &JobStatusCheckQueue{
		tasks:     tasks,
		queueName: qn,
		targetURL: fmt.Sprintf("https://%s/api/v1/datastore-export-job-check/", host),
	}, nil
}

func (q *JobStatusCheckQueue) AddTask(ctx context.Context, body *DatastoreExportJobCheckRequest) error {
	ctx, span := trace.StartSpan(ctx, "JobStatusCheckQueue.AddTask")
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
