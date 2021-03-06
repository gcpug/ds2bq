package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/cloudtasks/apiv2beta3"
	ds "cloud.google.com/go/datastore"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/sinmetal/gcpmetadata"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
	"go.opencensus.io/exporter/stackdriver/propagation"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var ServiceAccountEmail string
var ProjectID string
var TasksClient *cloudtasks.Client
var DatastoreClient datastore.Client

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/bigquery-load-job-check/", HandleBQLoadJobCheckAPI)
	mux.HandleFunc("/api/v1/datastore-export-job-check/", HandleDatastoreExportJobCheckAPI)
	mux.HandleFunc("/api/v1/datastore-export/", HandleDatastoreExportAPI)
	mux.HandleFunc("/", HandleHealthCheck)

	http.Handle("/", &ochttp.Handler{
		Propagation: &propagation.HTTPFormat{},
		Handler:     mux,
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

func init() {
	ctx := context.Background()

	projectID, err := gcpmetadata.GetProjectID()
	if err != nil {
		log.Fatalf("failed GetProjectID.err=%+v\n", err)
	}
	ProjectID = projectID
	log.Printf("ProjectID is %s\n", projectID)

	sa, err := gcpmetadata.GetServiceAccountEmail()
	if err != nil {
		log.Fatalf("failed get ServiceAccountEmail.err=%+v\n", err)
	}
	ServiceAccountEmail = sa

	if gcpmetadata.OnGCP() {
		exporter, err := stackdriver.NewExporter(stackdriver.Options{
			ProjectID: ProjectID,
		})
		if err != nil {
			log.Fatalf("failed stackdriver.NewExporter.err=%+v\n", err)
		}
		trace.RegisterExporter(exporter)
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	createClients(ctx)
}

func createClients(ctx context.Context) {
	ctx, span := trace.StartSpan(ctx, "CreateClients")
	defer span.End()

	var err error
	opts := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(grpc.WaitForReady(true))),
	}
	{
		TasksClient, err = cloudtasks.NewClient(ctx, opts...)
		if err != nil {
			log.Fatalf("failed cloudtasks.NewClient.err=%+v", err)
		}
	}
	{
		client, err := ds.NewClient(ctx, ProjectID, opts...)
		if err != nil {
			log.Fatalf("failed clouddatastore.NewClient.err=%+v", err)
		}
		DatastoreClient, err = clouddatastore.FromClient(ctx, client)
		if err != nil {
			log.Fatalf("failed clouddatastore.FromClient.err=%+v", err)
		}
	}
}

func HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	msg := "Hello ds2bq"
	log.Print(msg)
	_, err := fmt.Fprintf(w, msg)
	if err != nil {
		log.Print(err)
	}
}
