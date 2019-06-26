package main

import (
	"context"
	"fmt"

	"log"
	"net/http"
	"os"

	"cloud.google.com/go/cloudtasks/apiv2beta3"
	ds "cloud.google.com/go/datastore"
	"github.com/sinmetal/gcpmetadata"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

var ServiceAccountEmail string
var ProjectID string
var TasksClient *cloudtasks.Client
var DatastoreClient datastore.Client

func main() {
	http.HandleFunc("/api/v1/datastore-export-job-check/", HandleDatastoreExportJobCheckAPI)
	http.HandleFunc("/api/v1/datastore-export/", HandleDatastoreExportAPI)
	http.HandleFunc("/", HandleHealthCheck)

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
		log.Fatalf("failed ProjectID.err=%+v\n", err)
		os.Exit(1)
	}
	ProjectID = projectID
	log.Printf("ProjectID is %s\n", projectID)

	sa, err := gcpmetadata.GetServiceAccountEmail()
	if err != nil {
		log.Fatalf("failed get ServiceAccountEmail.err=%+v\n", err)
		os.Exit(1)
	}
	ServiceAccountEmail = sa

	{
		TasksClient, err = cloudtasks.NewClient(ctx)
		if err != nil {
			log.Fatalf("failed cloudtasks.NewClient.err=%+v", err)
		}
	}
	{
		client, err := ds.NewClient(ctx, ProjectID)
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
