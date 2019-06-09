package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/api/v1/datastore-export", HandleDatastoreExportAPI)
	http.HandleFunc("/", HandleHealthCheck)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

func HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	msg := "Hello ds2bq"
	log.Print(msg)
	_, err := fmt.Fprintf(w, msg)
	if err != nil {
		log.Print(err)
	}
}
