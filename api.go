package main

import (
	"fmt"
	"log"
	"net/http"
)

func WriteError(w http.ResponseWriter, statusCode int, message string, err error) {
	msg := fmt.Sprintln(message, " : ", err)
	log.Println(msg)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(msg))
	if err != nil {
		log.Println(err)
	}
}
