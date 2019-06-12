package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type RunEnvAPIResponse struct {
	Host   string
	Header map[string]interface{}
}

func HandleRunEnvAPI(w http.ResponseWriter, r *http.Request) {
	res := RunEnvAPIResponse{
		Host:   r.Host,
		Header: map[string]interface{}{},
	}

	for k, v := range r.Header {
		res.Header[k] = v
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("failed json.NewEncoder %+v\n", err)
	}
}
