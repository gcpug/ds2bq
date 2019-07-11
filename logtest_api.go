package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	slog "github.com/sinmetal/slog/v2"
)

func HandleLogTestAPI(w http.ResponseWriter, r *http.Request) {
	ctx := slog.WithValue(r.Context())
	defer slog.Flush(ctx)

	for k, v := range r.Header {
		slog.Info(ctx, slog.KV{k, v})
	}

	lc, ok := slog.Value(ctx)
	if !ok {
		slog.Info(ctx, slog.KV{"MSG", "slog.Value is ng"})
	}
	lc.Entry.Severity = "INFO"
	lc.Entry.HttpRequest.RequestURL = r.RequestURI
	j, err := json.Marshal(lc)
	if err != nil {
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("failed json.Marshal", err)})
	}

	w.Header().Set("Content-type", "application/json;charset=utf-8")
	_, err = w.Write(j)
	if err != nil {
		slog.Info(ctx, slog.KV{"MSG", fmt.Sprintf("failed json.Marshal", err)})
	}
}
