package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// HandleStorageChangeNotifyAPI is Cloud Pub/Sub Notifications for Cloud Storage を受け取るハンドラ
func HandleStorageChangeNotifyAPI(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("failed ioutil.Read(request.Body).err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}
	log.Printf("BASE64 BODY:%s\n", string(b))

	dst, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		msg := fmt.Sprintf("failed base64.StdEncoding.Decode.err=%+v", err)
		log.Println(msg)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Println(err)
		}
		return
	}

	log.Printf("JSON BODY:%s\n", string(dst))
}
