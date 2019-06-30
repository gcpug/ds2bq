package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

type PubSubStorageNotifyPayload struct {
	Message      PubSubStorageNotifyMessage `json:"message"`
	Subscription string                     `json:"subscription"`
}

type PubSubStorageNotifyPayloadRaw struct {
	Message      PubSubStorageNotifyMessageRaw `json:"message"`
	Subscription string                        `json:"subscription"`
}

type PubSubStorageNotifyMessageRaw struct {
	Attributes  PubSubStorageNotifyMessageAttributes `json:"attributes"`
	Data        string                               `json:"data"`
	MessageID   string                               `json:"messageId"`
	PublishTime time.Time                            `json:"publishTime"`
}

type PubSubStorageNotifyMessage struct {
	Attributes  PubSubStorageNotifyMessageAttributes `json:"attributes"`
	GCSObject   GCSObject                            `json:"gcsObject"`
	MessageID   string                               `json:"messageId"`
	PublishTime time.Time                            `json:"publishTime"`
}

type PubSubStorageNotifyMessageAttributes struct {
	BucketID           string    `json:"bucketId"`
	EventTime          time.Time `json:"eventTime"`
	EventType          string    `json:"eventType"`
	NotificationConfig string    `json:"notificationConfig"`
	ObjectGeneration   string    `json:"objectGeneration"`
	ObjectID           string    `json:"objectId"`
	PayloadFormat      string    `json:"payloadFormat"`
}

type GCSObject struct {
	Kind                    string    `json:"kind"`
	ID                      string    `json:"id"`
	SelfLink                string    `json:"selfLink"`
	Name                    string    `json:"name"`
	Bucket                  string    `json:"bucket"`
	Generation              int64     `json:"generation,string"`
	MetaGeneration          int64     `json:"metageneration,string"`
	ContentType             string    `json:"contentType"`
	Updated                 time.Time `json:"updated"`
	Size                    int64     `json:"size,string"`
	Md5Hash                 string    `json:"md5Hash"`
	MediaLink               string    `json:"mediaLink"`
	Crc32c                  string    `json:"crc32c"`
	Etag                    string    `json:"etag"`
	StorageClass            string    `json:"storageClass"`
	TimeCreated             time.Time `json:"timeCreated"`
	TimeDeleted             time.Time `json:"timeDeleted"`
	TimeStorageClassUpdated time.Time `json:"timeStorageClassUpdated"`
}

func EncodePayload(payload []byte) (*PubSubStorageNotifyPayload, error) {
	var raw PubSubStorageNotifyPayloadRaw
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(raw.Message.Data)
	if err != nil {
		return nil, err
	}
	var o GCSObject
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, err
	}

	return &PubSubStorageNotifyPayload{
		Subscription: raw.Subscription,
		Message: PubSubStorageNotifyMessage{
			Attributes:  raw.Message.Attributes,
			MessageID:   raw.Message.MessageID,
			PublishTime: raw.Message.PublishTime,
			GCSObject:   o,
		},
	}, nil
}

func EncodePayloadPull(payload []byte) (*GCSObject, error) {
	var raw GCSObject
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}

	return &raw, nil
}

func IsDatastoreExportMetadataFile(objectID string) bool {
	return strings.HasSuffix(objectID, ".export_metadata")
}

func SearchKindName(objectID string) (string, bool) {
	if IsDatastoreExportMetadataFile(objectID) == false {
		return "", false
	}

	prefixs := []string{"/default_namespace_kind_", "/all_namespaces_kind_"}
	const suffix = ".export_metadata"
	for _, prefix := range prefixs {
		index := strings.Index(objectID, prefix)
		if index < 0 {
			continue
		}
		v := objectID[index:len(objectID)]

		v = strings.TrimPrefix(v, prefix)
		return strings.TrimSuffix(v, suffix), true
	}

	return "", false
}
