package main

import "testing"

func TestEncodePayload(t *testing.T) {
	inputPayload := `{"message":{"attributes":{"bucketId":"datastore-backup-gcpugjp-dev","eventTime":"2019-06-27T04:53:44.281168Z","eventType":"OBJECT_FINALIZE","notificationConfig":"projects/_/buckets/datastore-backup-gcpugjp-dev/notificationConfigs/2","objectGeneration":"1561611224281438","objectId":"2019-06-27T04:53:23_95496/all_namespaces/kind_BQLoadJob/output-6","payloadFormat":"JSON_API_V1"},"data":"ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0IiwKICAiaWQiOiAiZGF0YXN0b3JlLWJhY2t1cC1nY3B1Z2pwLWRldi8yMDE5LTA2LTI3VDA0OjUzOjIzXzk1NDk2L2FsbF9uYW1lc3BhY2VzL2tpbmRfQlFMb2FkSm9iL291dHB1dC02LzE1NjE2MTEyMjQyODE0MzgiLAogICJzZWxmTGluayI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9zdG9yYWdlL3YxL2IvZGF0YXN0b3JlLWJhY2t1cC1nY3B1Z2pwLWRldi9vLzIwMTktMDYtMjdUMDQ6NTM6MjNfOTU0OTYlMkZhbGxfbmFtZXNwYWNlcyUyRmtpbmRfQlFMb2FkSm9iJTJGb3V0cHV0LTYiLAogICJuYW1lIjogIjIwMTktMDYtMjdUMDQ6NTM6MjNfOTU0OTYvYWxsX25hbWVzcGFjZXMva2luZF9CUUxvYWRKb2Ivb3V0cHV0LTYiLAogICJidWNrZXQiOiAiZGF0YXN0b3JlLWJhY2t1cC1nY3B1Z2pwLWRldiIsCiAgImdlbmVyYXRpb24iOiAiMTU2MTYxMTIyNDI4MTQzOCIsCiAgIm1ldGFnZW5lcmF0aW9uIjogIjEiLAogICJ0aW1lQ3JlYXRlZCI6ICIyMDE5LTA2LTI3VDA0OjUzOjQ0LjI4MVoiLAogICJ1cGRhdGVkIjogIjIwMTktMDYtMjdUMDQ6NTM6NDQuMjgxWiIsCiAgInN0b3JhZ2VDbGFzcyI6ICJSRUdJT05BTCIsCiAgInRpbWVTdG9yYWdlQ2xhc3NVcGRhdGVkIjogIjIwMTktMDYtMjdUMDQ6NTM6NDQuMjgxWiIsCiAgInNpemUiOiAiMzI3NjgiLAogICJtZDVIYXNoIjogIlNmVkRoSFU3azFyZ3dSRkNsV09WTlE9PSIsCiAgIm1lZGlhTGluayI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9kb3dubG9hZC9zdG9yYWdlL3YxL2IvZGF0YXN0b3JlLWJhY2t1cC1nY3B1Z2pwLWRldi9vLzIwMTktMDYtMjdUMDQ6NTM6MjNfOTU0OTYlMkZhbGxfbmFtZXNwYWNlcyUyRmtpbmRfQlFMb2FkSm9iJTJGb3V0cHV0LTY/Z2VuZXJhdGlvbj0xNTYxNjExMjI0MjgxNDM4JmFsdD1tZWRpYSIsCiAgImNyYzMyYyI6ICJRZm1hRkE9PSIsCiAgImV0YWciOiAiQ043aXM1THZpT01DRUFFPSIKfQo=","messageId":"640718152617003","message_id":"640718152617003","publishTime":"2019-06-27T04:53:44.331Z","publish_time":"2019-06-27T04:53:44.331Z"},"subscription":"projects/gcpugjp-dev/subscriptions/gcpug-ds2bq-ds-export-object-change-handler"}`

	body, err := EncodePayload([]byte(inputPayload))
	if err != nil {
		t.Fatal(err)
	}

	if e, g := "projects/gcpugjp-dev/subscriptions/gcpug-ds2bq-ds-export-object-change-handler", body.Subscription; e != g {
		t.Errorf("Subscription want %v but got %v", e, g)
	}

	if e, g := "640718152617003", body.Message.MessageID; e != g {
		t.Errorf("Message.MessageID want %v but got %v", e, g)
	}
	if body.Message.PublishTime.IsZero() {
		t.Error("Message.PublishTime is zero")
	}

	if e, g := "datastore-backup-gcpugjp-dev", body.Message.Attributes.BucketID; e != g {
		t.Errorf("Message.Attributes.BucketID want %v but got %v", e, g)
	}
	if body.Message.Attributes.EventTime.IsZero() {
		t.Error("Message.Attributes.EventTime is zero")
	}
	if e, g := "OBJECT_FINALIZE", body.Message.Attributes.EventType; e != g {
		t.Errorf("Message.Attributes.EventType want %v but got %v", e, g)
	}
	if e, g := "projects/_/buckets/datastore-backup-gcpugjp-dev/notificationConfigs/2", body.Message.Attributes.NotificationConfig; e != g {
		t.Errorf("Message.Attributes.NotificationConfig want %v but got %v", e, g)
	}
	if e, g := "1561611224281438", body.Message.Attributes.ObjectGeneration; e != g {
		t.Errorf("Message.Attributes.ObjectGeneration want %v but got %v", e, g)
	}
	if e, g := "2019-06-27T04:53:23_95496/all_namespaces/kind_BQLoadJob/output-6", body.Message.Attributes.ObjectID; e != g {
		t.Errorf("Message.Attributes.ObjectID want %v but got %v", e, g)
	}
	if e, g := "JSON_API_V1", body.Message.Attributes.PayloadFormat; e != g {
		t.Errorf("Message.Attributes.PayloadFormat want %v but got %v", e, g)
	}

	if e, g := "storage#object", body.Message.GCSObject.Kind; e != g {
		t.Errorf("Message.GCSObject.Kind want %v but got %v", e, g)
	}
	if e, g := "datastore-backup-gcpugjp-dev/2019-06-27T04:53:23_95496/all_namespaces/kind_BQLoadJob/output-6/1561611224281438", body.Message.GCSObject.ID; e != g {
		t.Errorf("Message.GCSObject.ID want %v but got %v", e, g)
	}
	if e, g := "https://www.googleapis.com/storage/v1/b/datastore-backup-gcpugjp-dev/o/2019-06-27T04:53:23_95496%2Fall_namespaces%2Fkind_BQLoadJob%2Foutput-6", body.Message.GCSObject.SelfLink; e != g {
		t.Errorf("Message.GCSObject.SelfLink want %v but got %v", e, g)
	}
	if e, g := "2019-06-27T04:53:23_95496/all_namespaces/kind_BQLoadJob/output-6", body.Message.GCSObject.Name; e != g {
		t.Errorf("Message.GCSObject.Name want %v but got %v", e, g)
	}
	if e, g := "datastore-backup-gcpugjp-dev", body.Message.GCSObject.Bucket; e != g {
		t.Errorf("Message.GCSObject.Bucket want %v but got %v", e, g)
	}
	if e, g := int64(1561611224281438), body.Message.GCSObject.Generation; e != g {
		t.Errorf("Message.GCSObject.Generation want %v but got %v", e, g)
	}
	if e, g := int64(1), body.Message.GCSObject.MetaGeneration; e != g {
		t.Errorf("Message.GCSObject.MetaGeneration want %v but got %v", e, g)
	}
	if e, g := "REGIONAL", body.Message.GCSObject.StorageClass; e != g {
		t.Errorf("Message.GCSObject.StorageClass want %v but got %v", e, g)
	}
	if e, g := int64(32768), body.Message.GCSObject.Size; e != g {
		t.Errorf("Message.GCSObject.Size want %v but got %v", e, g)
	}
	if e, g := "SfVDhHU7k1rgwRFClWOVNQ==", body.Message.GCSObject.Md5Hash; e != g {
		t.Errorf("Message.GCSObject.Md5Hash want %v but got %v", e, g)
	}
	if e, g := "https://www.googleapis.com/download/storage/v1/b/datastore-backup-gcpugjp-dev/o/2019-06-27T04:53:23_95496%2Fall_namespaces%2Fkind_BQLoadJob%2Foutput-6?generation=1561611224281438&alt=media", body.Message.GCSObject.MediaLink; e != g {
		t.Errorf("Message.GCSObject.MediaLink want %v but got %v", e, g)
	}
	if e, g := "QfmaFA==", body.Message.GCSObject.Crc32c; e != g {
		t.Errorf("Message.GCSObject.Crc32c want %v but got %v", e, g)
	}
	if e, g := "CN7is5LviOMCEAE=", body.Message.GCSObject.Etag; e != g {
		t.Errorf("Message.GCSObject.Etag want %v but got %v", e, g)
	}
	if body.Message.GCSObject.TimeCreated.IsZero() {
		t.Error("Message.GCSObject.TimeCreated is zero")
	}
	if body.Message.GCSObject.TimeStorageClassUpdated.IsZero() {
		t.Error("Message.GCSObject.TimeStorageClassUpdated is zero")
	}
	if body.Message.GCSObject.Updated.IsZero() {
		t.Error("Message.GCSObject.Updated is zero")
	}
}

func TestIsDatastoreExportMetadataFile(t *testing.T) {
	cases := []struct {
		name     string
		objectID string
		want     bool
	}{
		{"empty",
			"",
			false,
		},
		{"not export metadata file",
			"2019-06-27T04:53:23_95496/all_namespaces/kind_DatastoreSample/output-3",
			false,
		},
		{"export metadata file",
			"2019-06-27T10:24:38_6984/all_namespaces/kind_BQLoadJob/all_namespaces_kind_BQLoadJob.export_metadata",
			true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := IsDatastoreExportMetadataFile(tt.objectID)
			if got != tt.want {
				t.Errorf("want %v but got %v", tt.want, got)
			}
		})
	}
}

func TestSearchKindName(t *testing.T) {
	cases := []struct {
		name     string
		objectID string
		wantOK   bool
		wantName string
	}{
		{"empty",
			"",
			false,
			"",
		},
		{"not export metadata file",
			"2019-06-27T04:53:23_95496/all_namespaces/kind_DatastoreSample/output-3",
			false,
			"",
		},
		{"export root metadata file",
			"2019-06-17T23:05:01_26856/all_namespaces/all_kinds/all_namespaces_all_kinds.export_metadata",
			false,
			"",
		},

		{"export metadata file. default_namespaces",
			"2019/06/30 20:52:50 2019-06-30T11:52:30_53384/default_namespace/kind_PugEvent/default_namespace_kind_PugEvent.export_metadata",
			true,
			"PugEvent",
		},
		{"export metadata file. all_namespaces",
			"2019-06-27T10:24:38_6984/all_namespaces/kind_BQLoadJob/all_namespaces_kind_BQLoadJob.export_metadata",
			true,
			"BQLoadJob",
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			kind, ok := SearchKindName(tt.objectID)
			if ok != tt.wantOK {
				t.Errorf("want OK %v but got %v", tt.wantOK, ok)
			}
			if kind != tt.wantName {
				t.Errorf("want Kind %v but got %v", tt.wantName, kind)
			}
		})
	}
}
