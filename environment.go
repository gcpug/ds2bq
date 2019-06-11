package main

import (
	"fmt"
)

func ServiceAccountEmail() string {
	return fmt.Sprintf("%s@appspot.gserviceaccount.com", ProjectID)
}
