package arm

import (
	"log"
	"os"
	"testing"
)

var subscriptionID string

func init() {
	subscriptionID = os.Getenv("AZURE_SUBSCRIPTION")
	if subscriptionID == "" {
		log.Fatal("AZURE_SUBSCRIPTION environment variable not set!")
	}
}
func TestListResourcesWithPolicy(t *testing.T) {
	ListResourcesWithPolicy(subscriptionID)
}
