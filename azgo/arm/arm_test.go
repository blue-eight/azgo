package arm

import (
	"os"
	"testing"
)

func TestListResourcesWithPolicy(t *testing.T) {
	subscriptionID := os.Getenv("AZURE_SUBSCRIPTION")
	if subscriptionID == "" {
		t.Error("AZURE_SUBSCRIPTION environment variable not set!")
	}
	ListResourcesWithPolicy(subscriptionID)
}
