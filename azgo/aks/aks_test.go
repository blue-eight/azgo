package aks

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

func TestRunCommand(t *testing.T) {
	resourceGroup := os.Getenv("RESOURCE_GROUP")
	if resourceGroup == "" {
		log.Fatal("RESOURCE_GROUP not set.")
	}
	clusterName := os.Getenv("AKS_NAME")
	if clusterName == "" {
		log.Fatal("AKS_NAME not set")
	}
	command := "kubectl run nginx --image=nginx"
	res, err := RunCommand(subscriptionID, resourceGroup, clusterName, command)
	log.Println(res)
	if err != nil {
		t.Error(err)
	}
	_ = res
}
