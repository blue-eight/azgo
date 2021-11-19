package arm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resources/armresources"
)

func DeployTemplateGroup(subscriptionID, resourceGroup, location string) error {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return err
	}
	con := arm.NewDefaultConnection(cred, nil)
	if err != nil {
		return err
	}

	// deploy an empty template to empty a resource group
	// TODO: add warning!
	armTemplateStr := `{
		"$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
		"contentVersion": "1.0.0.0",
		"resources": []
	}`
	armTemplate := map[string]interface{}{}

	err = json.Unmarshal([]byte(armTemplateStr), &armTemplate)
	if err != nil {
		return err
	}

	client := armresources.NewDeploymentsClient(con, subscriptionID)
	deployment := armresources.Deployment{}
	deployment.Location = &location
	deployment.Properties.Template = armTemplate
	options := &armresources.DeploymentsBeginCreateOrUpdateOptions{}

	ctx := context.Background()
	deploymentName := resourceGroup

	poller, err := client.BeginCreateOrUpdate(ctx, resourceGroup, deploymentName, deployment, options)

	if err != nil {
		return err
	}
	resp, err := poller.PollUntilDone(context.Background(), 5*time.Minute)
	if err != nil {
		return err
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", b)
	return nil
}

func DeployTemplateSubscription() error {
	return errors.New("not implemented")
}
