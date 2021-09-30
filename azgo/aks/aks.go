package aks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/containerservice/armcontainerservice"
)

// RunCommand runs a command on an AKS cluster
// specified via its subscription, resource group,
// and cluster name. The command is a string which
// is run inside a container on the cluster itself.
// This command authenticates against Azure via the
// DefaultAzureCredential (see: https://docs.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication?tabs=bash#3-use-defaultazurecredential-to-authenticate-resourceclient ) in the azidentity package.
func RunCommand(subscriptionID, resourceGroup, resourceName, command string) (string, error) {

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", err
	}

	con := arm.NewDefaultConnection(cred, nil)
	if err != nil {
		return "", err
	}

	client := armcontainerservice.NewManagedClustersClient(con, subscriptionID)

	ctx := context.Background()
	request := armcontainerservice.RunCommandRequest{}
	request.Command = &command

	result, err := client.BeginRunCommand(ctx, resourceGroup, resourceName, request, nil)
	if err != nil {
		return "", err
	}
	res, err := result.PollUntilDone(ctx, 5*time.Second)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(res.RunCommandResult)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
