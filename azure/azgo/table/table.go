package table

import (
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/tables/aztable"
)

func TableClient() (*aztable.TableServiceClient, error) {
	tableAccount := os.Getenv("AZGO_TABLE_ACCOUNT")
	tableKey := os.Getenv("AZGO_TABLE_KEY")
	credential, err := aztable.NewSharedKeyCredential(tableAccount, tableKey)
	if err != nil {
		return nil, err
	}

	serviceURL := fmt.Sprintf("https://%s.table.core.windows.net/", tableAccount)
	tableClientOptions := &aztable.TableClientOptions{}
	serviceClient, err := aztable.NewTableServiceClient(serviceURL, credential, tableClientOptions)
	if err != nil {
		return nil, err
	}
	return serviceClient, nil
}

func ListTables() error {
	client, err := TableClient()
	if err != nil {
		return err
	}
	pager := client.Query(nil)
	ctx := context.TODO()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, item := range resp.TableQueryResponse.Value {
			fmt.Printf("TableName: %s\n", *item.TableName)
		}
	}
	return nil
}
