package table

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/tables/aztable"
	"github.com/google/uuid"
)

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Require environment variable: %s\n", key)
	}
	return value
}

// TableClientFromEnv creates an *aztable.TableServiceClient authenticated
// by the environment variables AZGO_TABLE_ACCOUNT and AZGO_TABLE_KEY.
// This uses Cosmos DB by default, but also switches to Storage Account
// if the optional environment variable AZGO_TABLE_TYPE="storage"
func TableClientFromEnv() (*aztable.TableServiceClient, error) {
	tableAccount := mustGetEnv("AZGO_TABLE_ACCOUNT")
	tableKey := mustGetEnv("AZGO_TABLE_KEY")
	tableType := os.Getenv("AZGO_TABLE_TYPE")

	credential, err := aztable.NewSharedKeyCredential(tableAccount, tableKey)
	if err != nil {
		return nil, err
	}
	accountEndpoint := "https://%s.table.cosmos.azure.com/"
	if tableType == "storage" {
		accountEndpoint = "https://%s.table.core.windows.net/"
	}

	serviceURL := fmt.Sprintf(accountEndpoint, tableAccount)
	tableClientOptions := &aztable.TableClientOptions{}
	serviceClient, err := aztable.NewTableServiceClient(serviceURL, credential, tableClientOptions)
	if err != nil {
		return nil, err
	}
	return serviceClient, nil
}

// ListTables lists all the tables in the account and prints them to
// the standard output. It does this without supplying any filter
// to the Query() function.
func ListTables() error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	pager := client.Query(nil)
	ctx := context.Background()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, item := range resp.TableQueryResponse.Value {
			map1 := map[string]interface{}{
				"name": *item.TableName,
			}
			b, err := json.Marshal(map1)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}

// CreateTable creates a table in the account
func CreateTable(name string) error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = client.Create(ctx, name)
	return err
}

// DeleteTable deletes a table from the account
func DeleteTable(name string) error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = client.Delete(ctx, name)
	return err
}

// InsertKeyValue inserts an entity to the table with a default PartitionKey of
// kv. It is an example of using a struct as an entity. Its only field is Value.
func InsertKeyValue(table, key, value string) error {
	type KeyValue struct {
		ETag         string
		PartitionKey string
		RowKey       string
		Value        string
	}

	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}

	entity := KeyValue{
		ETag:         "*",
		PartitionKey: "kv",
		RowKey:       key,
		Value:        value,
	}

	ctx := context.Background()
	tableClient := client.NewTableClient(table)
	_, err = tableClient.AddEntity(ctx, entity)
	if err != nil {
		return err
	}
	return nil

}

// InsertJSON Unmarshals the supplied value and defaults the PartitionKey to
// main and the RowKey to a UUIDv4 if not exist. It then uses AddEntity to add
// it to the table. It is an example of using a map[string]interface{} as
// the entity type.
func InsertJSON(table string, value []byte) error {

	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}

	entity := map[string]interface{}{}
	if err := json.Unmarshal(value, &entity); err != nil {
		return err
	}

	if _, ok := entity["PartitionKey"]; !ok {
		entity["PartitionKey"] = "main"
	}
	if _, ok := entity["RowKey"]; !ok {
		entity["RowKey"] = uuid.NewString()
	}

	// TODO: insert stalls in certain cases if we add this
	//entity["ETag"] = "*"

	ctx := context.Background()
	tableClient := client.NewTableClient(table)
	_, err = tableClient.AddEntity(ctx, entity)
	if err != nil {
		return err
	}
	return nil

}

// InsertStdn takes one or more records from the standard input and inserts
// them individually using InsertJSON
func InsertStdin(table string) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		err := InsertJSON(table, scanner.Bytes())
		if err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// Query queries the table using an OData filter (see:
// https://docs.microsoft.com/en-us/azure/search/query-odata-filter-orderby-syntax).
//
// Examples of OData filters include:
// 	RowKey eq '1'
// 	PartitionKey eq 'main' and resourceGroup ge '2' and resourceGroup le '3'
func Query(table, filter string) error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	tableClient := client.NewTableClient(table)
	queryOptions := &aztable.QueryOptions{}
	queryOptions.Filter = &filter
	pager := tableClient.Query(queryOptions)
	ctx := context.Background()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		// TODO: let's explore AsModels here, too
		for _, x := range resp.TableEntityQueryResponse.Value {
			delete(x, "odata.etag")
			b, err := json.Marshal(x)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}

func Delete(table, filter string) error {
	if filter == "" {
		return errors.New("filter must be supplied for Delete operation")
	}
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	tableClient := client.NewTableClient(table)
	queryOptions := &aztable.QueryOptions{}
	queryOptions.Filter = &filter
	pager := tableClient.Query(queryOptions)
	ctx := context.Background()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, x := range resp.TableEntityQueryResponse.Value {
			_, err := tableClient.DeleteEntity(ctx, x["PartitionKey"].(string), x["RowKey"].(string), "*")
			if err != nil {
				return err
			}
			delete(x, "odata.etag")
			b, err := json.Marshal(x)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}
