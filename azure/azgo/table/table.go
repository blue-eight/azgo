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

// TableClientFromEnv creates an *aztable.TableServiceClient authenticated
// by the environment variables AZGO_TABLE_ACCOUNT and AZGO_TABLE_KEY.
// This uses Cosmos DB by default, but also lets us choose Storage Account
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

// UpsertKeyValue upserts an entity into the table with a default PartitionKey of
// main. In this case we use a map[string]interface{} to do so. Its only field is Value.
func UpsertKeyValue(table, key, value string) error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}

	entity := map[string]interface{}{
		"ETag":         "*",
		"PartitionKey": "main",
		"RowKey":       key,
		"Value":        value,
	}

	ctx := context.Background()
	tableClient := client.NewTableClient(table)
	_, err = tableClient.UpsertEntity(ctx, entity, aztable.TableUpdateMode(aztable.Replace))
	if err != nil {
		return err
	}
	return nil

}

// InsertKeyValue inserts an entity into the table with a default PartitionKey of
// main. It is an example of using a struct as an entity. Its only field is Value.
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
		PartitionKey: "main",
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
// main and the RowKey to a UUIDv4 if not provided. It then uses AddEntity to
// add it to the table. It is an example of using a map[string]interface{} as
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

// InsertStdin takes one or more records from the standard input and inserts
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
// The latter is a useful way to find items with a particular prefix (in this case '2')
func Query(table, filter string) error {
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	tableClient := client.NewTableClient(table)
	queryOptions := &aztable.QueryOptions{
		Filter: &filter,
	}
	pager := tableClient.Query(queryOptions)
	ctx := context.Background()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		// TODO: let's explore AsModels here, too
		for _, x := range resp.TableEntityQueryResponse.Value {
			// we remove the odata.etag for cleaner/friendlier output
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

// QueryDelete is similar to Query, and queries the table using an OData filter (see:
// https://docs.microsoft.com/en-us/azure/search/query-odata-filter-orderby-syntax).
// but in QueryDelete we both *require* a filter, and delete each item in the
// query before printing it to the standard output.
func QueryDelete(table, filter string) error {
	if filter == "" {
		return errors.New("filter must be supplied for Delete operation")
	}
	client, err := TableClientFromEnv()
	if err != nil {
		return err
	}
	tableClient := client.NewTableClient(table)
	queryOptions := &aztable.QueryOptions{
		Filter: &filter,
	}
	pager := tableClient.Query(queryOptions)
	ctx := context.Background()
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, x := range resp.TableEntityQueryResponse.Value {
			_, err := tableClient.DeleteEntity(ctx, x["PartitionKey"].(string), x["RowKey"].(string), "*")
			if err != nil {
				return err
			}
			// we remove the odata.etag for cleaner/friendlier output
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

// Get returns a single entity from a table by its PartitionKey and RowKey
// This guarantees we return a single item, or an error, and also avoids
// us having to create a Query for a single item
func Get(table, partitionKey, rowKey string) (map[string]interface{}, error) {
	client, err := TableClientFromEnv()
	if err != nil {
		return nil, err
	}
	tableClient := client.NewTableClient(table)
	ctx := context.Background()
	resp, err := tableClient.GetEntity(ctx, partitionKey, rowKey)
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Delete deletes and returns a single item from a table by its PartitionKey
// and RowKey. It will return an error if the item is not found.
func Delete(table, partitionKey, rowKey string) (map[string]interface{}, error) {
	client, err := TableClientFromEnv()
	if err != nil {
		return nil, err
	}
	tableClient := client.NewTableClient(table)
	ctx := context.Background()
	resp, err := tableClient.GetEntity(ctx, partitionKey, rowKey)
	if err != nil {
		return nil, err
	}
	x := resp.Value
	_, err = tableClient.DeleteEntity(ctx, x["PartitionKey"].(string), x["RowKey"].(string), "*")
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Require environment variable: %s\n", key)
	}
	return value
}
