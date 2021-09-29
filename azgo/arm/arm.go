package arm

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resources/armresources"
)

// ListResources lists all the resources in the subscription and prints them to the
// standard output in JSON format.
func ListResources(subscriptionID string) error {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return err
	}
	con := arm.NewDefaultConnection(cred, nil)
	if err != nil {
		return err
	}

	client := armresources.NewResourcesClient(con, subscriptionID)
	pager := client.List(nil)
	for pager.NextPage(context.Background()) {
		if err := pager.Err(); err != nil {
			log.Fatalf("failed to advance page: %v", err)
		}
		for _, r := range pager.PageResponse().ResourceListResult.Value {
			printJSON(r)
		}
	}
	return nil
}

// MyPolicy is an example of a custom policy that logs
// the request URL and duration with an optional LogPrefix.
// This is an example only and not designed to be used
// in a production scenario as-is.
type MyPolicy struct {
	LogPrefix string
}

func (m *MyPolicy) Do(req *policy.Request) (*http.Response, error) {
	// mutate/process req
	start := time.Now()
	// Forward the request to next policy in the pipeline
	res, err := req.Next()
	// mutate/process res
	// Return the response & error back to the previous policy in the pipeline.
	record := struct {
		Policy   string
		URL      string
		Duration time.Duration
	}{
		Policy:   "MyPolicy",
		URL:      req.Raw().URL.RequestURI(),
		Duration: time.Duration(time.Since(start).Milliseconds()),
	}
	b, _ := json.Marshal(record)
	log.Printf("%s %s\n", m.LogPrefix, b)
	return res, err
}

// ListResourcesWithPolicy lists all the resources
// in a subscription and prints them to the standard
// output in JSON format. It also demonstrates how to
// use a custom policy, MyPolicy, which logs data to
// standard error.
func ListResourcesWithPolicy(subscriptionID string) error {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return err
	}

	mp := &MyPolicy{
		LogPrefix: "[MyPolicy]",
	}
	options := &arm.ConnectionOptions{}
	options.PerCallPolicies = []policy.Policy{mp}
	options.Retry = policy.RetryOptions{
		RetryDelay: 20 * time.Millisecond,
	}

	con := arm.NewDefaultConnection(cred, options)
	if err != nil {
		return err
	}

	client := armresources.NewResourcesClient(con, subscriptionID)
	pager := client.List(nil)
	for pager.NextPage(context.Background()) {
		if err := pager.Err(); err != nil {
			log.Fatalf("failed to advance page: %v", err)
		}
		for _, r := range pager.PageResponse().ResourceListResult.Value {
			printJSON(r)
		}
	}
	return nil
}

func printJSON(i interface{}) {
	b, err := json.Marshal(i)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", b)
}
