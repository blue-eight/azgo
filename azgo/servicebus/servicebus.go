package servicebus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
)

// ServiceBusFromEnv creates a new *servicebus.Namespace authenticated via the
// AZGO_SERVICEBUS_CONNECTION_STRING environment variable
func ServiceBusFromEnv() (*servicebus.Namespace, error) {
	connStr := mustGetEnv("AZGO_SERVICEBUS_CONNECTION_STRING")
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		return nil, err
	}
	return ns, nil
}

// CreateQueue creates a service bus queue.
func CreateQueue(name string) error {
	ns, err := ServiceBusFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	qm := ns.NewQueueManager()
	_, err = qm.Put(ctx, name)
	if err != nil {
		return err
	}
	return nil
}

// DeleteQueue deletes a service bus queue.
func DeleteQueue(name string) error {
	ns, err := ServiceBusFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	qm := ns.NewQueueManager()
	err = qm.Delete(ctx, name)
	if err != nil {
		return err
	}
	return nil
}

// ListQueues lists service bus queues in the account and marshals them
// to JSON which provides full details.
func ListQueues() error {
	ns, err := ServiceBusFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	qm := ns.NewQueueManager()
	result, err := qm.List(ctx)
	if err != nil {
		return err
	}
	for _, x := range result {
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
	}
	return nil
}

// Send sends a message to the named service bus queue.
func Send(queue, message string) error {
	ns, err := ServiceBusFromEnv()
	if err != nil {
		return err
	}

	q, err := ns.NewQueue(queue)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = q.Send(ctx, servicebus.NewMessageFromString(message))
	if err != nil {
		return nil
	}
	return nil
}

// Receive receives a single message from the service bus queue using the
// ReceiveOne method, with the help of a channel.
func Receive(queue string) (string, error) {

	ns, err := ServiceBusFromEnv()
	if err != nil {
		return "", err
	}

	q, err := ns.NewQueue(queue)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := make(chan string, 1)
	err = q.ReceiveOne(
		ctx,
		servicebus.HandlerFunc(func(ctx context.Context, message *servicebus.Message) error {
			// we use a buffered channel to avoid blocking here
			c <- string(message.Data)
			return message.Complete(ctx)
		}),
	)
	if err != nil {
		return "", err
	}
	result := <-c
	return result, nil
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Require environment variable: %s\n", key)
	}
	return value
}

func Test() error {
	return nil
}
