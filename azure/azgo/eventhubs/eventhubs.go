package eventhubs

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

// EventHubFromEnv creates a new *eventhub.Hub authenticated via the
// AZGO_EVENTHUBS_CONNECTION_STRING environment variable. The event
// hub should be pre-created and specified as part of the connection
// string.
func EventHubFromEnv() (*eventhub.Hub, error) {
	connStr := mustGetEnv("AZGO_EVENTHUBS_CONNECTION_STRING")
	hub, err := eventhub.NewHubFromConnectionString(connStr)
	if err != nil {
		return nil, err
	}
	return hub, nil
}

// Send sends a message to the event hub via the NewEventFromString
// method.
func Send(message string) error {
	hub, err := EventHubFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = hub.Send(ctx, eventhub.NewEventFromString(message))
	if err != nil {
		return err
	}
	return nil
}

// SendStdin sends a stream of events from the standard input to the
// event hub via the NewEventFromString method.
func SendStdin() error {
	hub, err := EventHubFromEnv()
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()
	for scanner.Scan() {
		err = hub.Send(ctx, eventhub.NewEventFromString(scanner.Text()))
		if err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// Receive receives events from the event hub, marshals them to JSON,
// and writes them to the standard output.
func Receive() error {
	hub, err := EventHubFromEnv()
	if err != nil {
		return err
	}

	handler := func(c context.Context, event *eventhub.Event) error {
		fmt.Printf("%s\n", event.Data)
		return nil
	}

	ctx := context.Background()
	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		return err
	}
	for _, partitionID := range runtimeInfo.PartitionIDs {
		// Listen to each partition of the Event Hub and start receiving messages
		//
		// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
		// <- listenerHandle.Done() signals listener has stopped
		// listenerHandle.Err() provides the last error the receiver encountered
		_, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
		if err != nil {
			return err
		}
	}

	// Wait for a signal to quit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan

	err = hub.Close(ctx)
	if err != nil {
		return err
	}

	return nil
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
