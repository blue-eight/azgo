package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func BlobFromEnv() (*azblob.ServiceURL, error) {
	accountName := mustGetEnv("AZGO_STORAGE_ACCOUNT_NAME")
	accountKey := mustGetEnv("AZGO_STORAGE_ACCOUNT_KEY")
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := azblob.NewServiceURL(*u, p)
	return &serviceURL, nil
}

func CreateContainer(container string) error {
	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL("mycontainer")
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return err
	}
	return nil
}

func ListContainers() error {
	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	marker := azblob.Marker{}
	for marker.NotDone() {
		listContainer, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			return err
		}
		marker = listContainer.NextMarker

		for _, containerInfo := range listContainer.ContainerItems {
			b, err := json.Marshal(containerInfo)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}

func InsertKeyValue(container, key, value string) error {
	if container == "" {
		container = "main"
	}

	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(key)
	body := strings.NewReader(value)
	headers := azblob.BlobHTTPHeaders{ContentType: "text/plain"}
	metadata := azblob.Metadata{}
	ac := azblob.BlobAccessConditions{}
	cpk := azblob.ClientProvidedKeyOptions{}
	_, err = blobURL.Upload(ctx, body, headers, metadata, ac, azblob.DefaultAccessTier, nil, cpk)
	if err != nil {
		return err
	}
	return nil
}

func Get(container, key string) (string, error) {
	if container == "" {
		container = "main"
	}

	serviceURL, err := BlobFromEnv()
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(key)
	res, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	}
	b := &bytes.Buffer{}
	reader := res.Body(azblob.RetryReaderOptions{})
	defer reader.Close()
	b.ReadFrom(reader)
	return b.String(), nil
}

func Delete(container, key string) error {
	if container == "" {
		container = "main"
	}

	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(key)
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		return err
	}
	return nil
}

func DeleteContainer(container string) error {
	if container == "" {
		container = "main"
	}

	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return err
	}
	_, err = containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	if err != nil {
		return err
	}
	return nil
}

func List(container string) error {
	if container == "" {
		container = "main"
	}

	serviceURL, err := BlobFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)

	marker := azblob.Marker{}
	for marker.NotDone() {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return err
		}
		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			b, err := json.Marshal(blobInfo)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}

func Test() error {
	return nil
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Require environment variable: %s\n", key)
	}
	return value
}
