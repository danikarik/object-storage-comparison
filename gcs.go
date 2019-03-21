package main

import (
	"context"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSStorage implements GCS interface.
type GCSStorage struct {
	client *storage.Client
}

// NewGCS returns a new instance of GCS storage.
func NewGCS(ctx context.Context) (Storage, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	store := &GCSStorage{client: client}
	return store, nil
}

// Contents reads all files within given bucket.
func (g *GCSStorage) Contents(ctx context.Context, bucketName string) ([]FileContent, error) {
	// creates list of file's content
	fileContents := make([]FileContent, 0)
	// send request to obtain bucket items
	bucket := g.client.Bucket(bucketName)
	it := bucket.Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rc, err := bucket.Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		fc := FileContent{
			Name:    attrs.Name,
			Content: data,
		}
		fileContents = append(fileContents, fc)
	}
	// return
	return fileContents, nil
}
