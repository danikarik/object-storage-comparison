package main

import (
	"context"
	"log"
	"os"
	"time"
)

// FileContent holds object's name and content.
type FileContent struct {
	Name    string
	Content []byte
}

// Storage is a common interface for S3 and GCS.
type Storage interface {
	Contents(ctx context.Context, bucketName string) ([]FileContent, error)
}

var (
	bucketNameS3  = os.Getenv("S3_BUCKET_NAME")
	bucketNameGCS = os.Getenv("GCS_BUCKET_NAME")
)

func main() {
	ctx := context.Background()

	// AWS S3
	s3storage, err := NewS3()
	if err != nil {
		log.Fatalf("s3: %v", err)
	}
	log.Printf("s3: elapsed time %v", track(func() {
		files, err := s3storage.Contents(ctx, bucketNameS3)
		if err != nil {
			log.Printf("s3: %v", err)
			return
		}
		for _, file := range files {
			log.Println("s3:", file.Name)
		}
	}))

	// Google Cloud Storage
	gcstorage, err := NewGCS(ctx)
	if err != nil {
		log.Fatalf("gcs: %v", err)
	}
	log.Printf("gcs: elapsed time %v", track(func() {
		files, err := gcstorage.Contents(ctx, bucketNameGCS)
		if err != nil {
			log.Printf("gcs: %v", err)
			return
		}
		for _, file := range files {
			log.Println("gcs:", file.Name)
		}
	}))
}

func track(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}
