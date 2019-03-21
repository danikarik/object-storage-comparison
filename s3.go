package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Storage implements S3 interface.
type S3Storage struct {
	session    *session.Session
	service    *s3.S3
	downloader *s3manager.Downloader
}

// NewS3 returns a new instance of S3 storage.
func NewS3() (Storage, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-south-1"),
		Credentials: credentials.NewSharedCredentials("", ""),
	})
	if err != nil {
		return nil, fmt.Errorf("could not start session: %v", err)
	}
	store := &S3Storage{
		session: sess,
		service: s3.New(sess),
		downloader: s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024
		}),
	}
	return store, nil
}

// Contents reads all files within given bucket.
func (s *S3Storage) Contents(ctx context.Context, bucketName string) ([]FileContent, error) {
	// creates list of file's content
	fileContents := make([]FileContent, 0)
	// basic query to get bucket items
	query := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}
	// send request to obtain bucket items
	list, err := s.service.ListObjects(query)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return nil, fmt.Errorf("%s: %v", s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				return nil, fmt.Errorf("%v", aerr.Error())
			}
		}
		return nil, err
	}
	// iterate over items
	for _, o := range list.Contents {
		key := *o.Key
		buf := &aws.WriteAtBuffer{}
		query := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}
		s.downloader.Download(buf, query)
		fc := FileContent{
			Name:    key,
			Content: buf.Bytes(),
		}
		fileContents = append(fileContents, fc)
	}
	return fileContents, nil
}
