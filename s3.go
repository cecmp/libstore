package libstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Ops provides operations for AWS S3 bucket interactions.
type S3Ops struct {
	s3Client *s3.Client
	bucket   string
}

// NewS3Ops initializes an S3Ops instance with AWS S3 client authorization.
//
// Parameters:
//   - ctx: Context for managing request lifecycles.
//   - bucket: The name of the AWS S3 bucket to interact with.
//
// Returns:
//   - A pointer to an initialized S3Ops instance.
//   - An error if the AWS configuration cannot be loaded or if the specified bucket cannot be accessed.
//
// Environment Variables:
//   - AWS_ACCESS_KEY_ID: AWS access key ID.
//   - AWS_SECRET_ACCESS_KEY: AWS secret access key.
//   - AWS_REGION: AWS region.
//
// Note:
// These environment variables are required for the AWS SDK to authenticate and perform operations on the S3 bucket.
func NewS3Ops(ctx context.Context, bucket string) (*S3Ops, error) {
	// Load the default configuration.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", LocationError("failed to load AWS configuration"), err)
	}

	// Create an S3 client using the loaded configuration
	s3Client := s3.NewFromConfig(cfg)

	// Check if the bucket exists and is accessible
	_, err = s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", LocationError("failed to access S3 bucket"), err)
	}

	return &S3Ops{
		s3Client: s3Client,
		bucket:   bucket,
	}, nil
}

// Create creates a new key in S3.
func (s *S3Ops) Create(ctx context.Context, key string) error {
	_, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	// If the object exists, return a KeyError
	if err == nil {
		return KeyError("key already exists: " + key)
	}

	// If the error is not a "Not Found" error, return an OpsInternalError
	var nfe *types.NotFound
	if !errors.As(err, &nfe) {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to check if key exists"), err)
	}

	// Create an empty object
	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(""),
	})
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to create key"), err)
	}

	return nil
}

// ReadAll reads the entire content of the given key.
func (s *S3Ops) ReadAll(ctx context.Context, key string) ([][]byte, error) {
	output, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nfe *types.NotFound
		if errors.As(err, &nfe) {
			return nil, KeyNotFoundError("key not found: " + key)
		}
		return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to read key"), err)
	}
	defer output.Body.Close()

	content, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", EntryError("failed to read content"), err)
	}

	// Assume entries are separated by newlines
	return [][]byte{content}, nil
}

// ReadLast reads the last entry of the given key.
func (s *S3Ops) Read(ctx context.Context, key string) ([]byte, error) {
	entries, err := s.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, EntryError("no entries found for key: " + key)
	}
	return entries[len(entries)-1], nil
}

// Put replaces an entry to the file with the given key.
func (s *S3Ops) Put(ctx context.Context, key string, entry []byte) error {
	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(entry)),
	})
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to replace entry"), err)
	}
	return nil
}

// Delete deletes the given key and associated content.
func (s *S3Ops) Delete(ctx context.Context, key string) error {
	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nfe *types.NotFound
		if errors.As(err, &nfe) {
			return KeyNotFoundError("key not found: " + key)
		}
		return fmt.Errorf("%w: %w", OpsInternalError("failed to delete key"), err)
	}
	return nil
}

// List lists all keys in the bucket-scope.
func (s *S3Ops) List(ctx context.Context) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to list keys"), err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}
