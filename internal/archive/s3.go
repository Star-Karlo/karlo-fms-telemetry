package archive

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// UploadFile puts a local file at s3://bucket/key with the given storage class.
// For cold telemetry we use GLACIER_IR (Glacier Instant Retrieval): much cheaper
// storage than Standard, still millisecond retrieval. Credentials/region come
// from the standard AWS chain (AWS_PROFILE / env / instance role).
func UploadFile(ctx context.Context, bucket, key, path string, storageClass types.StorageClass) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = s3.NewFromConfig(cfg).PutObject(ctx, &s3.PutObjectInput{
		Bucket:       &bucket,
		Key:          &key,
		Body:         f,
		StorageClass: storageClass,
		ContentType:  aws.String("application/vnd.apache.parquet"),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}
