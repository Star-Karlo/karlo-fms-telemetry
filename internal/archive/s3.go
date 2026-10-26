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
// Cold telemetry goes to INTELLIGENT_TIERING: a day that is being read costs
// Standard storage and nothing to read, and a day nobody reads drifts down to
// Glacier-IR storage prices on its own, still with no retrieval charge. It used
// to go to GLACIER_IR, which stores for a fifth of Standard's price but charges
// $0.03 for every GB read — and every Athena
// query reads the whole date range for every device, so a read-heavy archive
// cost several times its storage in retrievals (October 2026: $244 in a month on
// a 453 GB archive whose storage was $2). Credentials/region come from the
// standard AWS chain (AWS_PROFILE / env / instance role).
func UploadFile(ctx context.Context, bucket, key, path string, storageClass types.StorageClass) error {
	return UploadFileAs(ctx, bucket, key, path, storageClass, "application/vnd.apache.parquet")
}

// UploadFileAs is UploadFile with an explicit content type. Dashcam clips are
// "video/mp4", not parquet — the content type drives how a browser opening the
// S3 URL later handles the object.
func UploadFileAs(ctx context.Context, bucket, key, path string, storageClass types.StorageClass, contentType string) error {
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
		ContentType:  aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}
