package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/spf13/viper"
)

func handler(ctx context.Context, s3Event events.S3Event) error {
	log.Printf("INFO: Received '%d' events.", len(s3Event.Records))
	config := loadConfig()
	dbConStr := config.GetString("database_connection_string")

	// TODO: Open connection only when "upload object" events are present
	db := openDbConnection(dbConStr)
	defer closeDbConnection(db)

	expectedBucket := config.GetString("file_uploads_bucket_name")
	for _, record := range s3Event.Records {
		bucket := record.S3.Bucket.Name
		eventName := record.EventName
		key := record.S3.Object.Key
		tokens := strings.Split(key, "/")
		log.Printf("INFO: Received event '%s' for '%s/%s'.", eventName, bucket, key)

		err := sanityCheck(bucket, expectedBucket, eventName, tokens)
		if err != nil {
			log.Println(err)
			continue
		}

		rowsAffected, lastUpdateId, err := updateStatus(db, tokens[0], tokens[1])
		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("INFO: Update done. LastUpdateId: %d, RowsAffected: %d", lastUpdateId, rowsAffected)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}

func sanityCheck(bucket, expectedBucket, eventName string, tokens []string) error {
	if bucket != expectedBucket {
		return fmt.Errorf("WARN: Skipped event because of bucket missmatch. Expected: '%s', Actual: '%s'", expectedBucket, bucket)
	}

	const expectedEventName = "ObjectCreated:Put"
	if eventName != expectedEventName {
		return fmt.Errorf("WARN: Skipped event because of eventName missmatch. Expected: '%s', Actual: '%s'", expectedEventName, eventName)
	}

	if len(tokens) != 2 {
		return fmt.Errorf("ERROR: Object key has more parts. Expected: 2, Actual: %d, %s", len(tokens), tokens)
	}
	return nil
}

func updateStatus(db *sql.DB, userId, objectKey string) (int64, int64, error) {
	// status_id = 2, means uploaded
	exec, err := db.Exec("update UploadRequests set status_id = 2 where user_id = ? and object_key = ?", userId, objectKey)
	if err != nil {
		return 0, 0, fmt.Errorf("ERROR: Failed to update metadata database for userId '%s' and objectKey '%s'", userId, objectKey)
	}

	rowsAffected, err := exec.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("ERROR: Failed to get affected rows for userId '%s' and objectKey '%s'", userId, objectKey)
	}

	lastUpdateId, err := exec.LastInsertId()
	if err != nil {
		return 0, 0, fmt.Errorf("ERROR: Failed to get last update id for userId '%s' and objectKey '%s'", userId, objectKey)
	}

	return rowsAffected, lastUpdateId, nil
}

func openDbConnection(conStr string) *sql.DB {
	db, err := sql.Open("mysql", conStr)
	if err != nil {
		log.Panic("Can't connect to a database!", err)
	}
	return db
}

func closeDbConnection(db *sql.DB) {
	err := db.Close()
	if err != nil {
		log.Panic("Can't close a connection to a database!", err)
	}
}

func loadConfig() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix("FA")
	v.AutomaticEnv()
	return v
}
