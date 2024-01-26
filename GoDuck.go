package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/marcboeker/go-duckdb"
)

// Command-line arguments
var (
	filename    string
	createTable bool
	s3_bucket   string
	s3_prefix   string
	aws_profile string
	region      string
)

// Execute a command on the database
func execDbCommand(db *sql.DB, command string) {
	_, err := db.Exec(command)
	if err != nil {
		log.Fatalf("Error executing command '%s': %v", command, err)
	}
}

func main() {
	// Parse command-line arguments
	s3_bucket := flag.String("s3_bucket", "", "Enter the S3 bucket")
	s3_prefix := flag.String("s3_prefix", "", "Enter the S3 prefix")
	region := flag.String("region", "us-east-1", "Enter the AWS region")
	filename := flag.String("filename", "database.duckdb", "Enter the filename")
	aws_profile := flag.String("aws_profile", "default", "Enter the AWS profile")
	createTable := flag.Bool("create_table", false, "Create as tables or views")
	flag.Parse()

	// check if filename exists and if so, delete it
	if _, err := os.Stat(*filename); err == nil {
		os.Remove(*filename)
	}

	// If createTable is true then assign table to a variable, else assign view
	var createWhat string
	if *createTable {
		createWhat = "TABLE"
	} else {
		createWhat = "VIEW"
	}

	// Open database
	db, err := sql.Open("duckdb", *filename)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Install Required DuckDB Extensions for loading parquet from s3 and authenticate with AWS
	execDbCommand(db, "install httpfs; load httpfs;")
	execDbCommand(db, "install aws; load aws;")
	execDbCommand(db, fmt.Sprintf("CALL load_aws_credentials('%s');", *aws_profile))

	// Connect to S3
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: *aws_profile,
		Config: aws.Config{
			Region: aws.String(*region),
		},
	})

	if err != nil {
		fmt.Printf("Failed to initialize new session: %v", err)
		return
	}

	s3Client := s3.New(sess)

	// List objects in S3 bucket/prefix for loading
	input := &s3.ListObjectsInput{
		Bucket: aws.String(*s3_bucket),
		Prefix: aws.String(*s3_prefix),
	}
	result, err := s3Client.ListObjects(input)
	if err != nil {
		log.Fatal(err)
	}

	// Create tables/view for each s3 parquet file
	createdSchemas := make(map[string]bool)
	var wg sync.WaitGroup
	for _, item := range result.Contents {
		wg.Add(1)
		// Check filename matches expected format
		if *item.Key != "" && strings.Contains(*item.Key, ".parquet") && strings.Contains(*item.Key, "_") {
			// Get the schema and table_name from the filename
			filename := filepath.Base(*item.Key)
			schema_name := strings.Split(filename, "_")[0]
			table := strings.Split(filename, "_")[1:]
			table_name := strings.Join(table, "_")
			table_name = strings.Split(table_name, ".")[0]

			// Create schema if it hasn't been created yet
			if !createdSchemas[schema_name] {
				sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema_name)
				fmt.Println(sql)
				execDbCommand(db, sql)
				createdSchemas[schema_name] = true
			}

			// Create table or view based on args
			go func() {
				defer wg.Done()
				execDbCommand(db, fmt.Sprintf("CREATE OR REPLACE %s %s.%s AS FROM read_parquet('s3://%s/%s');", createWhat, schema_name, table_name, *s3_bucket, *item.Key))
			}()

		}
	}
	wg.Wait()

	fmt.Println("DuckDB loading complete.  There were ", len(result.Contents), "items in bucket", *s3_bucket, "with prefix", *s3_prefix)

}
