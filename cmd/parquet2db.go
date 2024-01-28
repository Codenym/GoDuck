package cmd

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
	"github.com/codenym/goduck/pkg/utils"
	_ "github.com/marcboeker/go-duckdb"
)

func HandleParquet2Db(parquet2dbCMD *flag.FlagSet, s3_bucket, s3_prefix, region, filename, aws_profile *string, createTable *bool) {
	parquet2dbCMD.Parse(os.Args[2:])

	// If createTable is true then assign table to a variable, else assign view
	var createWhat string
	if *createTable {
		createWhat = "TABLE"
	} else {
		createWhat = "VIEW"
	}
	fmt.Println("Loading parquet files from S3 bucket", *s3_bucket, "with prefix", *s3_prefix, "into", *filename, "as", createWhat, "S")

	// check if filename exists and if so, delete it
	if _, err := os.Stat(*filename); err == nil {
		os.Remove(*filename)
	}

	// Open database
	db, err := sql.Open("duckdb", *filename)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Install Required DuckDB Extensions for loading parquet from s3 and authenticate with AWS
	utils.ExecDbCommand(db, "install httpfs; load httpfs;")
	utils.ExecDbCommand(db, "install aws; load aws;")
	utils.ExecDbCommand(db, fmt.Sprintf("CALL load_aws_credentials('%s');", *aws_profile))

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
		// Check filename matches expected format
		current_key := *item.Key
		if current_key != "" && strings.Contains(current_key, ".parquet") && strings.Contains(current_key, "_") {
			wg.Add(1)
			// Get the schema and table_name from the filename
			filename := filepath.Base(current_key)
			schema_name := strings.Split(filename, "_")[0]
			table := strings.Split(filename, "_")[1:]
			table_name := strings.Join(table, "_")
			table_name = strings.Split(table_name, ".")[0]
			// Create schema if it hasn't been created yet
			if !createdSchemas[schema_name] {
				sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema_name)
				fmt.Println(sql)
				utils.ExecDbCommand(db, sql)
				createdSchemas[schema_name] = true
			}

			// Create table or view based on args
			go func(ck, tbl, sch string) {
				defer wg.Done()
				utils.ExecDbCommand(db, fmt.Sprintf("CREATE OR REPLACE %s %s.%s AS FROM read_parquet('s3://%s/%s');", createWhat, sch, tbl, *s3_bucket, ck))
			}(current_key, table_name, schema_name)

		}
	}
	wg.Wait()

	fmt.Println("DuckDB loading complete.  There were ", len(result.Contents), "items in bucket", *s3_bucket, "with prefix", *s3_prefix)

}
