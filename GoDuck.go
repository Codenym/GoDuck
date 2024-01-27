package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/marcboeker/go-duckdb"
)

// Command-line arguments
// var (
// 	filename    string
// 	createTable bool
// 	s3_bucket   string
// 	s3_prefix   string
// 	aws_profile string
// 	region      string
// )

// Execute a command on the database
func execDbCommand(db *sql.DB, command string) {
	_, err := db.Exec(command)
	if err != nil {
		log.Fatalf("Error executing command '%s': %v", command, err)
	}
}

func main() {
	// Parse command-line arguments

	parquet2dbCMD := flag.NewFlagSet("parquet2db", flag.ExitOnError)
	template2sqlCMD := flag.NewFlagSet("runifysql", flag.ExitOnError)

	// parquet2dbCMD command-line arguments
	parquet2dbCMDs3_bucket := parquet2dbCMD.String("s3_bucket", "", "Enter the S3 bucket")
	parquet2dbCMDs3_prefix := parquet2dbCMD.String("s3_prefix", "", "Enter the S3 prefix")
	parquet2dbCMDregion := parquet2dbCMD.String("region", "us-east-1", "Enter the AWS region")
	parquet2dbCMDfilename := parquet2dbCMD.String("filename", "database.duckdb", "Enter the filename")
	parquet2dbCMDaws_profile := parquet2dbCMD.String("aws_profile", "default", "Enter the AWS profile")
	parquet2dbCMDcreateTable := parquet2dbCMD.Bool("create_table", false, "Create as tables or views")

	//runifysqlCMD command-line arguments
	template2sqlCMDfrom := template2sqlCMD.String("from", "", "Enter the template file path")
	template2sqlCMDto := template2sqlCMD.String("to", "", "Enter the output file path")

	switch os.Args[1] {
	case "parquet2db":
		handleParquet2Db(parquet2dbCMD, parquet2dbCMDs3_bucket, parquet2dbCMDs3_prefix, parquet2dbCMDregion, parquet2dbCMDfilename, parquet2dbCMDaws_profile, parquet2dbCMDcreateTable)
	case "template2sql":
		handletemplate2sql(template2sqlCMD, template2sqlCMDfrom, template2sqlCMDto)
	default:
		fmt.Println("expected 'hello' or 'add' subcommands")
		os.Exit(1)
	}
}

func isDirectory(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}

func getSqlFromTemplatefile(fpath *string) string {
	file, err := os.Open(*fpath)
	if err != nil {
		// Handle the error and exit.
		fmt.Println("Error opening file:", err)
		return ""
	}
	defer file.Close()
	data, _ := io.ReadAll(file)
	template := string(data)

	// Replace all instances of $something.something with something_somthing
	regexPattern := `\$(\w+)_(\w+)`
	re, _ := regexp.Compile(regexPattern)

	sql := re.ReplaceAllStringFunc(template, func(match string) string {
		// Replace '_' with '.' in the match
		return strings.Replace(match, "_", ".", 1)[1:]
	})
	return sql
}

func writeFile(fpath, content string) {
	f, err := os.Create(fpath)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}
	defer f.Close()
	f.WriteString(content)
}

func listFilesInDir(dir string) []string {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// fmt.Println(path)
		files = append(files, path)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return files
}

func handletemplate2sql(template2sqlCMD *flag.FlagSet, from_fpath, to_fpath *string) {
	template2sqlCMD.Parse(os.Args[2:])
	fmt.Println("Converting templates from", *from_fpath, "to", *to_fpath, "recursively")

	isDirectoryFrom := isDirectory(*from_fpath)
	isDirectoryTo := isDirectory(*to_fpath)

	var isdir bool
	if isDirectoryFrom && isDirectoryTo {
		isdir = true
	} else if !isDirectoryFrom && !isDirectoryTo {
		isdir = false
	} else {
		fmt.Println("Error: both from and to must be either directories or files")
		return
	}

	if isdir {
		// Get all files in directory
		files := listFilesInDir(*from_fpath)
		fmt.Println(*from_fpath)
		fmt.Println(*to_fpath)
		for _, from_file := range files {
			if strings.Contains(from_file, ".sql") {
				query := getSqlFromTemplatefile(&from_file)
				to_file := filepath.Join(*to_fpath, filepath.Base(from_file))
				writeFile(to_file, query)
				fmt.Println(from_file, " Successfully converted to sql file located at ", to_file)
			}
		}
	} else {
		query := getSqlFromTemplatefile(from_fpath)
		writeFile(*to_fpath, query)
		fmt.Println(*from_fpath, " Successfully converted to sql file located at ", *to_fpath)
	}
}

func handleParquet2Db(parquet2dbCMD *flag.FlagSet, s3_bucket, s3_prefix, region, filename, aws_profile *string, createTable *bool) {
	parquet2dbCMD.Parse(os.Args[2:])

	// If createTable is true then assign table to a variable, else assign view
	var createWhat string
	if *createTable {
		createWhat = "TABLE"
	} else {
		createWhat = "VIEW"
	}
	fmt.Println("Loading parquet files from S3 bucket", *s3_bucket, "with prefix", *s3_prefix, "into", *filename, "as", createWhat, 'S')

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

	fmt.Println("DuckDB loading complete.  There were ", len(result.Contents), "items in bucket", s3_bucket, "with prefix", s3_prefix)

}
