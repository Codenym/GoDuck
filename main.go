package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/codenym/goduck/cmd"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	// Parse command-line arguments

	parquet2dbCMD := flag.NewFlagSet("parquet2db", flag.ExitOnError)
	Template2sqlCMD := flag.NewFlagSet("runifysql", flag.ExitOnError)

	// parquet2dbCMD command-line arguments
	parquet2dbCMDs3_bucket := parquet2dbCMD.String("s3_bucket", "", "Enter the S3 bucket")
	parquet2dbCMDs3_prefix := parquet2dbCMD.String("s3_prefix", "", "Enter the S3 prefix")
	parquet2dbCMDregion := parquet2dbCMD.String("region", "us-east-1", "Enter the AWS region")
	parquet2dbCMDfilename := parquet2dbCMD.String("filename", "database.duckdb", "Enter the filename")
	parquet2dbCMDaws_profile := parquet2dbCMD.String("aws_profile", "default", "Enter the AWS profile")
	parquet2dbCMDcreateTable := parquet2dbCMD.Bool("create_table", false, "Create as tables or views")

	//runifysqlCMD command-line arguments
	Template2sqlCMDfrom := Template2sqlCMD.String("from", "", "Enter the template file or directory path")
	Template2sqlCMDto := Template2sqlCMD.String("to", "", "Enter the output file or directory path")

	// Run Commands
	switch os.Args[1] {
	case "parquet2db":
		cmd.HandleParquet2Db(parquet2dbCMD, parquet2dbCMDs3_bucket, parquet2dbCMDs3_prefix, parquet2dbCMDregion, parquet2dbCMDfilename, parquet2dbCMDaws_profile, parquet2dbCMDcreateTable)
	case "template2sql":
		cmd.HandleTemplate2sql(Template2sqlCMD, Template2sqlCMDfrom, Template2sqlCMDto)
	default:
		fmt.Println("expected 'parquet2db' or 'template2sql' subcommands")
		os.Exit(1)
	}
}
