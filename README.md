# GoDuck

## Quick Start
1. Clone the Repository: `git clone https://github.com/codenym/GoDuck.git`
1. Navigate to the Directory: `cd GoDuck`
1. Build the CLI: Ensure Go is installed and run `go build`.
1. Run the CLI: Use ./GoDuck with the necessary flags. Example: ``./GoDuck -s3_bucket your-bucket -s3_prefix your-prefix -aws_profile your-profile`

## Description

GoDuck is a command-line interface designed for working with an s3 parquet data lake locally in duckDB.  

+ Load parquet files from an S3 bucket into a DuckDB database, either as tables with downloaded data or as views referencing the external parquet files in S3.  
+ Convert templated sql file(s) like you might have in a data pipeline to be run in an IDE. 

## Usage
The GoDuck CLI accepts hasseveral command-line arguments:

### parquet2db

Load parquet files from an S3 bucket into a DuckDB database, either as tables with downloaded data or as views referencing the external parquet files in S3.  Uutilizes Go's concurrency for efficient data downloading from S3 and writing to into duckdb tables.

**Arguments:**
- `s3_bucket`: Specify the source S3 bucket name.
- `s3_prefix`: Specify the source S3 prefix.
- `filename`: Set the filename for the local DuckDB database (default=database.duckdb)
- `aws_profile`: Define the AWS profile for accessing S3 (default=default)
- `create_table`: Flag to determine if the data should be loaded as tables (default is views).

>Note: GoDuck using the [normal AWS credential chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)

**Examples**
Create views:
`./GoDuck -s3_bucket my-bucket -s3_prefix data/directory/ -aws_profile myProfile`

Create tables:
`./GoDuck -s3_bucket my-bucket -s3_prefix data/directory/ -aws_profile myProfile -create_table true`

### template2sql

Convert templated sql file(s) like you might have in a data pipeline to be run in an IDE.  Table names in the query are changed from `$schemaname_table_name` format to `schemaname.table_name`.

**Arguments:**
- `from`: The file or directory to be converted
- `to`: The file or directory to move converted file(s) to

**Examples**

Convert File:
`./GoDuck template2sql -from test_files/test1.sql -to test_out/test1.sql`

Convert Directory of Files (non recursive):
`./GoDuck template2sql -from test_files -to test_out/`


## Contributing

Contributions are welcome! Please submit pull requests with bug fixes or feature improvements.  If you're unsure about something feel free to open an issue and ask.

## Support

For support, please open an issue in the GitHub repository.
