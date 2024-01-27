package cmd

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/codenym/goduck/pkg/utils"
	_ "github.com/marcboeker/go-duckdb"
)

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

func HandleTemplate2sql(template2sqlCMD *flag.FlagSet, from_fpath, to_fpath *string) {
	template2sqlCMD.Parse(os.Args[2:])
	fmt.Println("Converting templates from", *from_fpath, "to", *to_fpath)

	isDirectoryFrom := utils.IsDirectory(*from_fpath)
	isDirectoryTo := utils.IsDirectory(*to_fpath)

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
		files := utils.ListFilesInDir(*from_fpath)
		for _, from_file := range files {
			if strings.Contains(from_file, ".sql") {
				query := getSqlFromTemplatefile(&from_file)
				to_file := filepath.Join(*to_fpath, filepath.Base(from_file))
				utils.WriteFile(to_file, query)
				fmt.Println(from_file, "Successfully converted to sql file located at", to_file)
			}
		}
	} else {
		query := getSqlFromTemplatefile(from_fpath)
		utils.WriteFile(*to_fpath, query)
		fmt.Println(*from_fpath, "Successfully converted to sql file located at", *to_fpath)
	}
}
