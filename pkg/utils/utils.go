package utils

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

func ExecDbCommand(db *sql.DB, command string) {
	_, err := db.Exec(command)
	if err != nil {
		log.Fatalf("Error executing command '%s': %v", command, err)
	}
}

func IsDirectory(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}

func WriteFile(fpath, content string) {
	f, err := os.Create(fpath)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}
	defer f.Close()
	f.WriteString(content)
}

func ListFilesInDir(dir string) []string {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return files
}
