package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"os"
	"time"
)

func main() {
	var (
		backupFile = flag.String("i", "./backups/prod/2022-01-17/1642435789.gz", "the backup file")
		table      = flag.String("table", "", "the table to restore items to")
		clear      = flag.Bool("clear", false, "will clear all rows in existing table first when set")
		ids        = flag.String("ids", "PK,SK", "comma separated list of the partition keys")
	)
	flag.Parse()
	_, _, _ = table, clear, ids

	f, err := os.Open(*backupFile)
	if err != nil {
		panic(err)
	}
	reader, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	var total int
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		total++
		var item map[string]*dynamodb.AttributeValue
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			panic(err)
		}
	}

	fmt.Println(total, "records read in", time.Since(start))
}
