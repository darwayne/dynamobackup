package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/darwayne/dyc"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func main() {
	var (
		folder = flag.String("folder", "./backups", "the folder where backups will be stored")
		table  = flag.String("table", "", "dynamo table name to backup")
	)
	flag.Parse()
	if *table == "" || *folder == "" {
		flag.Usage()
		os.Exit(1)
	}

	t := time.Now()
	dayFolder := fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day())
	if err := os.MkdirAll(filepath.Join(*folder, dayFolder), os.ModePerm); err != nil {
		panic(err)
	}
	fileName := fmt.Sprintf("%d.gz", t.Unix())
	f, err := os.Create(filepath.Join(*folder, dayFolder, fileName))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sess := session.Must(session.NewSession())
	cli := dyc.NewClient(dynamodb.New(sess))
	scanInput := &dynamodb.ScanInput{TableName: table}
	messages := make(chan *dynamodb.ScanOutput, runtime.NumCPU())
	start := time.Now()

	go func() {
		defer close(messages)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := cli.ParallelScanIterator(ctx, scanInput, runtime.NumCPU(), func(output *dynamodb.ScanOutput) error {
			select {
			case messages <- output:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}, true)

		if err != nil {
			panic(err)
		}
	}()

	writer := gzip.NewWriter(f)
	defer writer.Close()
	var rowsSaved int
	for output := range messages {
		for _, i := range output.Items {
			result, err := json.Marshal(i)
			if err != nil {
				panic(err)
			}

			if _, err := writer.Write(bytes.TrimSpace(result)); err != nil {
				panic(err)
			}
			if _, err := writer.Write([]byte("\n")); err != nil {
				panic(err)
			}

			rowsSaved++
		}
	}

	log.Println(rowsSaved, "rows backed up", "took", time.Since(start))

}
