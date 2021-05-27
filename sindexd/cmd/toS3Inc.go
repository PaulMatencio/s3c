// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bufio"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"sync"
	"time"
)

// readLogCmd represents the readLog command
var (
	/*
		logFile string
		logging int
	*/
	replace    bool
	toS3IncCmd = &cobra.Command{
		Use:   "incToS3",
		Short: "Incremental migration of index-ids to S3",
		Long: `
		The Sindexd log file must be preformatted as following:  
			Input record format: Month day hour ADD|DELETE index_id: [Index-id] key:[key] value: [value]
 			month: [Jan|Feb|Mar|Apr......|Dec]
			day:0..31
			hour: hh:mm:ss
			[index-id]: the Sindexd index -id (Ex: 6B62FA8EF3B1B1E9C5FBBD7E5773FC0300002A20)
			[key] : a key of the index-id
			[value]: the value of the key
		`,
		Run: func(cmd *cobra.Command, args []string) {
			toS3Inc(cmd, args)
		},
	}
)

func init() {
	rootCmd.AddCommand(toS3IncCmd)
	toS3IncCmd.Flags().StringVarP(&logFile, "logFile", "i", "", "The preformatted Sindexd input log file")
	toS3IncCmd.Flags().IntVarP(&maxKey, "maxLine", "m", 50, "The maximum number of lines to be processed concurrently")
	toS3IncCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "The prefix of the S3  bucket names Ex: moses-meta-prod")
	toS3IncCmd.Flags().BoolVarP(&check, "check", "v", false, "Run in Checking mode")
	toS3IncCmd.Flags().BoolVarP(&replace, "replace", "r", false, "Replace existing objects")
	// toS3IncCmd.Flags().IntVarP(&logging, "logging", "", 10000, "logging frequency")
}

func toS3Inc(cmd *cobra.Command, args []string) {

	if len(logFile) == 0 {
		usage(cmd.Name())
		return
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket)
			return
		}
	}

	tos3 := datatype.CreateSession{
		EndPoint:  viper.GetString("toS3.url"),
		Region:    viper.GetString("toS3.region"),
		AccessKey: viper.GetString("toS3.access_key_id"),
		SecretKey: viper.GetString("toS3.secret_access_key"),
	}
	svc := s3.New(api.CreateSession2(tos3))
	ToS3Inc(logFile, maxKey, bucket, svc)

}

func ToS3Inc(logFile string, maxKey int, bucket string, svc *s3.S3) {

	var (
		scanner                         *bufio.Scanner
		err                             error
		idxMap                          = buildIdxMap()
		total, skip, delete, add, error int
		stop                            bool
		mu, mu1, mu2, mue               sync.Mutex
	)

	if scanner, err = utils.Scanner(logFile); err != nil {
		gLog.Error.Printf("Error scanning %v file %s", err, logFile)
		return
	}

	var wg sync.WaitGroup
	stop = false
	total, skip, delete, add = 0, 0, 0, 0
	start := time.Now()
	for !stop {
		if linea, _ := utils.ScanLines(scanner, int(maxKey)); len(linea) > 0 {
			if l := len(linea); l > 0 {
				wg.Add(l)
				for _, v := range linea {
					total++
					go func(v string, svc *s3.S3) {
						defer wg.Done()
						if err, oper := parseSindexdLog(v, idxMap, bucket, bucketNumber); err == nil {

							// k:=oper.Key;buck:=oper.Bucket;value:=[]byte(oper.Value)
							if oper.Oper == "ADD" {
								gLog.Trace.Println(oper.Oper, oper.Bucket, oper.Key, oper.Value)
								if !check {
									if replace {
										if r, err := writeToS3(svc, oper.Bucket, oper.Key, []byte(oper.Value)); err == nil {
											gLog.Trace.Println(oper.Key, oper.Bucket, *r.ETag, *r)
											mu2.Lock()
											add++
											mu2.Unlock()
										} else {
											gLog.Error.Printf("Error %v - Writing key %s to bucket %s", err, oper.Key, oper.Bucket)
											mue.Lock()
											error++
											mue.Unlock()
										}
									} else {
										stat := datatype.StatObjRequest{Service: svc, Bucket: oper.Bucket, Key: oper.Key}
										if _, err := api.StatObject(stat); err == nil {
											gLog.Warning.Printf("Object %s already existed in the Bucket %s", oper.Key, oper.Bucket)
											mu.Lock()
											skip++
											mu.Unlock()
										} else {
											if aerr, ok := err.(awserr.Error); ok {
												switch aerr.Code() {
												case s3.ErrCodeNoSuchBucket:
													gLog.Error.Printf("Bucket %s is not found - error %v", oper.Bucket, err)
												case s3.ErrCodeNoSuchKey:
													if r, err := writeToS3(svc, oper.Bucket, oper.Key, []byte(oper.Value)); err == nil {
														gLog.Trace.Println(oper.Key, oper.Bucket, *r.ETag, *r)
														mu2.Lock()
														add++
														mu2.Unlock()
													} else {
														gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, oper.Key, oper.Bucket)
														mue.Lock()
														error++
														mue.Unlock()
													}
												default:
													if strings.Contains(err.Error(), "NotFound") {
														if r, err := writeToS3(svc, oper.Bucket, oper.Key, []byte(oper.Value)); err == nil {
															gLog.Trace.Println(oper.Key, oper.Bucket, *r.ETag, *r)
															mu2.Lock()
															add++
															mu2.Unlock()
														} else {
															gLog.Error.Printf("Error %v - Writing key %s to bucket %s", err, oper.Key, oper.Bucket)
															mue.Lock()
															error++
															mue.Unlock()
														}
													} else {
														gLog.Error.Printf("Error %v - Checking key %s in bucket %s", err.Error(), oper.Key, oper.Bucket)
														mue.Lock()
														error++
														mue.Unlock()
													}
												}
											}
										}
									}
								} else {
									gLog.Info.Printf("Checking mode: Writing Key/Value %s/%s - to bucket %s", oper.Key, oper.Value, oper.Bucket)
								}
							} else {
								if oper.Oper == "DELETE" {
									gLog.Trace.Println(oper.Bucket, oper.Oper, oper.Key)
									if !check {
										delreq := datatype.DeleteObjRequest{
											Service: svc,
											Bucket:  oper.Bucket,
											Key:     oper.Key,
										}
										// api.DeleteObjects(delreq)
										if ret, err := api.DeleteObjects(delreq); err == nil {
											gLog.Warning.Printf("Object %s is deleted from %s - %v", oper.Key, oper.Bucket, ret.DeleteMarker)
											mu1.Lock()
											delete++
											mu1.Unlock()

										} else {
											gLog.Error.Printf("Error %v  while deleting %s in %s", err, oper.Key, oper.Bucket)
										}
									} else {
										gLog.Info.Printf("Checking mode: Deleting object key %s from bucket %s", oper.Key, oper.Bucket)
									}
								} else {
									gLog.Warning.Printf("Invalid operation %s in input line %s", oper.Oper, v)
								}
							}
						} else {
							gLog.Error.Printf("Error: %v while parsing sindexd log entry: %s", err, v)
						}
					}(v, svc)
				}
				wg.Wait()
				gLog.Info.Printf("Total processed: %d - Added :%d - Skipped: %d - Deleted: %d - Errors: %d - Duration: %v", total, add, skip, delete, error, time.Since(start))
			}
		} else {
			stop = true
		}
	}
	gLog.Info.Printf("Total processed: %d - Added :%d - Skipped: %d - Deleted: %d - Errors: %d - Duration: %v", total, add, skip, delete, error, time.Since(start))
}
