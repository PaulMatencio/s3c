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
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// readLogCmd represents the readLog command
var (
	logFile string
	maxLine int
	parseLogCmd = &cobra.Command{
		Use:   "parseLog",
		Short: "parse a preformated sindexd log file",
		Long: `
         The sindexd log must be preformated as following  
		 input record  format: Month day hour ADD|DELETE index_id: [Index-id] key:[key] value: [value]
 		 month: [Jan|Feb|Mar|Apr......|Dec]
		 day:0..31
		 hour: hh:mm:ss
		`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("parsing sindexd log")
			parseLog(cmd,args)
		},
	}
)

func init() {
	rootCmd.AddCommand(parseLogCmd)
	parseLogCmd.Flags().StringVarP(&logFile,"logFile","i","","sindexd input log file")
	parseLogCmd.Flags().IntVarP(&maxLine,"maxline","m",10,"maximum number of lines")
	parseLogCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the prefix of the S3  bucket names")
}

func parseLog(cmd *cobra.Command,args []string) {

	if len(logFile) == 0 {
		usage(cmd.Name())
		return
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket);
			return
		}
	}
	ParseLog(logFile,maxKey,bucket)

	/*
	if scanner, err = utils.Scanner(logFile); err != nil {
		gLog.Error.Printf("Error scanning %v file %s",err,logFile)
		return
	}

	if linea, _ := utils.ScanLines(scanner, int(maxKey)); len(linea) > 0 {
		if l := len(linea); l > 0 {
			for _, v := range linea {
				oper :=parseSindexdLog(v ,idxMap, bucket,bucketNumber)
				gLog.Info.Println(oper.Oper,oper.Bucket,oper.Key,oper.Value)
			}
		}
	}
	*/

}

func ParseLog(logFile string,maxKey int,bucket string) {

	var (
		scanner *bufio.Scanner
		err     error
		idxMap  = buildIdxMap()
	)

	if scanner, err = utils.Scanner(logFile); err != nil {
		gLog.Error.Printf("Error scanning %v file %s", err, logFile)
		return
	}
	stop := false
	for (!stop) {
		if linea, _ := utils.ScanLines(scanner, int(maxKey)); len(linea) > 0 {
			for _, v := range linea {
				if err,oper := parseSindexdLog(v, idxMap, bucket, bucketNumber);err ==nil {
					gLog.Info.Println(oper.Oper, oper.Bucket, oper.Key, oper.Value)
				} else {
					gLog.Error.Printf("Error: %v while parsing sindexd log entry: %s",err,v)
				}
			}
		} else {
			stop = true
		}
	}
}
