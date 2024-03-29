
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
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/cobra"
	"strings"
)

// listObjectCmd represents the listObject command

var (
	testCmd = &cobra.Command{
		Use:   "test-sproxyd-replication",
		Short: "Command to put/get/delete objects by path Id",
		Long:  `Command to put/get/delete objects objets using their path Id`,
		Hidden: true,
		Run:   TestReplication,
	}

	method string

)

func initPoIdFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&pn, "pn", "k", "", "key of the the publication number cc/pn/kc; cc=country code;pn=publication number;kc=Kind code")
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source S3 bucket without its suffix 00..05")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of target S3 bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 20, "maximum number of documents (keys) to be backed up concurrently -Check --maxpage for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "prefix  delimiter")
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, "replace the existing target moses pages")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages per document. check  --maxKey  for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")
	cmd.Flags().BoolVarP(&check, "check", "v", true, "Run in Checking  mode")
	cmd.Flags().StringVarP(&method, "method", "", "put", "test ")

}


func init() {
	rootCmd.AddCommand(testCmd)
	initPoIdFlags(testCmd)
}

func TestReplication(cmd *cobra.Command, args []string) {
	var err error
	if err = mosesbc.SetSourceSproxyd("check",srcUrl,driver,env); err != nil {
		return
	}
	if err = mosesbc.SetTargetSproxyd("check",targetUrl,targetDriver,targetEnv) ; err != nil {
		return
	}
	if len(prefix) > 0 {
		if len(srcBucket) == 0 {
			gLog.Error.Printf("Source S3  bucket is missing")
			return
		}
		OpById(strings.ToLower(method), srcBucket,marker,prefix,maxKey,maxLoop,check)
	} else {
		gLog.Error.Printf("--prefix is missing")
	}
}

func OpById(method string , bucket string, marker string,prefix string,maxKey int64,maxLoop int,check bool) {

	if err, suf := mosesbc.GetBucketSuffix(bucket, prefix); err != nil {
		gLog.Error.Printf("%v", err)
		return
	} else {
		if len(suf) > 0 {
			bucket += "-" + suf
			gLog.Warning.Printf("A suffix %s is appended to the source bucket %s", suf, bucket)
		}
	}
	if srcS3:= mosesbc.CreateS3Session("check","source"); srcS3 != nil {
		request := datatype.ListObjRequest{
			Service: srcS3,
			Bucket:  bucket,
			Prefix:  prefix,
			MaxKey:  maxKey,
			Marker:  marker,
			// Delimiter: delimiter,
		}
		mosesbc.OpByIds(method,request, maxLoop, replace,check)
	} else {
		gLog.Error.Printf("Failed to create a S3 source session")
	}
}

