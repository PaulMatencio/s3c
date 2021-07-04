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
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command

var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Command to compare Moses objects",
		Long:  `Check a restored blobs against its source blobs`,
		Run:   Check,
	}

	np, status, srcUrl, targetUrl string
	err                           error
	driver, targetDriver         string
	env, targetEnv    string

)

func initCkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&pn, "pn", "k", "", "key of the the publication number cc/pn/kc; cc=country code;pn=publication number;kc=Kind code")
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source S3 bucket")
	// cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of target S3 bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 20, "maximum number of documents (keys) to be backed up concurrently -Check --maxpage for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "prefix  delimiter")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages per document. check  --maxKey  for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")

}
func init() {
	rootCmd.AddCommand(checkCmd)
	initCkFlags(checkCmd)
}

func Check(cmd *cobra.Command, args []string) {
	mosesbc.SetSourceSproxyd("check",srcUrl,driver,env)
	mosesbc.SetTargetSproxyd("check",targetUrl,targetDriver,targetEnv)
	gLog.Info.Println(sproxyd.TargetEnv,sproxyd.TargetDriver,sproxyd.TargetUrl)
	if len(pn) > 0 {
		ChekBlob1(pn)
	} else if len(prefix) > 0 {
		if len(srcBucket) == 0 {
			gLog.Error.Printf("Source S3  bucket is missing")
			return
		}
		CheckBlobs(srcBucket,marker,prefix,maxKey,maxPage,maxLoop)
	} else {
		gLog.Error.Printf("Both publication number (pn)  and  prefix are missing")
		return
	}
}

/*
	called by Check()  for a given publication number
 */
func ChekBlob1(pn string) {
	if np, err, status := mosesbc.GetPageNumber(pn); err == nil && status == 200 {
		if np > 0 {
			mosesbc.CheckBlob1(pn, np, maxPage)
		} else {
			gLog.Error.Printf("The number of pages is %d ", np)
		}
	} else {
		gLog.Error.Printf("Error %v getting the number of pages", err)
	}
}

/*
	called by Check() for a given prefix
    the mSe etadata will be used to list all the publication for a given prefix
 */

func CheckBlobs(bucket string, marker string,prefix string,maxKey int64,maxPage int,maxLoop int) {

	if srcS3:= mosesbc.CreateS3Session("check","source"); srcS3 != nil {
		request := datatype.ListObjRequest{
			Service: srcS3,
			Bucket:  bucket,
			Prefix:  prefix,
			MaxKey:  maxKey,
			Marker:  marker,
			// Delimiter: delimiter,
		}
		mosesbc.CheckBlobs(request, maxLoop, maxPage)
	} else {
		gLog.Error.Printf("Failed to create a S3 source session")
	}
}

