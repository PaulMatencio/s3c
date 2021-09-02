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

/*
	Check "clone" and "restore"
		Compare the cloned or restored objects with the source objects
*/

var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Command to compare moses data and directory between two Moses instances.",
		Long: `Command to compare moses data and directory between two Moses instances.
        As for instance  comparing restore vs source instances.
        It will compare directory,data and metadata of the given key or those keys that begin with the specified prefix`,
		Run: Check,
	}

	np, status, srcUrl, targetUrl string
	err                           error
	driver, targetDriver          string
	env, targetEnv                string
	checkDocsMeta                 bool
)

func initCkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&pn, "pn", "k", "", "key of the the publication number cc/pn/kc; cc=country code;pn=publication number;kc=Kind code")
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source S3 bucket without its suffix 00..05")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of documents (keys) to be backed up concurrently -Check --max-page for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "prefix  delimiter")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent pages per document. check  --max-key for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")
	cmd.Flags().BoolVarP(&checkDocsMeta, "check-docs-meta", "", false, "check document metadata ")
}

func init() {
	rootCmd.AddCommand(checkCmd)
	initCkFlags(checkCmd)
}

func Check(cmd *cobra.Command, args []string) {

	var err error
	if err = mosesbc.SetSourceSproxyd("check", srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	if err = mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)
	gLog.Info.Printf("Target Env: %s - Target Driver: %s - Target Url: %s", sproxyd.TargetEnv, sproxyd.TargetDriver, sproxyd.TargetUrl)

	if len(srcBucket) == 0 {
		gLog.Error.Printf("Source bucket is missing")
		return
	}

	if len(pn) > 0 {
		//  check just a document
		chekBlob()
	} else if checkDocsMeta {
		//  check only documents metadata
		if len(prefix) == 0 {
			if !mosesbc.HasSuffix(srcBucket) {
				gLog.Error.Printf("if --prefix is missing then please provide a suffix [00..05]to the bucket %s", srcBucket)
				return
			}
		} else {
			if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
				gLog.Error.Printf("%v", err)
				return
			} else {
				if len(suf) > 0 {
					srcBucket += "-" + suf
					gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
				}
			}
		}
		checkDoc(srcBucket)

	} else if len(prefix) > 0 {
		/* check  documents metadata and their  pages */
		checkBlobs(srcBucket)
	} else {
		gLog.Error.Printf("Both publication number --pn   and  --prefix arguments are missing. Please specify either --pn or --prefix or --help for help")
		return
	}
}

/*
	called by Check()  for a given publication number
*/

func chekBlob() {
	if np, err, status := mosesbc.GetPageNumber(pn); err == nil && status == 200 {
		if np > 0 {
			mosesbc.CheckBlob(pn, np, maxPage)
		} else {
			gLog.Error.Printf("The number of pages is %d ", np)
		}
	} else {
		gLog.Error.Printf("Error %v getting the number of pages", err)
	}
}

/*
	called by Check() for a given prefix
    the mose metadata buckets are used to list all the publication for a given prefix
*/

// func checkBlobs(bucket string, marker string, prefix string, maxKey int64, maxPage int, maxLoop int) {
func checkBlobs(bucket string) {

	if err, suf := mosesbc.GetBucketSuffix(bucket, prefix); err != nil {
		gLog.Error.Printf("%v", err)
		return
	} else {
		if len(suf) > 0 {
			bucket += "-" + suf
			gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, bucket)
		}
	}

	if srcS3 := mosesbc.CreateS3Session("check", "source"); srcS3 != nil {
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

func checkDoc(bucket string) {
	//  create a session to the source S3
	//  list the source bucket
	if srcS3 := mosesbc.CreateS3Session("check", "source"); srcS3 != nil {
		request := datatype.ListObjRequest{
			Service: srcS3,
			Bucket:  bucket,
			Prefix:  prefix,
			MaxKey:  maxKey,
			Marker:  marker,
			// Delimiter: delimiter,
		}
		mosesbc.CheckDocs(request, maxLoop)
	} else {
		gLog.Error.Printf("Failed to create a S3 source session")
	}
}
