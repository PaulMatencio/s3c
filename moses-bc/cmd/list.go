// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	// "github.com/golang/gLog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command

var (
	loshort       = "Command to list objects  of a given bucket"
	listObjectCmd = &cobra.Command{
		Use:    "List-objects",
		Short:  loshort,
		Long:   ``,
		Hidden: true,
		Run:    ListObjectV2,
	}
	listBlobCmd = &cobra.Command{
		Use:    "List-blobs",
		Short:  loshort,
		Long:   ``,
		Hidden: true,
		Run:    ListBlobs,
	}
	lbCmd = &cobra.Command{
		Use:    "List-buckets",
		Short:  "list buckets of a given S3 location",
		Hidden: true,
		Long:   ``,
		Run:    ListBuckets,
	}

	lvCmd = &cobra.Command{
		Use:    "List-object-versions",
		Short:  "Command to list objects and their versions of a given bucket",
		Long:   ``,
		Hidden: true,
		Run:    ListObjVersions,
	}
	location string
	prefixs  string
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "K", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&location, "location", "", "backup", "S3 location - possible value [source|backup|clone]")
}

func initBlFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "K", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent pages per document. check  --max-key for maximum number of concurrent documents")
	cmd.Flags().StringVarP(&location, "location", "", "backup", "S3 location - possible value [source|backup|clone]")
	/*
		cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
		cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
		cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	*/
}
func initLvFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "key-marker", "K", "", "start processing from this key")
	cmd.Flags().StringVarP(&versionId, "version-id", "", "", "start processing from this version id")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&location, "location", "", "target", "S3 location - possible value [source|target]")
}

func initListS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&prefixs, "prefixs", "", "", "list of the key prefixes separated by a commma")
	cmd.Flags().StringVarP(&marker, "key-marker", "K", "", "Start with this marker (Key) for the Get Prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "delimiter character")
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the S3  bucket")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "L", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&location, "location", "", "target", "S3 location - possible value [source|target]")
	// cmd.Flags().StringVarP(&index, "index", "i", "pn", "bucket group [pn|pd|bn]")
}
func initLbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&location, "location", "", "backup", "S3 location - possible value [source|backup|clone]")
}

func init() {
	rootCmd.AddCommand(listObjectCmd)
	rootCmd.AddCommand(listBlobCmd)
	rootCmd.AddCommand(lbCmd)
	rootCmd.AddCommand(lvCmd)
	initLoFlags(listObjectCmd)
	initBlFlags(listBlobCmd)
	initLbFlags(lbCmd)
	initLvFlags(lvCmd)
}

func createS3Session(location string) (error, *s3.S3) {

	if location != "source" && location != "backup" && location != "clone" && location != "migrate" {
		return errors.New("location must be [source|backup|clone|migrate]"), nil
	}
	if s3 := mosesbc.CreateS3Session("list", location); s3 != nil {
		return nil, s3
	} else {
		return errors.New(fmt.Sprintf("Failed to create a session for %s S3", location)), nil
	}

}

func ListBlobs(cmd *cobra.Command, args []string) {

	var (
		req     datatype.ListObjRequest
		service *s3.S3
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}

	if len(prefix) > 0 {
		if err, suf := mosesbc.GetBucketSuffix(bucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				bucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, bucket)
			}
		}
	}
	srcUrl = ""
	driver = ""
	env = ""
	if err = mosesbc.SetLocationSproxyd("list", location, srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)

	if err, service = createS3Session(location); err == nil {
		req = datatype.ListObjRequest{
			Service:   service,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			Delimiter: delimiter,
		}
		mosesbc.ListBlobs(req, maxLoop, maxPage)
	} else {
		gLog.Error.Printf("%v", err)
		return
	}
}


func ListObjectV2(cmd *cobra.Command, args []string) {

	var (
		start   = utils.LumberPrefix(cmd)
		token   string
		req     datatype.ListObjV2Request
		service *s3.S3
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		utils.Return(start)
		return
	}

	if len(prefix) > 0 {
		if err, suf := mosesbc.GetBucketSuffix(bucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				bucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, bucket)
			}
		}
	}

	if err, service = createS3Session(location); err == nil {
		req = datatype.ListObjV2Request{
			Service:           service,
			Bucket:            bucket,
			Prefix:            prefix,
			MaxKey:            maxKey,
			Marker:            marker,
			Continuationtoken: token,
			Delimiter:         delimiter,
		}
	} else {
		gLog.Error.Printf("%v", err)
		return
	}
	if len(delimiter) == 0 {
		listV2Prefix(req)
	} else {
		listV2CommonPrefix(req)
	}
}


func ListObjVersions(cmd *cobra.Command, args []string) {
	var (
		service *s3.S3
		req     datatype.ListObjVersionsRequest
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if err, service = createS3Session(location); err == nil {
		req = datatype.ListObjVersionsRequest{
			Service:         service,
			Bucket:          bucket,
			Prefix:          prefix,
			MaxKey:          maxKey,
			KeyMarker:       marker,
			VersionIdMarker: versionId,
			Delimiter:       delimiter,
		}
	} else {
		gLog.Error.Printf("%v", err)
		return
	}
	listVersions(req)
}


func ListBuckets(cmd *cobra.Command, args []string) {
	if err, service = createS3Session(location); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}
	req := datatype.ListBucketRequest{
		Service: service,
	}
	if result, err := api.ListBucket(req); err != nil {
		gLog.Error.Printf("%v", err)
	} else {
		gLog.Info.Printf("Owner of the buckets: %s", result.Owner)
		for _, v := range result.Buckets {
			gLog.Info.Printf("Bucket Name: %s - Creation date: %s", *v.Name, v.CreationDate)
		}
	}
}

func listV2Prefix(req datatype.ListObjV2Request) {

	var (
		L          = 1
		total      = 0
		nextmarker string
		token      string
		result *s3.ListObjectsV2Output
		err    error
	)
	for {
		/*
		var (
			result *s3.ListObjectsV2Output
			err    error
		)
		 */
		if result, err = api.ListObjectV2(req); err == nil {
			if l := len(result.Contents); l > 0 {

				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						total += 1
						gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
					}
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					token = *result.NextContinuationToken
					gLog.Warning.Printf("Truncated %v - Next marker: %s - Next continuation token: %s", *result.IsTruncated, nextmarker, token)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		L++
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Continuationtoken = token

		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}
}

func listV2CommonPrefix(req datatype.ListObjV2Request) {

	var (
		L          = 1
		total      = 0
		nextmarker string
		token      string
		result *s3.ListObjectsV2Output
		err    error
	)
	for {
		/*
		var (
			result *s3.ListObjectsV2Output
			err    error
		)
		 */
		if result, err = api.ListObjectV2(req); err == nil {
			if l := len(result.Contents); l > 0 {

				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						total += 1
						gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
					}
				}
				// list the common prefixes
				if len(result.CommonPrefixes) > 0 {
					gLog.Info.Println("List Common prefix:")
					for _, v := range result.CommonPrefixes {
						gLog.Info.Printf("%s", *v.Prefix)
					}
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					token = *result.NextContinuationToken
					gLog.Warning.Printf("Truncated %v - Next marker: %s - Next continuation token: %s", *result.IsTruncated, nextmarker, token)
				}
			}
		}
		L++
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Continuationtoken = token

		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}
}

func listVersions(req datatype.ListObjVersionsRequest) {
	var (
		L                   = 1
		total               = 0
		nextMarker          string
		nextVersionIdMarker string
		result *s3.ListObjectVersionsOutput
		err    error
	)
	for {
		/*
		var (
			result *s3.ListObjectVersionsOutput
			err    error
		)

		 */
		if result, err = api.ListObjectVersions(req); err == nil {
			if l := len(result.Versions); l > 0 {
				for _, v := range result.Versions {
					gLog.Info.Printf("Key: %s - Size: %d  - Version id: %s - LastModified: %v - isLatest: %v", *v.Key, *v.Size, *v.VersionId, v.LastModified, *v.IsLatest)
					total += 1
				}
				if *result.IsTruncated {
					nextMarker = *result.Versions[l-1].Key
					// nextMarker = *result.NextKeyMarker
					// nextVersionIdMarker = *result.VersionIdMarker
					nextVersionIdMarker = *result.Versions[l-1].VersionId
					gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextMarker)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		L++
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.VersionIdMarker = nextVersionIdMarker
			req.KeyMarker = nextMarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}
}
