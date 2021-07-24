package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	// "github.com/golang/gLog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command
var (
	loshort = "Command to list multiple objects in a given bucket"
	listObjectCmd = &cobra.Command{
		Use:   "lsObjs",
		Short: loshort,
		Long: ``,
		// Hidden: true,
		Run: listObject,
	}

	loCmd = &cobra.Command{
		Use:   "lsObjV2",
		Short: loshort,
		Hidden: true,
		Long: ``,
		Run: listObjectV2,
	}
	lvCmd = &cobra.Command{
		Use:   "lsObjVersions",
		Short: "Command to list objects and their versions in a given bucket",
		Long: ``,
		Run: listObjVersions,
	}
)

var (
	prefix string
	maxKey  int64
	marker  string
	versionId string
	maxLoop int
	delimiter string
	loop,full,R  bool
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker,"marker","M","","start processing from this key")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	// cmd.Flags().BoolVarP(&,"maxLoop","",false,"maximum number of loop")
	cmd.Flags().BoolVarP(&full,"fullKey","F",false,"given prefix is a full documemt key")

	cmd.Flags().BoolVarP(&R,"reverse","R",false,"Reverse the prefix")

}
func initLvFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"max-key","m",100,"maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker,"key-marker","M","","start processing from this key")
	cmd.Flags().StringVarP(&versionId,"version-id","","","start processing from this version id")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"max-loop","",1,"maximum number of loop, 0 means no upper limit")
	// cmd.Flags().BoolVarP(&,"maxLoop","",false,"maximum number of loop")
	cmd.Flags().BoolVarP(&full,"full-key","F",false,"given prefix is a full documemt key")

	cmd.Flags().BoolVarP(&R,"reverse","R",false,"Reverse the prefix")

}
func init() {

	RootCmd.AddCommand(listObjectCmd)
	RootCmd.AddCommand(loCmd)
	RootCmd.AddCommand(lvCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLoFlags(listObjectCmd)
	initLoFlags(loCmd)
	initLvFlags(lvCmd)
}

func listObject(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
		total int64 = 0
		nextmarker string
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}
	if full {
		bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(prefix,bucketNumber))
	}

	if R {
		prefix = utils.Reverse(prefix)
	}
	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
		Delimiter: delimiter,
	}
	L:=1
	for {
		var (

			result  *s3.ListObjectsOutput
			err error
		)
		if result, err = api.ListObject(req); err == nil {
			if l := len(result.Contents); l > 0 {

				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
						total += 1
					}
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					// nextmarker = *result.NextMarker
					gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		L++
		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}

	utils.Return(start)
}

func listObjectV2(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
		total int64 = 0
		token string
		nextmarker  string

	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}
	if full {
		bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(prefix,bucketNumber))
	}

	if R {
		prefix = utils.Reverse(prefix)
	}
	req := datatype.ListObjV2Request{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
		Continuationtoken:  token,
		Delimiter: delimiter,
	}
	L:=1
	for {
		var (

			result  *s3.ListObjectsV2Output
			err error
		)
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
					token=*result.NextContinuationToken
					gLog.Warning.Printf("Truncated %v - Next marker: %s - Next continuation token: %s", *result.IsTruncated, nextmarker,token)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		L++
		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.Continuationtoken = token

		} else {
			gLog.Info.Printf("Total number of objects returned: %d",total)
			break
		}
	}

	utils.Return(start)
}

func listObjVersions(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
		total int64 = 0
		nextMarker string
		nextVersionIdMarker string
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}
	if full {
		bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(prefix,bucketNumber))
	}

	if R {
		prefix = utils.Reverse(prefix)
	}
	req := datatype.ListObjVersionsRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		KeyMarker : marker,
		Delimiter: delimiter,
	}
	L:=1
	for {
		var (

			result  *s3.ListObjectVersionsOutput
			err error
		)
		if result, err = api.ListObjectVersions(req); err == nil {
			if l := len(result.Versions); l > 0 {

				for _, v := range result.Versions {
						gLog.Info.Printf("Key: %s - Size: %d  - Version id %s - LastModified: %v", *v.Key, *v.Size, *v.VersionId, v.LastModified)
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
		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.VersionIdMarker =  nextVersionIdMarker
			req.KeyMarker = nextMarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}

	utils.Return(start)
}
