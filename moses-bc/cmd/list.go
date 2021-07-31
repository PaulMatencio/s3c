package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"

	"errors"
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
		Use:    "list-objects",
		Short:  loshort,
		Long:   ``,
		Hidden: true,
		Run:    listObjectV2,
	}

	lbCmd = &cobra.Command{
		Use:    "list-buckets",
		Short:  "list buckets of a given S3 location",
		Hidden: true,
		Long:   ``,
		Run:    listBucket,
	}

	lvCmd = &cobra.Command{
		Use:    "list-object-versions",
		Short:  "Command to list objects and their versions of a given bucket",
		Long:   ``,
		Hidden: true,
		Run:    listObjVersions,
	}
	location string
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&location, "location", "", "backup", "S3 location - possible value [source|backup|clone]")
}
func initLvFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "key-marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&versionId, "version-id", "", "", "start processing from this version id")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&location, "location", "", "target", "S3 location - possible value [source|target]")
}

func initLbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&location, "location", "", "backup", "S3 location - possible value [source|backup|clone]")
}

func init() {
	rootCmd.AddCommand(listObjectCmd)
	rootCmd.AddCommand(lbCmd)
	rootCmd.AddCommand(lvCmd)
	initLoFlags(listObjectCmd)
	initLbFlags(lbCmd)
	initLvFlags(lvCmd)
}

func listObject(cmd *cobra.Command, args []string) {
	var (
		total      int64 = 0
		nextmarker string
		req        datatype.ListObjRequest
		service    *s3.S3
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}

	if err, service = createS3Session(location); err == nil {
		req = datatype.ListObjRequest{
			Service:   service,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			Delimiter: delimiter,
		}
	} else {
		gLog.Error.Printf("%v", err)
		return
	}

	L := 1
	for {
		var (
			result *s3.ListObjectsOutput
			err    error
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
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}

}

func listObjectV2(cmd *cobra.Command, args []string) {
	var (
		start            = utils.LumberPrefix(cmd)
		total      int64 = 0
		token      string
		nextmarker string
		req        datatype.ListObjV2Request
		service    *s3.S3
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		utils.Return(start)
		return
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
	L := 1
	for {
		var (
			result *s3.ListObjectsV2Output
			err    error
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

func listObjVersions(cmd *cobra.Command, args []string) {
	var (
		total               int64 = 0
		nextMarker          string
		nextVersionIdMarker string
		service             *s3.S3
		req                 datatype.ListObjVersionsRequest
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
	L := 1
	for {
		var (
			result *s3.ListObjectVersionsOutput
			err    error
		)
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

func listBucket(cmd *cobra.Command, args []string) {

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

func createS3Session(location string) (error, *s3.S3) {

	if location != "source" && location != "backup" && location != "clone" {
		return errors.New("location must be [source|backup|clone]"), nil
	}

	if s3 := mosesbc.CreateS3Session("list", location); s3 != nil {
		return nil, s3
	} else {
		return errors.New(fmt.Sprintf("Failed to create a session for %s S3", location)), nil
	}

}
