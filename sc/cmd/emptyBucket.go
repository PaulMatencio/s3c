package cmd

import (
	"fmt"
	"github.com/paulmatencio/s3c/gLog"

	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"time"
)

var (
	ebshort = "Command to delete multiple objects  concurrently"
	empty   bool
	ebCmd   = &cobra.Command{
		Use:   "delObjs",
		Short: ebshort,
		Long:  ``,
		// 	Hidden: true,
		Run: deleteObjects,
	}
	dvCmd = &cobra.Command{
		Use:   "delObjVersions",
		Short: "Command to delete all objects versions",
		Long:  ``,
		Run:   deleteObjectVersions,
	}
)

func initLv1Flags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "key-marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&versionId, "version-id", "", "", "start processing from this version id")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&fromDate, "from-date", "", "2019-01-01T00:00:00Z", "delete objects from last modified from <yyyy-mm-ddThh:mm:ss>")
	cmd.Flags().StringVarP(&toDate, "to-date", "", "", "delete objects to last modified from <yyyy-mm-ddThh:mm:ss>")
	cmd.Flags().BoolVarP(&empty, "empty", "", false, "empty the bucket")
	cmd.Flags().BoolVarP(&check, "check", "", true, "run in check mode")

}
func init() {

	RootCmd.AddCommand(dvCmd)
	RootCmd.AddCommand(ebCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLv1Flags(ebCmd)
	initLv1Flags(dvCmd)
}

func deleteObjects(cmd *cobra.Command, args []string) {

	var (
		start = utils.LumberPrefix(cmd)
		N, T  = 0, 0
	)

	type Rd struct {
		Key    string
		LastRef  time.Time
		Result *s3.DeleteObjectOutput
		Err    error
	}

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s", missingBucket)
		utils.Return(start)
		return
	}

	if len(toDate) > 0 {
		if lastDate, err = time.Parse(time.RFC3339, toDate); err != nil {
			gLog.Error.Printf("Invalid date format %s", toDate)
			return
		}
	} else {
		lastDate = time.Now().AddDate(0, 0, dayToAdd)
	}

	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Invalid date format %s", frDate)
		return
	}

	req := datatype.ListObjRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKey:  maxKey,
		Marker:  marker,
	}
	ch := make(chan *Rd)
	var (
		nextMarker string
		result     *s3.ListObjectsOutput
		err        error
		// rd Rd
		l int
	)
	for {
		L := 1
		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {
				// N = len(result.Contents)
				N = 0
				T = 0
				for _, v := range result.Contents {
					if v.LastModified.Before(lastDate) && v.LastModified.After(frDate) {
						N++
						del := datatype.DeleteObjRequest{
							Service: req.Service,
							Bucket:  req.Bucket,
							Key:     *v.Key,
						}
						go func(request datatype.DeleteObjRequest) {

							rd := Rd {
								Key: del.Key,
								LastRef: *v.LastModified,
							}
							if !check {
								rd.Result, rd.Err = api.DeleteObjects(del)
							} else {
								rd.Err = nil
							}
							del = datatype.DeleteObjRequest{} // reset the structure to free memory
							ch <- &rd

						}(del)

					}
				}

				done := false
				if T == N {
					done = true
				}
				for ok := true; ok; ok = !done {
					select {
					case rd := <-ch:
						T++
						if rd.Err != nil {
							gLog.Error.Printf("Error %v deleting %s", rd.Err, rd.Key)
						} else {
							// lumber.Trace("Key %s is deleted", rd.Key)
							if !check {
								gLog.Info.Printf(" Key %s has been deleted - Last modified date %v ", rd.Key, rd.LastRef)
							} else {
								gLog.Info.Printf("DryRun: Deleting %s - Last modified date %v ",rd.Key,rd.LastRef)
							}
						}
						rd = &Rd{} // reset the structure to free memory

						if T == N {
							//utils.Return(start)
							gLog.Info.Printf("So far %d objects have been deleted ", N)
							done = true
						}

					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			} else {
				gLog.Warning.Printf("Bucket %s is empty", bucket)
			}
		} else {
			gLog.Error.Printf("ListObjects err %v", err)
			break
		}
		L++
		if *result.IsTruncated {

			nextMarker = *result.Contents[l-1].Key
			gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextMarker)

		}
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextMarker

		} else {
			break
		}
	}

	utils.Return(start)
}

func deleteObjectVersions(cmd *cobra.Command, args []string) {

	var (
		start = utils.LumberPrefix(cmd)
		N, T  = 0, 0
	)

	type Rd struct {
		Key    string
		Result *s3.DeleteObjectOutput
		Err    error
	}

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s", missingBucket)
		utils.Return(start)
		return
	}
	if len(toDate) > 0 {
		if lastDate, err = time.Parse(ISOLayout, toDate); err != nil {
			gLog.Error.Printf("Invalid date format %s", toDate)
			return
		}
	} else {
		lastDate = time.Now().AddDate(0, 0, dayToAdd)
	}

	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Invalid date format %s", frDate)
		return
	}
	req := datatype.ListObjVersionsRequest{
		Service:         s3.New(api.CreateSession()),
		Bucket:          bucket,
		Prefix:          prefix,
		MaxKey:          maxKey,
		KeyMarker:       marker,
		VersionIdMarker: versionId,
		Delimiter:       delimiter,
	}
	ch := make(chan *Rd)
	var (
		nextMarker          string
		nextVersionIdMarker string
		result              *s3.ListObjectVersionsOutput
		err                 error
		l                   int
	)
	for {
		L := 1
		if result, err = api.ListObjectVersions(req); err == nil {

			if l = len(result.Versions); l > 0 {

				// N = len(result.Versions)
				N = 0
				T = 0

				for _, v := range result.Versions {
					//lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
					//  delete the object
					if v.LastModified.Before(lastDate) && v.LastModified.After(frDate) {
						if !*v.IsLatest || empty {
							N++
							del := datatype.DeleteObjRequest{
								Service:   req.Service,
								Bucket:    req.Bucket,
								Key:       *v.Key,
								VersionId: *v.VersionId,
							}
							go func(request datatype.DeleteObjRequest) {

								rd := Rd{
									Key: del.Key,
								}
								rd.Result, rd.Err = api.DeleteObjects(del)
								del = datatype.DeleteObjRequest{} // reset the structure to free memory
								ch <- &rd

							}(del)
						}
					}
				}

				done := false
				for ok := true; ok; ok = !done {
					select {
					case rd := <-ch:
						T++
						if rd.Err != nil {
							gLog.Error.Printf("Error %v deleting %s - VersionId %s ", rd.Err, rd.Key, *rd.Result.VersionId)
						} else {
							// lumber.Trace("Key %s is deleted", rd.Key)
							gLog.Trace.Printf("Key %s - Version id %s  has been deleted", rd.Key, *rd.Result.VersionId)
						}

						rd = &Rd{} // reset the structure to free memory

						if T == N {
							//utils.Return(start)
							gLog.Info.Printf("So far %d objects have been deleted", N)
							done = true
						}

					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			} else {
				gLog.Warning.Printf("Bucket %s is empty", bucket)
			}
		} else {
			gLog.Error.Printf("ListObjects err %v", err)
			break
		}
		L++
		if *result.IsTruncated {

			nextMarker = *result.Versions[l-1].Key
			nextVersionIdMarker = *result.Versions[l-1].VersionId
			gLog.Warning.Printf("Truncated %v  - Next marker : %s - Next versionId marker %s", *result.IsTruncated, nextMarker, nextVersionIdMarker)

		}
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.KeyMarker = nextMarker
			req.VersionIdMarker = nextVersionIdMarker

		} else {
			break
		}
	}

	utils.Return(start)
}
