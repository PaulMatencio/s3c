
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

	ebCmd = &cobra.Command{
		Use:   "delObjs",
		Short: ebshort,
		Long: ``,
	// 	Hidden: true,
		Run: deleteObjects,
	}
	dvCmd = &cobra.Command{
		Use:   "delObjVersions",
		Short: "Command to delete all objects versions",
		Long: ``,
		Run: deleteObjectVersions,
	}
)


func init() {

	RootCmd.AddCommand(dvCmd)
	RootCmd.AddCommand(ebCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLoFlags(ebCmd)
	initLvFlags(dvCmd)
}

func deleteObjects(cmd *cobra.Command,args []string) {

	var (
		start= utils.LumberPrefix(cmd)
		N,T = 0,0
	)

	type  Rd struct {
		Key string
		Result   *s3.DeleteObjectOutput
		Err error
	}


	if len(bucket) == 0 {

		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	ch:= make(chan *Rd)
	var (
		nextMarker string
		result  *s3.ListObjectsOutput
		err error
		// rd Rd
		l  int
	)
	for {
		L:= 1
		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				T = 0

				for _, v := range result.Contents {
					//lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
					//  delete the object
					del := datatype.DeleteObjRequest{
						Service: req.Service,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.DeleteObjRequest) {

						rd := Rd{
							Key : del.Key,
						}
						rd.Result, rd.Err = api.DeleteObjects(del)
						del = datatype.DeleteObjRequest{} // reset the structure to free memory
						ch <- &rd

					}(del)

				}

				done:= false
				for ok:=true;ok;ok=!done {
					select {
					case rd := <-ch:

						T++

						if rd.Err != nil {
							gLog.Error.Printf("Error %v deleting %s", rd.Err, rd.Key)
						} else {
							// lumber.Trace("Key %s is deleted", rd.Key)
						}

						rd = &Rd{} // reset the structure to free memory

						if T == N {
							//utils.Return(start)
							gLog.Info.Printf("Deleting .... %d objects ",N)
							done = true
						}

					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}  else {
				gLog.Warning.Printf("Bucket %s is empty", bucket)
			}
		} else {
			gLog.Error.Printf("ListObjects err %v",err)
			break
		}
        L++
		if *result.IsTruncated {

			nextMarker = *result.Contents[l-1].Key
			gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextMarker)

		}
		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextMarker

		} else {
			break
		}
	}

	utils.Return(start)
}

func deleteObjectVersions(cmd *cobra.Command,args []string) {

	var (
		start= utils.LumberPrefix(cmd)
		N,T = 0,0
	)

	type  Rd struct {
		Key string
		Result   *s3.DeleteObjectOutput
		Err error
	}


	if len(bucket) == 0 {

		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	req := datatype.ListObjVersionsRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		KeyMarker : marker,
		VersionIdMarker: versionId,
		Delimiter: delimiter,
	}
	ch:= make(chan *Rd)
	var (
		nextMarker string
		nextVersionIdMarker string
		result  *s3.ListObjectVersionsOutput
		err error
		l  int
	)
	for {
		L:= 1
		if result, err = api.ListObjectVersions(req); err == nil {

			if l = len(result.Versions); l > 0 {

				N = len(result.Versions)
				T = 0

				for _, v := range result.Versions {
					//lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
					//  delete the object
					del := datatype.DeleteObjRequest{
						Service: req.Service,
						Bucket:  req.Bucket,
						Key:     *v.Key,
						VersionId: *v.VersionId,
					}
					go func(request datatype.DeleteObjRequest) {

						rd := Rd{
							Key : del.Key,
						}
						rd.Result, rd.Err = api.DeleteObjects(del)
						del = datatype.DeleteObjRequest{} // reset the structure to free memory
						ch <- &rd

					}(del)

				}

				done:= false
				for ok:=true;ok;ok=!done {
					select {
					case rd := <-ch:
						T++
						if rd.Err != nil {
							gLog.Error.Printf("Error %v deleting %s", rd.Err, rd.Key)
						} else {
							// lumber.Trace("Key %s is deleted", rd.Key)
						}

						rd = &Rd{} // reset the structure to free memory

						if T == N {
							//utils.Return(start)
							gLog.Info.Printf("Deleting .... %d objects ",N)
							done = true
						}

					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}  else {
				gLog.Warning.Printf("Bucket %s is empty", bucket)
			}
		} else {
			gLog.Error.Printf("ListObjects err %v",err)
			break
		}
		L++
		if *result.IsTruncated {

			nextMarker = *result.Versions[l-1].Key
			nextVersionIdMarker = *result.Versions[l-1].VersionId
			gLog.Warning.Printf("Truncated %v  - Next marker : %s - Next versionId marker %s", *result.IsTruncated, nextMarker,nextVersionIdMarker)

		}
		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.KeyMarker = nextMarker
			req.VersionIdMarker =nextVersionIdMarker

		} else {
			break
		}
	}

	utils.Return(start)
}


