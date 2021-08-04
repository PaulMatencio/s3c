
package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	gmshort  = "Command to retieve S3 object metadata"
	gmetaCmd = &cobra.Command{
		Use:    "head-objects",
		Short:  gmshort,
		Long:   ``,
	//	Hidden: true,
		Run:    statObjects,
	}
	odir,pdir  string
	svc,service *s3.S3

)

/*
type  Rd struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}
*/

func init() {

	rootCmd.AddCommand(gmetaCmd)
	initLoFlags(gmetaCmd)
	gmetaCmd.Flags().StringVarP(&odir, "odir", "O", "", "the output directory relative to the home directory")

}

func statObjects(cmd *cobra.Command, args []string) {

	var (
		N, T        = 0, 0
		total int64 = 0
		req datatype.ListObjRequest
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}


	if len(odir) > 0 {
		pdir = filepath.Join(utils.GetHomeDir(), odir)
		utils.MakeDir(pdir)
	}


	if err,service = createS3Session(location); err == nil {
		req = datatype.ListObjRequest{
			Service:   service,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
		}
	} else {
		gLog.Error.Printf("%v",err )
		return
	}

	// ch:= make(chan *datatype.Rh)
	ch := make(chan int)
	var (
		nextmarker string
		result     *s3.ListObjectsOutput
		err        error
		// rd Rd
		l int
	)

	//svc := s3.New(api.CreateSession()) // create another service point for  getting metadata
	/*
	if err,svc = createS3Session(location); err != nil {
		gLog.Error.Printf("%v",err )
		return
	}
	*/

	L := 1
	for {
		if result, err = api.ListObject(req); err == nil {
			if l = len(result.Contents); l > 0 {
				N = len(result.Contents)
				total += int64(N)
				T = 0
				for _, v := range result.Contents {
					head := datatype.StatObjRequest{
						Service: service,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.StatObjRequest) {

						rh := datatype.Rh{
							Key: head.Key,
						}

						rh.Result, rh.Err = api.StatObject(head)

						if rh.Result.VersionId != nil   {
							gLog.Info.Printf("Version id %s", *rh.Result.VersionId)
						}

						procStatResult(&rh)

						// ch <- &rh
						ch <- 1

					}(head)

				}

				done := false

				for ok := true; ok; ok = !done {

					select {
					case <-ch:
						T++
						// procStatResult(rd)
						if T == N {
							gLog.Info.Printf("Getting metadata of %d objects ", N)
							done = true
						}
					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}

		} else {
			gLog.Error.Printf("List-objects error %v  - use list-buckets to list the buckets", err)
			break
		}
		L++
		if *result.IsTruncated {
			nextmarker = *result.Contents[l-1].Key
			//nextmarker = *result.NextMarker
			gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)
		}

		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}

}

func procStatResult(rd *datatype.Rh) {
    var versionId string
	if rd.Err != nil {
		procS3Error(rd.Err)
	} else {
		if len(odir) == 0 {
			if rd.Result.VersionId != nil {
				versionId = *rd.Result.VersionId
			}
			gLog.Trace.Printf("Key: %s - ContentLength: %v - LastModified: %v - Version Id: %v" ,rd.Key ,*rd.Result.ContentLength,*rd.Result.LastModified,versionId)
		}
		procS3Meta(rd.Key,rd.Result.Metadata)
	}
	rd = &datatype.Rh{}
}

func procS3Meta(key string, metad map[string]*string) {

	if len(odir) == 0 {
		if len(metad) >0 {
			utils.PrintUsermd(key, metad)
		}
	} else {
		if len(metad) > 0 {
			pathname := filepath.Join(pdir, strings.Replace(key, string(os.PathSeparator), "_", -1)+".md")
			utils.WriteUserMeta(metad, pathname)
		}
	}
}

func procS3Error(err error) {

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
		default:
			gLog.Error.Printf("error [%v]",aerr.Error())
		}
	} else {
		gLog.Error.Printf("[%v]",err.Error())
	}
}