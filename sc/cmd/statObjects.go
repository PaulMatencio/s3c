package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"path/filepath"
	"time"
)

var (
	gmshort  = "Command to retieve some of the metadata of specific or every object in the bucket"
	gmetaCmd = &cobra.Command{
		Use:    "statObjects",
		Short:  gmshort,
		Long:   ``,
		Hidden: true,
		Run:    statObjects,
	}

	gmCmd = &cobra.Command{
		Use:   "statObjs",
		Short: gmshort,
		Long:  ``,
		Run:   statObjects,
	}

	hmCmd = &cobra.Command{
		Use:   "headObjs",
		Short: gmshort,
		Long:  ``,
		Run:   statObjects,
	}
)

/*
type  Rd struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}
*/

func init() {

	RootCmd.AddCommand(gmetaCmd)
	RootCmd.AddCommand(gmCmd)
	RootCmd.AddCommand(hmCmd)
	RootCmd.MarkFlagRequired("bucket")

	initLoFlags(gmetaCmd)
	initLoFlags(gmCmd)
	initLoFlags(hmCmd)
	gmetaCmd.Flags().StringVarP(&odir, "odir", "O", "", "the output directory relative to the home directory")
	gmCmd.Flags().StringVarP(&odir, "odir", "O", "", "the output directory relative to the home directory")
	hmCmd.Flags().StringVarP(&odir, "odir", "O", "", "the output directory relative to the home directory")
}

func statObjects(cmd *cobra.Command, args []string) {

	var (
		start       = utils.LumberPrefix(cmd)
		N, T        = 0, 0
		total int64 = 0
	)

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s", missingBucket)
		utils.Return(start)
		return
	}

	if len(odir) > 0 {
		pdir = filepath.Join(utils.GetHomeDir(), odir)
		utils.MakeDir(pdir)
	}
	// if prefix is a full document key
	if full {
		bucket = bucket + "-" + fmt.Sprintf("%02d", utils.HashKey(prefix, bucketNumber))
	}

	if R {
		prefix = utils.Reverse(prefix)
	}

	req := datatype.ListObjRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKey:  maxKey,
		Marker:  marker,
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

	svc := s3.New(api.CreateSession()) // create another service point for  getting metadata
	L := 1
	for {

		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				total += int64(N)
				T = 0

				for _, v := range result.Contents {

					head := datatype.StatObjRequest{
						// Service: req.Service,
						Service: svc,
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
			gLog.Error.Printf("ListObjects err %v", err)
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

	utils.Return(start)
}
