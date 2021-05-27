
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

// putObjectsCmd represents the putObjects command
var (
	idir string
	async bool
	pobjshort =  "Command to upload multiple objects and their user metadata from a given directory to a bucket"
	putObjectsCmd = &cobra.Command {
		Use:   "putObjects",
		Short: pobjshort,
		Long: ``,
		Hidden: true,
		Run: putObjects,
	}
	putObjsCmd = &cobra.Command {
		Use:   "putObjs",
		Short: pobjshort,
		Long: ``,
		Run: putObjects,
	}
)

func initPfsFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of concurrent files to upload ")
	cmd.Flags().StringVarP(&idir,"idir","I","","input directory containing the files you'd like to upload")
	cmd.Flags().BoolVarP(&async,"async","a",false,"upload files concurrently ")
}

func init() {
	RootCmd.AddCommand(putObjectsCmd)
	RootCmd.AddCommand(putObjsCmd)
	initPfsFlags(putObjectsCmd)
	initPfsFlags(putObjsCmd)
}

func putObjects(cmd *cobra.Command, args []string) {

	var (
		start = utils.LumberPrefix(cmd)
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(idir) == 0 {
		gLog.Warning.Printf("%s",missingInputFile)
		utils.Return(start)
		return
	}


	if !async {
		putfObjs(idir)
	} else {
		putfObjsAsync(idir)
	}

	utils.Return(start)
}

func putfObjs(idir string) {

	svc      := s3.New(api.CreateSession())
	if fis,err := utils.ReadDataDir(idir); err == nil {
		for _,fi := range fis {
			datafile := filepath.Join(idir,fi.Name())
			if result,err := fPutObj(svc,datafile); err == nil {
				gLog.Info.Printf("Successfuly upload file %s to  Bucket %s  - Etag : %s", datafile,bucket,*result.ETag)
			} else {
				gLog.Error.Printf("fail to upload %s - error: %v",datafile,err)
			}
		}
	} else {
		gLog.Error.Printf("%v",err)
	}

}

func putfObjsAsync(idir string) {

	ch:= make(chan *datatype.Rp)

	if fis,err := utils.ReadDataDir(idir); err == nil {

		var (
			N = len(fis)
			max = int(maxKey)
			m,n,t int
			svc  = s3.New(api.CreateSession())
		)

		for k:=0; k < N; k += max {

			if m = k+max; m >= N {
				m = N
			}

			for _, fi := range fis[k:m] {

				t = 0
				n = m - k
				key := fi.Name()
				// gLog.Info.Println(key, k,n,N)
				go func(key string) {

					rp := datatype.Rp{
						Key: key,
						Idir: idir,
					}

					rp.Result, rp.Err = fPutObj(svc,filepath.Join(idir, key))
					ch <- &rp

				}(key)
			}

			done := false

			for ok:=true;ok;ok=!done {
				select {
				case rg := <-ch:
					t++
					procPutResult(rg)
					if t == n {
						gLog.Info.Printf("%d objects are uploaded to bucket %s", n,bucket)
						done = true
					}
					rg = &datatype.Rp{}
					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
				}
			}
		}
	} else {

		gLog.Error.Printf("%v",err)
	}

}