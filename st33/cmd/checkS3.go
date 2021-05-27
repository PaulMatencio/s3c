
package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
)

// readConvalCmd represents the readConval command
var checkS3Cmd = &cobra.Command{

	Use:   "chkS3",
	Short: "Command to check if all content of a given st33 file has been migrated to a S3 bucket",
	Long: `Command to check if all the  Tiff images and Blobs of a given st33 data file have been migrated to a S3 bucket`,
	Run: func(cmd *cobra.Command, args []string) {
		checkS3(cmd,args)
	},
}

func initCvFlags(cmd *cobra.Command) {


	cmd.Flags().StringVarP(&ifile,"ifile","i","","the corresponding control file that was used to migrate the data file")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket you would like to check against a given data file")
	// cmd.Flags().StringVarP(&odir,"ofile","o","","output file")
}

func init() {

	RootCmd.AddCommand(checkS3Cmd)
	initCvFlags(checkS3Cmd)
}

func checkS3(cmd *cobra.Command, args []string) {

	var (
		pages,w  int=0,0
	)
	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {

		svc := s3.New(api.CreateSession())

		for k, v := range *c {

			req := datatype.StatObjRequest{
				Service:  svc,
				Bucket: bucket+"-"+fmt.Sprintf("%02d",utils.HashKey(v.PxiId,bucketNumber)),
				Key:utils.Reverse(v.PxiId)+".1",
			}

			if result, err  := api.StatObject(req); err == nil {

				p,_ := strconv.Atoi(*result.Metadata["Pages"]) // Number of pages of the document
				vp := int(v.Pages) // number of pages taken from the control file
				if vp != 0 && p != vp  {  // the number of pages of a blob document = 0
					gLog.Warning.Printf("Conval index: %d - Conval Key: %s - S3 Key: %s - Bucket: %s - Conval pages: %d != S3 pages: %d",k, v.PxiId, req.Key,req.Bucket,v.Pages,p)
					w++
				} else {
					gLog.Trace.Printf("Conval index: %d - Conval key: %s - S3 Key: %s S3 Bucket: %s # Pages: %d",k,v.PxiId,req.Key,req.Bucket,p)
				}
				pages += p

			} else {
				gLog.Error.Printf("Error %v while getting the metadata of key %s",err,req.Key)
				w++
			}
		}

		if w ==  0 {
			gLog.Info.Printf("%d documents, %d  pages have been migrated without error",len(*c),pages)
		}
	} else {
		gLog.Error.Println(err)
	}
}

func ChkS3(page int,v st33.Conval) {

}
