package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)


var (
	bucket string
	getBucketCmd = &cobra.Command{
	Use:   "getBucket",
	Short: "Command to retrieve  Bucket ACL",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		getBucket(cmd,args)
	},
})

func init() {
	RootCmd.AddCommand(getBucketCmd)
	getBucketCmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the buket you would like to retrieve the ACL")

}


func getBucket(cmd *cobra.Command,args []string) {


	// handle any missing args
	utils.LumberPrefix(cmd)



	if  len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}


	req := datatype.GetBucketAclRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
	}
	result, err := api.GetBucketAcl(req)

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]", s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("error [%v]", aerr.Error())
				}
		} else {
			gLog.Error.Printf("[%v]", err.Error())
		}
	} else {
			// fmt.Println("Result:",result)
			utils.PrintBucketAcl(result)

	}
}
