package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"

	// "github.com/golang/gLog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)


var (
	gbpshort = "Command to get policies of a bucket"
	getBPCmd = &cobra.Command{
		Use:   "getBucketPol",
		Short: gbpshort,
		Long: ``,
		// Hidden: true,
		Run:  func (cmd *cobra.Command,args []string ) {
			GetBucketPolicy(cmd,args)
		},
	}


)

func initGBPFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
}

func init() {
	RootCmd.AddCommand(getBPCmd)
	RootCmd.MarkFlagRequired("bucket")
	initGBPFlags(getBPCmd)
}

func GetBucketPolicy(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	req := datatype.GetBucketPolicyRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
	}

		var (

			result  *s3.GetBucketPolicyOutput
			err error
		)
		if result, err = api.GetBucketPolicy(req); err == nil {
			gLog.Info.Printf("%v",*result.Policy)
		} else {
			gLog.Error.Printf("%v",err)
		}

	utils.Return(start)
}
