

package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"time"
)

// statBucketCmd represents the statBucket command
var statBucketCmd = &cobra.Command{
	Use:   "statBucket",
	Short: "Command to verify if a  given bucket exist",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		statBucket(cmd,args)
	},
}

func initHbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")

}

func init() {

	RootCmd.AddCommand(statBucketCmd)
	RootCmd.MarkFlagRequired("bucket")
	initHoFlags(statBucketCmd)

}
//  statObject utilizes the api to get object

func statBucket(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return
	}
	start := time.Now()
	var (

		req = datatype.StatBucketRequest{
			Service:  s3.New(api.CreateSession()),
			Bucket: bucket,
		}
		_, err = api.StatBucket(req)
	)

	/* handle error */

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			gLog.Info.Printf("Bucket %s %v ",bucket,aerr.Code())
		} else {
			gLog.Error.Println(err.Error())
		}
	} else {
		gLog.Info.Printf("Bucket %s exists (Response in %s ms) ",bucket, time.Since(start))
	}

}


