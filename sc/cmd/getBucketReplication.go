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
	gbrshort = "Command to get bucket replication"
	getBRCmd = &cobra.Command{
		Use:   "getBucketRep",
		Short: gbrshort,
		Long: ``,
		// Hidden: true,
		Run:  func (cmd *cobra.Command,args []string ) {
			GetBucketRep(cmd,args)
		},
	}
)

func initGBRFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
}

func init() {
	RootCmd.AddCommand(getBRCmd)
	RootCmd.MarkFlagRequired("bucket")
	initGBRFlags(getBRCmd)
}

func GetBucketRep(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	req := datatype.GetBucketReplicationRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
	}

	var (
		result  *s3.GetBucketReplicationOutput
		err error
	)
	if result, err = api.GetBucketReplication(req); err == nil {
		gLog.Info.Printf("%v",*result.ReplicationConfiguration)
	} else {
		gLog.Error.Printf("%v",err)
	}

	utils.Return(start)
}
