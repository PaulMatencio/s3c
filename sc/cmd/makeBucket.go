
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// makeBucketCmd represents the makeBucket command
var (
	mbshort = "Command to create a bucket"
	makeBucketCmd = &cobra.Command{
		Use:   "mkBucket",
		Short: mbshort,
		Long: ``,
		// Hidden:true,
		Run:makeBucket,
	}

	createBucketCmd = &cobra.Command{
		Use:   "createBucket",
		Short: mbshort,
		Long: ``,
		Hidden: true,
		Run:makeBucket,
	}
	mkBucketCmd = &cobra.Command{
		Use:   "makeBucket",
		Short: mbshort,
		Long: ``,
		Run:makeBucket,
	}
	mbCmd = &cobra.Command{
		Use:   "mb",
		Short: mbshort,
		Long: ``,
		Hidden: true,
		Run:makeBucket,
	}

	cbCmd = &cobra.Command{
		Use:   "cb",
		Short: mbshort,
		Long: ``,
		Hidden: true,
		Run:makeBucket,
	}

)

func initMbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket you'd like to create")
}

func init() {

	RootCmd.AddCommand(makeBucketCmd)
	RootCmd.AddCommand(mbCmd)
	initMbFlags(makeBucketCmd)
	initMbFlags(createBucketCmd)
	initMbFlags(mbCmd)
	initMbFlags(cbCmd)

}

func makeBucket(cmd *cobra.Command,args []string) (){

	start:= utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	req:= datatype.MakeBucketRequest{
		Service: s3.New(api.CreateSession()),
		Bucket: bucket,
	}

	if _,err := api.MakeBucket(req); err != nil {

		gLog.Error.Printf("Create Bucket fails [%v]",err)
	}
	utils.Return(start)

}