
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// deleteBucketCmd represents the deleteBucket command
var (
	dbshort =  "Command to delete a bucket"

	deleteBucketCmd = &cobra.Command{
		Use:   "delBucket",
		Short: dbshort,
		Long: ``,
		// Hidden: true,
		Run:deleteBucket,
	}

	removeBucketCmd = &cobra.Command{
		Use:   "rmBucket",
		Short: dbshort,
		Long: ``,
		Run:deleteBucket,
	}
	rbCmd = &cobra.Command{
		Use:   "db",
		Short: dbshort,
		Long: ``,
		Hidden: true,
		Run:deleteBucket,
	}

	dbCmd = &cobra.Command{
		Use:   "rb",
		Short: dbshort,
		Hidden: true,
		Long: ``,
		Run:deleteBucket,
	}

)

func initRbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the buket to be deleted")
}

func init() {

	RootCmd.AddCommand(deleteBucketCmd)
	RootCmd.AddCommand(dbCmd)
	RootCmd.AddCommand(removeBucketCmd)
	RootCmd.AddCommand(rbCmd)
	initRbFlags(deleteBucketCmd)
	initRbFlags(removeBucketCmd)
	initRbFlags(rbCmd)
	initRbFlags(dbCmd)

}

func deleteBucket(cmd *cobra.Command,args []string) (){

	start := utils.LumberPrefix(cmd)

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s",missingBucket)
		return
	}

	req := datatype.DeleteBucketRequest {
		Service : s3.New(api.CreateSession()),
		Bucket : bucket,

	}
	if _,err := api.DeleteBucket(req); err != nil {
		gLog.Error.Printf("Delete bucket fails  [%v]",err)
	}
	utils.Return(start)

}

