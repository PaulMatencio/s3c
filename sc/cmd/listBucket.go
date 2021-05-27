

package cmd

import (
	// "bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	// "github.com/golang/glog"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"

)

// listBucketCmd represents the listBucket command
var (
	lbshort = "Command to list all your buckets"
	listBucketCmd = &cobra.Command {
		Use:   "listBucket",
		Short: lbshort,
		Hidden: true,
		Long: ``,
		// Hidden: true,
		Run: listBucket,
	}
	lbCmd = &cobra.Command {
		Use:   "lsBucket",
		Short: lbshort,
		Long: ``,
		Run: listBucket,
	}
)



func init() {

	RootCmd.AddCommand(listBucketCmd)
	RootCmd.AddCommand(lbCmd)
}

func listBucket(cmd *cobra.Command,args []string) {
	start:= utils.LumberPrefix(cmd)
	req := datatype.ListBucketRequest{
		Service:  s3.New(api.CreateSession()),
	}
	if result,err := api.ListBucket(req); err != nil {
		gLog.Error.Printf("%v",err)
	} else {
		gLog.Info.Printf("Owner of the buckets: %s", result.Owner)
		for _, v := range result.Buckets {
			gLog.Info.Printf("Bucket Name: %s - Creation date: %s", *v.Name, v.CreationDate)
		}
	}
	utils.Return(start)

}