
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// copyObjectCmd represents the copyObject command
var (
	sbucket,skey string
	copyObjectCmd = &cobra.Command{
	Use:   "copyObj",
	Short: "Command to copy an object from one bucket to another",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		copyObject(cmd,args)
	},
}
)


func initCoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"to-bucket","t","","the name of target the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the key of the target object")
	cmd.Flags().StringVarP(&sbucket,"from-bucket","f","","the name  the source bucket")
	cmd.Flags().StringVarP(&skey,"skey","","","the  key of the source object you'd like to copy")

}

func init() {

	RootCmd.AddCommand(copyObjectCmd)
	initCoFlags(copyObjectCmd)

}

func copyObject(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s","missing target bcket")
		return

	case len(sbucket) == 0:
		gLog.Warning.Printf("%s","missing source bucket")
		return

	case len(skey) == 0:
		gLog.Warning.Printf("%s","missing source key")
		return
	}

	if len(key) == 0 {
		key= skey
	}
	var (

		req = datatype.CopyObjRequest{
			Service:  s3.New(api.CreateSession()),
			Sbucket: sbucket,
			Skey: skey,
			Tbucket: bucket,
			Tkey: key,
		}
		result, err = api.CopyObject(req)
	)

	/* handle error */

	if err != nil {
		procS3Error(err)

	} else {
		gLog.Info.Printf("new object ETag %s",*result.CopyObjectResult.ETag)
	}

}



