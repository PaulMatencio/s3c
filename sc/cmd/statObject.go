package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/http"
)

// getObjectCmd represents the getObject command
var (
	soshort = "Command to  verify if a given object exist and display the object metadata"

	statObjectCmd = &cobra.Command {
		Use:   "statObj",
		Short: soshort,
		Long: ``,
		// Hidden: true,
		Run: statObject,
	}

	headObjCmd = &cobra.Command {
		Use:   "headObj",
		Short: soshort,
		Long: ``,
		// Hidden: true,
		Run: statObject,
	}

	statObjCmd = &cobra.Command {
		Use:   "statObjV2",
		Short: soshort,
		Hidden: true,
		Long: ``,
		Run: statObjectV2,
	}

)

func initHoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object you'd like to check")
	cmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory")
	cmd.Flags().BoolVarP(&full,"fullKey","F",false,"given prefix is a full document key")
}

func init() {

	RootCmd.AddCommand(statObjectCmd)
	RootCmd.AddCommand(statObjCmd)
	RootCmd.AddCommand(headObjCmd)
	RootCmd.MarkFlagRequired("bucket")
	RootCmd.MarkFlagRequired("key")
	initHoFlags(statObjectCmd)
	initHoFlags(statObjCmd)
	initHoFlags(headObjCmd)


}


//  statObject utilizes the api to get object

func statObject(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return

	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return
	}

		req := datatype.StatObjRequest{
			Service:  s3.New(api.CreateSession()),
			Bucket: bucket,
			Key: key,
		}
		result, err := api.StatObject(req)


	/* handle error */

	if err != nil {
		procS3Error(err)
	} else {
		if result.VersionId != nil  {
			gLog.Info.Printf("Version id %s", *result.VersionId)
		}
		procS3Meta(key, result.Metadata)
	}

}

/* use  V2 Authentication*/
func statObjectV2(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return

	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return
	}

	var (
		req = datatype.StatObjRequestV2{
			Client:     new(http.Client),
			AccessKey:    viper.GetString("credential.access_key_id"),
			SecretKey:    viper.GetString("credential.secret_access_key"),
			Bucket: bucket,
			Key: key,
		}
		resp, _ = api.StatObjectV2(req)
	)
	gLog.Info.Printf(resp.Status,resp.Header)


}


