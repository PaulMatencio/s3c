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
	key string
	getObjCmd = &cobra.Command{
		Use:   "getObj",
		Short: "Command to retrieve Object ACL",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			getObject(cmd,args)
		},
	})

func init() {
	RootCmd.AddCommand(getObjCmd)
	getObjCmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the buket you would like to retrieve the ACL")
	getObjCmd.Flags().StringVarP(&key,"key","k","","the object key you would like to retrieve the ACL")
}


func getObject(cmd *cobra.Command,args []string) {


	// handle any missing args
	utils.LumberPrefix(cmd)



	if  len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if  len(key) == 0 {
		gLog.Warning.Printf("%s", missingKey)
		return
	}

	req := datatype.GetObjAclRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
		Key :  key,
	}
	result, err := api.GetObjectAcl(req)

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
		utils.PrintObjectAcl(result)

	}
}
