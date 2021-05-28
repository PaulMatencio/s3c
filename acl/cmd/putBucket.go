package cmd

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)


var (
	email,usertype,permission string

	putBucketCmd = &cobra.Command{
		Use:   "putBucket",
		Short: "Command to create/update Bucket ACL",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			putBucket(cmd,args)
		},
	})

func initSbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&email,"email","E","","The email of the user")
	cmd.Flags().StringVarP(&usertype,"usertype","t","CanonicalUser","the type of the user")
	cmd.Flags().StringVarP(&permission,"permission","p","READ","Permission valid value are: FULL_CONTROL,READ, WRITE, READ_ACP,WRITE_ACP")
}

func init() {
	RootCmd.AddCommand(putBucketCmd)
	initSbFlags(putBucketCmd)
}


func putBucket(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)
	if  len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}

	if  len(email) == 0 {
		gLog.Warning.Printf(missingEmail)
		return
	}

	if permission != "READ" &&  permission != "WRITE" &&  permission != "READ_ACP" && permission != "WRITE_ACP" && permission != "FULL_CONTROL" {
		err := errors.New(illegalPermission )
		gLog.Info.Printf("%v",err)
		return
	}

	if  usertype != "CanonicalUser" {
		err := errors.New(invalidUserType )
		gLog.Info.Printf("%v",err)
		return
	}

	id := "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
	req := datatype.PutBucketAclRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
		ACL: datatype.Acl{
			Grantee: s3.Grantee {
				DisplayName: &email,
				ID: &id,
				Type: &usertype,
			},
			Email: email,
			ID : id,
			Type: usertype,
			Permission:permission,
		},

	}
	result, err := api.AddBucketAcl(req)

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
		fmt.Println("Result:",result)
	}
}
