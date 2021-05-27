// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	putObjCmd = &cobra.Command{
		Use:   "putObj",
		Short: "Command to create/update Object ACL",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			putObject(cmd,args)
		},
	})

func initPoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&bucket,"key","k","","the object key yo would like to add an ACL")
	cmd.Flags().StringVarP(&email,"email","E","","the email of the user")
	cmd.Flags().StringVarP(&usertype,"usertype","t","CanonicalUser","the type of the user")
	cmd.Flags().StringVarP(&permission,"permission","p","READ","valid permission value are: FULL_CONTROL,READ, WRITE, READ_ACP,WRITE_ACP")
}

func init() {
	RootCmd.AddCommand(putObjCmd)
	initPoFlags(putObjCmd)
}

func putObject(cmd *cobra.Command,args []string) {

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
	req := datatype.PutObjectAclRequest{
		Service: s3.New(api.CreateSession()),
		Bucket:  bucket,
		Key: key,
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
	result, err := api.AddObjectAcl(req)

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