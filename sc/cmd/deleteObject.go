// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
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
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
)

// deleteObjectCmd represents the deleteObject command
var  (

	deleteObjectCmd = &cobra.Command{
		Use:   "delObj",
		Short: "Command to delete an object",
		Long: ``,
	//	Hidden: true,
		Run: deleteObject,
	}

	delObjectCmd = &cobra.Command{
		Use:   "rmObj",
		Short: "Command to delete an object",
		Long: ``,
		Run: deleteObject,
	}

)

func initDoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the object to be deleted")

}

func init() {

	RootCmd.AddCommand(deleteObjectCmd)
	RootCmd.AddCommand(delObjectCmd)
	initDoFlags(deleteObjectCmd)
	initDoFlags(delObjectCmd)

}


func deleteObject(cmd *cobra.Command, args []string) {

	start := utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return

	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return
	}

	req:= datatype.DeleteObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key: key,
	}

	if _,err := api.DeleteObjects(req); err == nil {
		gLog.Info.Printf("Object %s is removed from %s",key,bucket)
	} else {
		gLog.Error.Printf("%v",err)
	}
	utils.Return(start)
}

