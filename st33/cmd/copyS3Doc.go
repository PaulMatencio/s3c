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
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)


var (

	frombucket, tobucket  string

	copyS3DocCmd = &cobra.Command{
		Use:   "copyS3Doc",
		Short: "Command to copy PXI S3 document(s)",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			copyS3Doc(cmd,args)
		},
	})

func init() {

	RootCmd.AddCommand(copyS3DocCmd)
	copyS3DocCmd.Flags().StringVarP(&key,"key","k","","the PXI Id of a document you'd like to copy")
	copyS3DocCmd.Flags().StringVarP(&frombucket,"from-bucket","f","","the name of the source bucket")
	copyS3DocCmd.Flags().StringVarP(&tobucket,"to-bucket","t","","the name of the target bucket")
	copyS3DocCmd.Flags().StringVarP(&ifile,"ifile","i","","full pathname of an input file containing a list of pixid to be copied")

}


func copyS3Doc(cmd *cobra.Command,args []string)  {

	var (
		start = utils.LumberPrefix(cmd)
		Keys   []string
	)

	if len(key) == 0  && len(ifile)== 0 {
		gLog.Warning.Printf("%s","missing PXI id and input file containing list of pxi ids")
		utils.Return(start)
		return
	}

	if len(key) > 0 {
		Keys=append(Keys, key)
	}

	if len(ifile) >  0 {

		if b,err := ioutil.ReadFile(ifile); err == nil  {
			Keys = strings.Split(string(b)," ")
		} else {
			gLog.Error.Printf("%v",err)
			utils.Return(start)
			return
		}
	}

	if len(frombucket) == 0 {
		gLog.Warning.Printf("%s",missingSourceBucket)
		utils.Return(start)
		return
	}

	if len(tobucket) == 0 {
		gLog.Warning.Printf("%s",missingTargetBucket)
		utils.Return(start)
		return
	}

	if len(odir) >0 {

		pdir = filepath.Join(utils.GetHomeDir(),odir)
		if _,err:=os.Stat(pdir); os.IsNotExist(err) {
			utils.MakeDir(pdir)
		}
	}

	svc := s3.New(api.CreateSession())

	for _,key := range Keys{
		lp := len(key);
		if key[lp-2:lp-1] == "P"{
		copyDocP(key, svc)
	} else if key[lp-2:lp-1] == "B"{
		copyDocB(key, svc, frombucket, tobucket)
	}
	}

	utils.Return(start)
}


func copyDocB(key string,svc*s3.S3,frombucket string ,tobucket string) {

	KEY := utils.Reverse(key)
	if err := st33.CopyObject(KEY,svc,frombucket,tobucket); err == nil {
		gLog.Info.Printf("Pxi id %s is copied from bucket %s  to bucket %s",key,frombucket,tobucket)
	}
}

//  copy St33 images
func copyDocP(key string,svc *s3.S3 ) {

	KEY := utils.Reverse(key)
	KEYx := KEY+".1"

	req := datatype.GetObjRequest{
		Service : svc,
		Bucket: frombucket,
		Key : KEYx,  // Get the first Object
	}

	if resp,err  := api.GetObject(req); err == nil {

		var (
			pages int
			err   error
			val   string
		)

		// the first page should contain PXI metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Error.Printf("Key : %s - Document metadata %s is missing ",KEYx)
			return
		}

		if pages,err = strconv.Atoi(val); err != nil || pages == 0 {
			gLog.Error.Printf("Key : %s - Document metadata %s is invalid", KEYx)
			return
		}

		//Copy the first object
		st33.CopyObject(KEY,svc,frombucket,tobucket)
		// copy other objects
		if pages > 1 {
			st33.CopyObjects(KEY, pages-1, svc,frombucket,tobucket)
		}

	} else {
		gLog.Error.Printf("%v",err)
	}
}

