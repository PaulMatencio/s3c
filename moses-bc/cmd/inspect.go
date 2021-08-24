// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"time"
)

// restoreMosesCmd represents the restoreMoses command

var (
	inspectCmd = &cobra.Command{
		Use:   "_inspect_",
		Short: "Command to inspect backup objects",
		Long:  ``,
		Hidden: true,
		Run:   Inspect,
	}
)

func initInsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "name of the bucket")
	cmd.Flags().StringVarP(&pn, "key", "k", "", "publication number to be restored")
	cmd.Flags().StringVarP(&versionId, "versionId", "", "", "Version id of the publication number to be restored - default the last version will be restored ")
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 40, "Maximum partsize (MB) for multipart download")
	cmd.Flags().BoolVarP(&verbose,"verbose","v",false,"Verbose display")
}

func init() {
	rootCmd.AddCommand(inspectCmd)
	initInsFlags(inspectCmd)
}

func Inspect(cmd *cobra.Command, args []string) {

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if err, suf := mosesbc.GetBucketSuffix(bucket, pn); err != nil {
		gLog.Error.Printf("%v", err)
		return
	} else {
		if len(suf) > 0 {
			bucket += "-" + suf
			gLog.Warning.Printf("A suffix %s is appended to the bucket %s", suf, bucket)
		}
	}
	service = mosesbc.CreateS3Session("backup", "target")
	req := datatype.GetObjRequest{
		Service: service,
		Bucket:  bucket,
		Key:     pn,
	}
	inspectPn(req)

}

/*

		Get  the backup object
		verify that  user metadata stored in the backup object is valid .Exit if not
        if metadata valid
			Read the backup object
        	Extract the pdf document if it exist and  restore it
        	check if page 0 exist the document
			Extract  pages ( page 0 inclusive if exists)

*/

func inspectPn(request datatype.GetObjRequest)  {

	var (
		result                    *s3.GetObjectOutput
		nerrors int = 0
		usermd                    string
		document                  *documentpb.Document
		start2  = time.Now()

	)
	if result, err = api.GetObject(request); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]", s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("Error: %v", aerr.Error())
				nerrors += 1
			}
		} else {
			gLog.Error.Printf("Error:%v", err.Error())
			nerrors += 1
		}
	} else {
		defer result.Body.Close()
		if usermd, err = utils.GetUserMeta(result.Metadata); err == nil {
			userm := UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
		} else {
			gLog.Error.Printf("Error %v - The user metadata %s is invalid", err, result.Metadata)
			return
		}
		gLog.Info.Printf("Get Object key %s - Elapsed time %v ", request.Key, time.Since(start2))

		/*
			retrieve the backup document
		*/

		if body, err := utils.ReadObjectv(result.Body, CHUNKSIZE); err == nil {
			defer result.Body.Close()
			document, err = mosesbc.GetDocument(body.Bytes())
			fmt.Printf("\tDocument id: %s - Version Id: %s - Number of pages: %d - Document size: %d\n",document.DocId, document.VersionId, document.NumberOfPages, document.Size)
			usermd,_ :=  base64.StdEncoding.DecodeString(document.Metadata)
			fmt.Printf("\tDocument user metadada: %s\n",string(usermd))
			s3meta,_ :=  base64.StdEncoding.DecodeString(document.S3Meta)
			fmt.Printf("\tDocument s3 metadata: %s\n",string(s3meta))
			Pdf:= document.Pdf
			if len(Pdf.Pdf) > 0 {
				fmt.Printf("\tDocument PDFid: %s - PDF size %d\n",Pdf.PdfId,Pdf.Size)
			}
			if document.Clip {
				fmt.Printf("\tDocument %s  has a clipping page (page 0)\n",document.DocId)
			}
			mosesbc.InspectBlobs(document, maxPage,verbose)
		} else {
			gLog.Error.Printf("Error %v when retrieving the document %s\n", err, request.Key)
			nerrors = 1
		}
	}
}
