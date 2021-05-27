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
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"time"
)


var (
	cposhort = "Command to retieve some of the metadata of specific or every object in the bucket"
	cpoCmd = &cobra.Command{
		Use:   "copyObjs",
		Short: cposhort,
		Long: ``,
		Run: copyObjects,
	}


)
/*
type  Rd struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}
*/

func init() {

	RootCmd.AddCommand(cpoCmd)
	cpoCmd.Flags().StringVarP(&sbucket,"from-bucket","f","","the name  the source bucket")
	cpoCmd.Flags().StringVarP(&bucket,"to-bucket","t","","the name  the target bucket")
	cpoCmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cpoCmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of keys to be processed concurrently")
	cpoCmd.Flags().StringVarP(&marker,"marker","M","","start processing from this key")
	cpoCmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cpoCmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
}


func copyObjects(cmd *cobra.Command,args []string) {

	var (
		start= utils.LumberPrefix(cmd)
		N,T = 0,0
		total int64 = 0
	)

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s",missingToBucket)
		utils.Return(start)
		return
	}

	if len(sbucket) == 0 {

		gLog.Warning.Printf("%s",missingFromBucket)
		utils.Return(start)
		return
	}


	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: sbucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	ch:= make(chan *datatype.Rc)
	// ch:= make(chan int)
	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		// rd Rd
		l  int
	)

	svc  := s3.New(api.CreateSession()) // create another service point for  getting metadata
    L:= 1
	for {
		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				total += int64(N)
				T = 0

				for _, v := range result.Contents {

					key := *v.Key
					cpo := datatype.CopyObjRequest{

						Service: svc,
						Sbucket:  req.Bucket, // source bucket
						Tbucket: bucket,
						Skey: key,
						Tkey: key,
					}
					go func(request datatype.CopyObjRequest) {

						rc  := datatype.Rc {
							Key : request.Skey,
						}
						rc.Result, rc.Err = api.CopyObject(cpo)
						ch <- &rc
						cpo = datatype.CopyObjRequest{}

					}(cpo)

				}

				done:= false

				for ok:=true;ok;ok=!done {

					select {
					case  rc:= <-ch:
						T++
						if rc.Err != nil {
							gLog.Error.Printf("%v ",rc.Err)
						} else {
							gLog.Trace.Printf("Key %s is copied from %s to %s",rc.Key,sbucket,bucket)
						}
						if T == N {
							gLog.Info.Printf("%d objects are copied from %s to %s",N,sbucket,bucket)
							done = true
						}
						rc = &datatype.Rc{}
					case <-time.After(200 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}

		} else {
			gLog.Error.Printf("ListObjects err %v",err)
			break
		}
		L++
		if *result.IsTruncated {
			nextmarker = *result.Contents[l-1].Key
			gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)
		}

		if  *result.IsTruncated  && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextmarker

		} else {
			gLog.Info.Printf("Total number of objects copied: %d",total)
			break
		}
	}

	utils.Return(start)
}

