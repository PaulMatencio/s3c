
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
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// getS3DocCmd represents the dirInq command

var (
	key string
	reverse,con bool
	getS3DocCmd = &cobra.Command{
		Use:   "getS3Doc",
		Short: "Command to get PXI S3 document(s)",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			getS3Doc(cmd,args)
		},
	}
	statS3DocCmd = &cobra.Command{
		Use:   "statS3Doc",
		Short: "Command to check PXI S3 document(s)",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			statS3Doc(cmd,args)
		},
	}


	)

func initS3DFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&key,"key","k","","the PXI ID of the document you would like to retrieve")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the  bucket")
	cmd.Flags().StringVarP(&odir,"odir","O","","the ouput directory (relative to the home directory)")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","full pathname of an input  file containing a list of pix ids to be downloaded")
	cmd.Flags().BoolVarP(&reverse,"reverse","r",false,"reverse the input key - Default false ")
	cmd.Flags().BoolVarP(&con,"async","a",false,"concurrent read")

}

func init() {
	RootCmd.AddCommand(getS3DocCmd)
	RootCmd.AddCommand(statS3DocCmd)
	initS3DFlags(getS3DocCmd)
	initS3DFlags(statS3DocCmd)

}


func getS3Doc(cmd *cobra.Command,args []string)  {

	var (
		start = utils.LumberPrefix(cmd)
		Keys   []string
		p  = 0
		b = 0
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(key) == 0  && len(ifile)== 0 {
		gLog.Warning.Printf("%s","missing PXI id and input file containing list of pxi ids")
		utils.Return(start)
		return
	}

	if len(key) > 0 {
		Keys=append(Keys, key)
	}

	if len(ifile) >  0 {
		/*
		if b,err := ioutil.ReadFile(ifile); err == nil  {
			Keys = strings.Split(string(b)," ")
		} else {
			gLog.Error.Printf("%v",err)
			utils.Return(start)
			return
		}

		 */
		 if sc,err := utils.Scanner(ifile); err == nil {
		 	if Keys,err = utils.ScanAllLines(sc); err == nil {
		 		gLog.Error.Printf("Error %v reading file %s",err,ifile)
			}
		 }
	}
	gLog.Info.Printf("Nunber of documents to retieve: %d ",len(Keys))

	if len(odir) > 0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		if _,err:=os.Stat(pdir); os.IsNotExist(err) {
			utils.MakeDir(pdir)
		}
		for  _,key := range Keys {
			//key = strings.TrimSpace(key)
			if checkDoc(key) == "P" {
				p += saveDocP(key,pdir)
			} else  if checkDoc(key) == "B"  {
				getDocB(key, pdir)
				b++
			}
		}
	} else {
		for  _,key := range Keys {
			key = strings.TrimSpace(key)
			if checkDoc(key) == "P" {
				p += getDocP(key)
			} else  if checkDoc(key) == "B"  {
				getDocB(key, pdir)
				b++
			}
		}
	}
	gLog.Info.Printf("Total number of pages retrieved: %d - Total number of blob retrieved: %d",p,b)
	utils.Return(start)
}


func statS3Doc(cmd *cobra.Command,args []string)  {

	var (
		start = utils.LumberPrefix(cmd)
		Keys   []string
		p  = 0
		b = 0
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(key) == 0  && len(ifile)== 0 {
		gLog.Warning.Printf("%s","missing PXI id and input file containing list of pxi ids")
		utils.Return(start)
		return
	}

	if len(key) > 0 {
		Keys=append(Keys, key)
	}

	if len(ifile) >  0 {
		/*
			if b,err := ioutil.ReadFile(ifile); err == nil  {
				Keys = strings.Split(string(b)," ")
			} else {
				gLog.Error.Printf("%v",err)
				utils.Return(start)
				return
			}

		*/
		if sc,err := utils.Scanner(ifile); err == nil {
			if Keys,err = utils.ScanAllLines(sc); err == nil {
				gLog.Error.Printf("Error %v reading file %s",err,ifile)
			}
		}
	}
	gLog.Info.Printf("Nunber of documents to retieve: %d ",len(Keys))

	if len(odir) > 0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		if _,err:=os.Stat(pdir); os.IsNotExist(err) {
			utils.MakeDir(pdir)
		}
		for  _,key := range Keys {
			//key = strings.TrimSpace(key)
			if checkDoc(key) == "P" {
				p += saveDocP(key,pdir)
			} else  if checkDoc(key) == "B"  {
				getDocB(key, pdir)
				b++
			}
		}
	} else {
		for  _,key := range Keys {
			key = strings.TrimSpace(key)
			if checkDoc(key) == "P" {
				p += statDocP(key)
			} else  if checkDoc(key) == "B"  {
				statDocP(key)
				b++
			}
		}
	}
	gLog.Info.Printf("Total number of pages retrieved: %d - Total number of blob retrieved: %d",p,b)
	utils.Return(start)
}

func checkDoc(key string) (string) {
	typex := "P"
	if !reverse {
		if key[1:2] == "B" {
			typex="B"
		}
	} else {
		lp := len(key);
		if key[lp-2:lp-1] == "B" {
			typex="B"
		}
	}
	return typex
}

func getDocP(key string ) (int) {
	var (
		n = 0
		buck = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(key,bucketNumber))
	)
	if reverse {
		key= utils.Reverse(key)
	}
	KEYx := key+".1"
	svc := s3.New(api.CreateSession())
	req := datatype.GetObjRequest{
		Service : svc,
		Bucket: buck,
		Key : KEYx,  // Get the first Object
	}
	start := time.Now()
	if resp,err  := api.GetObject(req); err == nil {
		var (
			pages int
			err   error
			val   string
		)
		// the first page should contain PXI metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Error.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			return n
		}

		if pages,err = strconv.Atoi(val); err != nil || pages == 0 {
			gLog.Error.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			return n
		}
		// read teh first page
		readObject(KEYx,resp,start)
		n++
		// read the other pages
		if pages > 1 {
			if con {
			n += readObjects(key, buck, pages-1, svc)
			} else {
				n += readObjs(key, buck, pages-1, svc)
			}
		}

		gLog.Info.Printf("%d pages are retrieved for pxid %s",n,key)

	} else {
		gLog.Error.Printf("%v",err)
	}
	return n
}


func statDocP(key string ) (int) {
	var (
		n = 0
		key1 = strings.Replace(key," ","_",-1)
		buck = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(key1,bucketNumber))
	)
	if reverse {
		key1= utils.Reverse(key1)
	}
	KEYx := key1+".1"
	svc := s3.New(api.CreateSession())
	req := datatype.StatObjRequest{
		Service : svc,
		Bucket: buck,
		Key : KEYx,  // Get the first Object
	}

	if resp,err  := api.StatObject(req); err == nil {
		gLog.Trace.Println(resp.ETag)
	} else {
		gLog.Error.Printf("Key: %s - %v",key, err)
	}
	return n
}
//
//  Save  S3 ST33 Object into
//
//

func saveDocP(key string, pdir string )  (int) {
	var (
		n = 0
		buck = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(key,bucketNumber))
	)
	if reverse {
		key = utils.Reverse(key)
	}
	KEYx := key+".1"
	svc := s3.New(api.CreateSession())
	req := datatype.GetObjRequest{
		Service : svc,
		Bucket: buck,
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
			gLog.Error.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			return n
		}

		if pages,err = strconv.Atoi(val); err != nil || pages == 0 {
			gLog.Error.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			return n
		}

		// Save first object and its metadata
		saveMeta(KEYx,resp.Metadata)
		if err := saveObject(KEYx,resp,pdir); err != nil {
			gLog.Error.Printf("Saving page %s %v ",KEYx,err)
		}
		n++
		// save other objects
		if pages > 1 {
			n += saveObjects(key, buck,pages-1, svc)
		}
		gLog.Trace.Printf("%d pages are retrieved for pxid %s",n,key)
	} else {
		gLog.Error.Printf("%v",err)
	}
	return n
}

//
//   Retrieve S3 BLOB Object
//   key :  Key of the S3 Object
//   pdir : output directory
//   if pdir is given, output will be saved into pdir
//

func getDocB(key string,pdir string) {

	var (
		buck = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(key,bucketNumber))
	)
	if reverse {
		key = utils.Reverse(key)
	}
	// Get the number of Pages

	KEYx := key+".1"
	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: buck,
		Key : KEYx,
	}

	start := time.Now()
	if resp,err  := api.GetObject(req); err == nil {
		var (
			pages int
			err error
			val string
		)
		// document should contain user  metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Warning.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			// Check if usermeta data  is valid
			if pages,err = strconv.Atoi(val); err != nil || pages != 1 {
				gLog.Warning.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			}
		}

		if len(pdir) == 0 {
			if err := readObject(KEYx, resp,start ); err == nil {
				gLog.Info.Printf("Blob %s is retrieved",key)
			}
		} else {
			saveMeta(KEYx,resp.Metadata)
			if err := saveObject(KEYx, resp, pdir); err == nil { // save Object content
				gLog.Info.Printf("Blob %s is saved to %s", key,pdir)
			}
		}

		if pages > 1 {
			err = errors.New("Number of Pages:"+ *resp.Metadata["Pages"])
			gLog.Warning.Printf("Oop! Wrong number of pages for %s %v",KEYx, err)
		}

	} else {
		gLog.Error.Printf("%v",err)
	}
}



//  save S3 object in streaming mode
//  key : S3 key of the object
//  resp :  S3 GetObjectOutput response
//  pdir : output directory

func saveObject(key string, resp *s3.GetObjectOutput,pdir string ) (error) {
	pathname := filepath.Join(pdir,strings.Replace(key,string(os.PathSeparator),"_",-1))
	return utils.SaveObject(resp,pathname)
}


//  key : S3 key of the object
//  resp :  S3 GetObjectOutput response

func readObject(key string, resp*s3.GetObjectOutput,start time.Time) (error){

	b, err := utils.ReadObject(resp.Body)
	if err == nil {
		// gLog.Info.Printf("%d objects are retrieved for pxid %s",1,key)
		gLog.Trace.Printf("GET Key: %s  - ETag: %s  - Content length: %d - Object length: %d - Elapsed time %v ",key,*resp.ETag,*resp.ContentLength,b.Len(),time.Since(start))
	} else {
		gLog.Error.Printf("%v",err)
	}
	return err
}


//
// Retrieve Objects and display their length
//
// Input
//		Key : PXI id
//		Pages: Number of pages
//      svc : S3 service
//

func readObjects(key string, bucket string,pages int,svc *s3.S3 ) (int) {

	var (
		N = pages
		T = 0
		ch  = make(chan *datatype.Rb)
	)
	// bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(key,bucketNumber))

	for p:=1 ; p <= pages; p++ {

		KEYx := key + "." + strconv.Itoa(p)
		go func(KEYx string) {

			req := datatype.GetObjRequest{
				Service: svc,
				Bucket:  bucket,
				Key:     KEYx,
			}

			if resp, err := api.GetObject(req); err == nil {
				b, err := utils.ReadObject(resp.Body)
				// gLog.Trace.Printf("Go Response Key: %s %v", KEYx,err)
				if err == nil {
					ch <- &datatype.Rb{
						Key:      KEYx,
						Object:   b,
						Result:   resp,
						Err:      err,
					}
				}
			} else {
				//gLog.Trace.Printf("Response Key: %s  %v", KEYx, err)
				ch <- &datatype.Rb{
					Key:    KEYx,
					Object: nil,
					Result: nil,
					Err:    err,
				}
			}

			req = datatype.GetObjRequest{}
		}(KEYx)
	}

	for  {
		select {
		case rb := <-ch:
			T++
			if rb.Err == nil {
				gLog.Trace.Printf("GET Key: %s  - ETag: %s  - Content length: %d - Object length: %d", rb.Key, *rb.Result.ETag, *rb.Result.ContentLength, len(rb.Object.Bytes()))
			} else {
				gLog.Error.Printf("Error getting object key %s: %v",rb.Key,rb.Err)
			}
			if T == N {
				gLog.Trace.Printf("%d objects are retrieved for pxid %s",N,key)
				return N
			}
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("w")
		}
	}
}


func readObjs(key string, bucket string,pages int,svc *s3.S3 ) (int) {
	n:= 0
	for p := 1; p <= pages; p++ {
		KEYx := key + "." + strconv.Itoa(p)
		req := datatype.GetObjRequest{
			Service: svc,
			Bucket:  bucket,
			Key:     KEYx,
		}
		start := time.Now()
		if resp, err := api.GetObject(req); err == nil {
			n++
			if b, err := utils.ReadObject(resp.Body); err == nil {
				gLog.Trace.Printf("GET Key: %s  - ETag: %s  - Content length: %d - Object length: %d - Elapsed time %v ",KEYx,*resp.ETag,*resp.ContentLength,b.Len(),time.Since(start))
			}
		}

	}
	return n
}



//
// Download  Objects from S3  to Folder
//
// input
// 		key  PXI key
// 		number of pages to download
// 		S3 service
//

func saveObjects(key string, bucket string, pages int,svc *s3.S3 ) (int) {

	var (
		N  = pages
		T  = 0
		ch = make(chan *datatype.Ro)
		resp *s3.GetObjectOutput
		err  error
	)

	for p:=1 ; p <= pages; p++ {

		KEYx := key + "." + strconv.Itoa(p)
		go func(KEYx string) {

			req := datatype.GetObjRequest{
				Service: svc,
				Bucket:  bucket,
				Key:     KEYx,
			}

			if resp, err = api.GetObject(req); err == nil {
				saveObject(KEYx,resp,pdir)
			}

			ch <- &datatype.Ro {
				Key : KEYx,
				Result: resp,
				Err : err,
			}

			req = datatype.GetObjRequest{}
		}(KEYx)
	}

	for  {
		select {
		case ro := <-ch:
			T++
			if ro.Err == nil {
				gLog.Trace.Printf("Key: %s - ETag: %s - Content length: %d - downloaded to: %s", ro.Key, *ro.Result.ETag, *ro.Result.ContentLength,pdir)
			} else {
				gLog.Error.Printf("Error getting object key %s: %v",ro.Key,ro.Err)
			}
			if T == N {
				gLog.Trace.Printf("%d objects are saved for pxiId %s to %s",N,key, pdir)
				return N
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf("w")
		}
	}
}

func saveMeta(key string, metad map[string]*string) {

	pathname := filepath.Join(pdir,strings.Replace(key,string(os.PathSeparator),"_",-1)+".md")
	utils.WriteUsermd(metad,pathname)

}