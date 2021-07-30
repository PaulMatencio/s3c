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
	"bufio"
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"google.golang.org/protobuf/types/known/timestamppb"

	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

// listObjectCmd represents the listObject command
var (
	backupCmd = &cobra.Command{
		Use:   "_backup_",
		Short: "Command to backup MOSES objects and directories",
		Long: `        
        Command to backup Moses data and Moses directories
      
        Moses data are stored in Scality Ring native object storage accessed via sproxyd driver 
        Moses directories are stored in S3 buckets. Each bucket name has a suffix ( <bucket-name>-xx ; xx=00..05)
        
        Usage: 
        moses-bc --help or -h  to list all the commands
        moses-bc  <command> -h or --help to list all the arguments related to a specific <command>
  
        Config file: 
      	The Default config file is located $HOME/.clone/config.file 

        Example of full backup: 

        Backup all the objects listed in the S3 --source-bucket meta-moses-prod-pn-01  to the S3 bucket meta-moses-prod-bkup-pn-01 
     	
        moses-bc -c $HOME/.clone/config.yaml _backup_ --source-bucket meta-moses-prod-pn-01 \ 
        --target-bucket meta-moses-prod-bkup-pn-01 --max-loop 0
        **  suffix is requird and both source and target bucket must have the same suffix, for instance -01 ** 

        Example of backup of all document name started with a specific --prefix 

        moses-bc -c $HOME/.clone/config.yaml _backup_ --source-bucket meta-moses-prod-pn --prefix  FR/ 
        --target-bucket meta-moses-prod-bkup-pn   --max-loop 0   ** bucket suffix is not required **
		
        Example of an incremental backup of all documents created between  YYY-MM-DDT10:00:00Z and  YYY-MM-DDT012:00:00Z
        
        moses-bc -c $HOME/.clone/config.yaml _backup_ --input-bucket last-loaded-prod --prefix dd/mm/yy \
        --from-date YYY-MM-DDT00:00:00Z --to-date YYY-MM-DDT00:00:00Z \
        --target-bucket meta-moses-prod-bkup-pn  --max-loop 0  ** bucket suffix is not required  **

        Example of an incremental backup of all publication numbers listed in the --input-file

        moses-bc -c $HOME/.clone/config.yaml _backup_ --input-file <file containing a list of publication numbers>  \
        --target-bucket meta-moses-prod-bkup-pn  --max-loop 0   ** bucket suffix is not required **
         
		`,
		Hidden: true,
		Run:    Bucket_backup,
	}

	inFile, outDir, iBucket, delimiter string
	maxPartSize, maxKey                int64
	marker                             string
	maxLoop, maxPage                   int
	incr                               bool
	fromDate                           string
	maxVersions                        int
	frDate                             time.Time
)

type UserMd struct {
	FpClipping string `json:"fpClipping,omitempty"`
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source s3 bucket. Ex: meta-moses-prod-pn-xx, suffix xx is only required for bucket backup")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of the target s3 bucket. Ex: meta-moses-prod-bkp-pn-xx, xx must be the same as source-bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "prefix of a Moses document in the form of cc/pn/kc. Ex: FR/1234")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of moses documents  to be cloned concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent moses pages to be concurrently processed")
	cmd.Flags().IntVarP(&maxVersions, "max-versions", "", 3, "maximum number of backup versions")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", "input file containing the list of moses documents for incremental backup")
	cmd.Flags().StringVarP(&iBucket, "input-bucket", "", "", "input bucket containing the last uploaded documents for incremental backup - Ex: meta-moses-prod-last-loaded")
	// cmd.Flags().StringVarP(&outDir, "output-directory", "o", "", "output directory for --backupMedia = File")
	cmd.Flags().StringVarP(&fromDate, "from-date", "", "1970-01-01T00:00:00Z", "backup objects with last modified from <yyyy-mm-ddThh:mm:ss>")
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 40, "maximum partition size (MB) for multipart upload")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
}

func init() {
	rootCmd.AddCommand(backupCmd)
	initBkFlags(backupCmd)

	// viper.BindPFlag("maxPartSize",rootCmd.PersistentFlags().Lookup("maxParSize"))
}

func Bucket_backup(cmd *cobra.Command, args []string) {

	var (
		nextmarker string
		err        error
	)
	start := time.Now()
	incr = false //  full backup of moses backup

	if len(srcBucket) == 0 {
		gLog.Error.Printf(missingSrcBucket)
		return
	}

	if len(tgtBucket) == 0 {
		gLog.Warning.Printf("%s", missingTgtBucket)
		return
	}

	if len(inFile) > 0 || len(iBucket) > 0 {

		if len(inFile) > 0 && len(iBucket) > 0 {
			gLog.Error.Printf("--input-file  and --input-bucket are mutually exclusive", inFile, iBucket)
			return
		}
		if len(prefix) > 0 {
			gLog.Warning.Printf("Prefix is ignored with --input-file or --input-bucket ")
			prefix = ""
		}
		/*
			Prepare to Scan the input file
		*/
		if len(inFile) > 0 {
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
				return
			}
			// maxVersions = viper.GetInt("backup.MaximumNumberOfVersions")
		}
		incr = true
		// bucket must not  have a suffix
		if mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s must not have a suffix", srcBucket)
			return
		}
		if mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("target bucket %s must not have a suffix", tgtBucket)
			return
		}

	}
	/*
		if len(prefix) > 0 && len(inFile) == 0 && len(iBucket) == 0 {
			gLog.Warning.Printf("--prefix %s is ignored for full backup", prefix)
			prefix = ""
		}
	*/

	if len(prefix) > 0 {
		/*
			get the suffix of the bucket and append it to the source bucket
		*/

		if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}

		/*
			get the suffix of the bucket and append it to the target  bucket
		*/
		if err, suf := mosesbc.GetBucketSuffix(tgtBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				tgtBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the target Bucket %s", suf, tgtBucket)
			}
		}
	} else {
		//  Full backup
		if !incr && !mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s does not have a suffix. It should be  00..05 ", srcBucket)
			return
		}
		if !incr && !mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("Target bucket %s does not have a suffix. It should be  00..05 ", tgtBucket)
			return
		}
	}

	// Check  the suffix of both and target buckets
	if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
		gLog.Warning.Printf("%v", err)
		return
	}

	// validate fram date format
	if len(fromDate) > 0 {
		if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", fromDate)
			return
		}
	}

	/*
			Setup the source sproxyd url, driver and environment ( moses data)
		    Create a session to the the Source S3 cluster ( moses metadata )
			Create a session to the the target  S3 cluster ( moses backup metadata + data)
	*/

	if err = mosesbc.SetSourceSproxyd("backup", srcUrl, driver, env); err != nil {
		return
	}
	maxPartSize = maxPartSize * 1024 * 1024
	// srcS3 = mosesbc.CreateS3Session("backup", "source")
	if srcS3 = mosesbc.CreateS3Session("backup", "source"); srcS3 == nil {
		gLog.Error.Printf("Failed to create a S3 source session")
		return
	}
	// tgtS3 = mosesbc.CreateS3Session("backup", "target")
	if tgtS3 = mosesbc.CreateS3Session("backup", "target"); tgtS3 == nil {
		gLog.Error.Printf("Failed to create a S3 target session")
		return
	}

	// start the backup
	if nextmarker, err = backup_bucket(); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/*
	func backup_bucket(marker string, srcS3 *s3.S3, srcBucket string, tgtS3 *s3.S3, tgtBucket string) (string, error) {
*/
func backup_bucket() (string, error) {
	var (
		nextmarker, token               string
		N                               int
		tdocs, tpages, tsizes, tdeletes int64
		terrors                         int
		mu, mt                          sync.Mutex
		mu1                             sync.Mutex
		incr                            bool = false
		req, reql                       datatype.ListObjV2Request
	)

	req = datatype.ListObjV2Request{
		Service:           srcS3,
		Bucket:            srcBucket,
		Prefix:            prefix,
		MaxKey:            int64(maxKey),
		Marker:            marker,
		Continuationtoken: token,
	}

	if len(inFile) > 0 || len(iBucket) > 0 {
		incr = true
		if len(iBucket) > 0 {
			reql = datatype.ListObjV2Request{
				Service:           srcS3,
				Bucket:            iBucket,
				Prefix:            prefix,
				MaxKey:            int64(maxKey),
				Marker:            marker,
				Continuationtoken: token,
			}
		}
	}

	for {
		var (
			result           *s3.ListObjectsV2Output
			versionId         string
			err              error
			ndocs            int = 0
			npages, ndeletes int = 0, 0
			docsizes         int = 0
			gerrors          int = 0
			key, method      string
		)
		N++ // number of loop
		if !incr {
			result, err = api.ListObjectV2(req)
		} else {
			if len(inFile) > 0 {
				result, err = ListPn(listpn, int(maxKey))
			} else {
				result, err = api.ListObjectV2(reql)
			}
		}
		if err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						gLog.Info.Printf("Key: %s - Size: %d - LastModified: %v", *v.Key, *v.Size, v.LastModified)
						svc := req.Service
						var buck string
						if incr {
							buck = mosesbc.SetBucketName(*v.Key, req.Bucket)
						} else {
							buck = req.Bucket
						}
						if len(inFile) > 0 {
							if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
								gLog.Error.Printf("%v", err)
								continue
							}
						} else {
							//  full backup : method = PUT
							key = *v.Key
							method = "PUT"
						}
						request := datatype.StatObjRequest{
							Service: svc,
							Bucket:  buck,
							Key:     key,
						}
						if method == "PUT" {
							ndocs += 1
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, method string) {
							var (
								rh = datatype.Rh{
									Key: request.Key,
								}
								np, status, docsize, npage int
								err                        error
								usermd                     string
							)
							defer wg1.Done()
							gLog.Trace.Printf("Method %s - Key %s ", method, request.Key)
							if method == "PUT" {
								rh.Result, rh.Err = api.StatObject(request)
								versionId = *rh.Result.VersionId
								if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
									userm := UserMd{}
									json.Unmarshal([]byte(usermd), &userm)
									pn := rh.Key
									if np, err = strconv.Atoi(userm.TotalPages); err == nil {
										nerr, document := BackupPn(pn, np, usermd, versionId, maxPage)
										if nerr > 0 {
											mt.Lock()
											gerrors += nerr
											mt.Unlock()
										} else {
											docsize = (int)(document.Size)
											npage = (int)(document.NumberOfPages)
										}
									} else {
										gLog.Error.Printf("Document %s - S3 metadata has an invalid number of pages in %s - Try to get it from the document user metadata ", pn, usermd)
										if np, err, status = mosesbc.GetPageNumber(pn); err == nil {
											nerr, document := BackupPn(pn, np, usermd, versionId,maxPage)
											if nerr > 0 {
												mt.Lock()
												gerrors += nerr
												mt.Unlock()
											} else {
												docsize = (int)(document.Size)
												npage = (int)(document.NumberOfPages)
											}

										} else {
											gLog.Error.Printf(" Error %v - Status Code: %v  - Getting number of pages for %s ", err, status, pn)
											mt.Lock()
											gerrors += 1
											mt.Unlock()
										}
									}
								}
								mu.Lock()
								npages += npage
								docsizes += docsize
								mu.Unlock()
							} // end PUT method

							if method == "DELETE" {
								ndel, nerr := deleteVersions(request)
								if  ndel > 0 {
									mu1.Lock()
									ndeletes += ndel
									mu1.Unlock()
								}
								if nerr > 0 {
									mt.Lock()
									gerrors += nerr
									mt.Unlock()
								}
							}
						}(request, method)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					if !incr {
						token = *result.NextContinuationToken
					}
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				gLog.Info.Printf("Number of backed up documents: %d  - Number of pages: %d  - Document size: %d - Number of deletes: %d - Number of errors: %d", ndocs, npages, docsizes, ndeletes, gerrors)
				tdocs += int64(ndocs)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				tdeletes += int64(ndeletes)
				terrors += gerrors
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			gLog.Info.Printf("Total number of backed up documents: %d - total number of pages: %d  - Total document size: %d - Total number of older versions deleted %d -  Total number of errors: %d", tdocs, tpages, tsizes, tdeletes, terrors)
			break
		}
	}
	return nextmarker, nil
}

//func inc_backup(listpn *bufio.Scanner, srcS3 *s3.S3, srcBucket string, tgtS3 *s3.S3, tgtBucket string) (string, error) {

func printErr(errs []error) {
	for _, e := range errs {
		gLog.Error.Println(e)
	}
}

func ListPn(buf *bufio.Scanner, num int) (*s3.ListObjectsV2Output, error) {

	var (
		T            = true
		Z      int64 = 0
		D            = time.Now()
		result       = &s3.ListObjectsV2Output{
			IsTruncated: &T,
		}
		err     error
		objects []*s3.Object
	)
	for k := 1; k <= num; k++ {
		var object s3.Object
		if buf.Scan() {
			if text := buf.Text(); len(text) > 0 {
				object.Key = &text
				object.Size = &Z
				object.LastModified = &D
				objects = append(objects, &object)
				result.StartAfter = &text
			} else {
				T = false
				result.IsTruncated = &T
			}
		} else {
			T = false
			result.IsTruncated = &T
		}
	}
	result.Contents = objects
	return result, err
}

func BackupPn(pn string, np int, usermd string, versionId string, maxPage int) (int, *documentpb.Document) {

	var (
		nerrs    = 0
		document *documentpb.Document
		errs     []error
	)
	if errs, document = mosesbc.BackupAllBlob(pn, np, maxPage); len(errs) == 0 {

		/*
			Add  s3 moses metadata to the document even if it may be  invalid in the source bucket
			the purpose of the backup is not to fix source data
		*/

		document.S3Meta = base64.StdEncoding.EncodeToString([]byte(usermd))
		document.VersionId = versionId
		document.LastUpdated = timestamppb.Now()
		var buck1 string
		if incr {
			buck1 = mosesbc.SetBucketName(pn, tgtBucket)
		} else {
			buck1 = tgtBucket
		}
		if _, err := writeS3(tgtS3, buck1, maxPartSize, document); err != nil {
			gLog.Error.Printf("Error:%v writing document: %s to bucket %s", err, document.DocId, bucket)
			nerrs += 1
		}
		/*
			version management can be implement here
			list all the versions and delete the oldest one
		*/

	} else {
		printErr(errs)
		nerrs += len(errs)
	}
	return nerrs, document
}

func writeS3(service *s3.S3, bucket string, maxPartSize int64, document *documentpb.Document) (interface{}, error) {

	if maxPartSize > 0 && document.Size > maxPartSize {
		gLog.Warning.Printf("Multipart upload %s - size %d - max part size %d", document.DocId, document.Size, maxPartSize)
		return mosesbc.WriteS3Multipart(service, bucket, maxPartSize, document)
	} else {
		return mosesbc.WriteS3(service, bucket, document)
	}
}


func deleteVersions(request datatype.StatObjRequest) (int,int){
	// take the oldest version
	var (
		svc      = request.Service
		bucket   = request.Bucket
		key      = request.Key
		versions []*string
		ndeletes, nerrors int
	)

	listreq := datatype.ListObjVersionsRequest{
		Service: svc,
		Bucket:  bucket,
		Prefix:  prefix,
	}

	if listvers, err := api.ListObjectVersions(listreq); err == nil {

		objVersions := listvers.Versions
		l := len(objVersions)
		if l > maxVersions {
			for k := maxVersions; k < l; k++ {
				versions = append(versions, objVersions[k].VersionId)
			}
		}
		delreq := datatype.DeleteObjRequest{
			Service: request.Service,
			Bucket:  request.Bucket,
			Key:     key,
		}
		var delreqs []*datatype.DeleteObjRequest

		for k := 0; k < len(versions); k++ {
			delreq.VersionId = *versions[k]
			delreqs = append(delreqs, &delreq)
		}
		if ret, err := api.DeleteObjectVersions(delreqs); err == nil {
			for _, del := range ret.Deleted {
				gLog.Info.Printf("Object %s - Version id %s is deleted from %s", del.Key, del.VersionId, request.Bucket)
			}
			ndeletes = len(ret.Deleted)
		} else {
			gLog.Error.Printf("Error %v  while deleting multiple versions of %s in %s", err, request.Key, request.Bucket)
			nerrors +=1
		}

	} else {
		gLog.Error.Printf("Error %v  while listing versions of %s in  %s", err, request.Key, request.Bucket)
		nerrors +=1
	}
	return ndeletes,nerrors
}
