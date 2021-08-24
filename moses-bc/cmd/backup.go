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
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"

	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/viper"
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
		Short: "Command to backup a MOSES  objects to S3 or files",
		Long: `        
        Command to backup Moses data and Moses directories to S3 or to files
      
        Moses data are stored in Scality Ring native object storage accessed via sproxyd driver 
        Moses directories are stored in S3 buckets. Each bucket name has a suffix ( <bucket-name>-xx ; xx=00..05)
        
        Usage:

          moses-bc --help or -h  to list all the commands
          moses-bc  <command> -h or --help to list all the arguments related to a specific <command>
  
        Config file:

      	The Default config file is located $HOME/.clone/config.yaml. Use the --config or -c to change the default 
           moses-bc -c  <full path of another config.file>  _backup_  .....

        Example of a Bucket name:

           - Full name :  meta-moses-prod-pn-xx   ( xx = 00..05)
                Full name is  used for full backup 
           
           - Partial name:  meta-moses-prod-pn    ( without suffix)
                Partial name is used when a --prefix , --input-file  or --input-bucket  is specified 
                Partial is used for incremental backup 
           
        Example of full backup of document ids which are indexed by the S3 --source-bucket meta-moses-prod-pn-01

          moses-bc  _backup_ --source-bucket meta-moses-prod-pn-01 --target-bucket meta-moses-prod-bkup-pn-01 --max-loop 0
        **  Pay attention, both source and target bucket must have the same suffix, for instance -01 in this case ** 

        Example of backup of documents whose name started with a given --prefix.
        The backup tool will append a suffix to the given bucket names
  
          moses-bc - _backup_ --source-bucket meta-moses-prod-pn --prefix  FR/  --target-bucket meta-moses-prod-bkup-pn --max-loop 0   
        ** bucket suffix is not required **
		
        Example of an incremental backup of documents loaded  between  2021-11-01T07:00:00Z and  2021-11-01T08:00:00Z.
          The object key of the last-loaded bucket layout is :  YYYYMMDD/CC/PN/KC  
          
          moses-bc  _backup_ --input-bucket last-loaded-prod --prefix 20211101 --from-date 2021-11-01T07:00:00Z --to-date 2021-11-01T08:00:00Z \
          --target-bucket meta-moses-prod-bkup-pn  --max-loop 0  
        ** bucket suffix is not required  **

        Example of an incremental backup of the publication ids listed in the --input-file

          moses-bc -c $HOME/.clone/config.yaml _backup_ --input-file <file containing a list of publication numbers>  \
          --target-bucket meta-moses-prod-bkup-pn  --max-loop 0   
        ** bucket suffix is not required **
        ** input-file record layout:  <Method> CC/PN/KC . Method = [PUT|DELETE]  

        Example of backup which is not using default config file
          moses-bc -c $HOME/.clone/config-osa.yaml _backup_  ...
		`,
		Hidden: true,
		Run:    BackupPns,
	}

	inFile, outDir, iBucket, logBucket, delimiter string
	maxPartSize, maxKey                           int64
	marker                                        string
	maxLoop, maxPage                              int
	incr, logit                                   bool
	fromDate, toDate                              string
	maxVersions                                   int
	frDate, tDate                                 time.Time
	ctimeout                                      time.Duration
)

type UserMd struct {
	FpClipping string `json:"fpClipping,omitempty"`
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", uSrcBucket)
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", uTgtBucket)
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 21, uMaxkey)
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, uMaxPage)
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, uMaxLopp)
	cmd.Flags().StringVarP(&marker, "marker", "M", "", uMaker)
	cmd.Flags().IntVarP(&maxVersions, "max-versions", "", 3, uMaxVerion)
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().StringVarP(&iBucket, "input-bucket", "", "", "input bucket containing the last uploaded documents for the incremental backup - Ex: meta-moses-prod-last-loaded")
	// cmd.Flags().StringVarP(&outDir, "output-directory", "o", "", "output directory for --backupMedia = File")
	cmd.Flags().StringVarP(&fromDate, "from-date", "", "1970-01-01T00:00:00Z", uFromDate)
	cmd.Flags().StringVarP(&toDate, "to-date", "", "", uToDate)
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 64, uMaxPartSize)
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "the list of source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().DurationVarP(&ctimeout, "ctimeout", "", 10, uCtimeout)
	cmd.Flags().BoolVarP(&logit, "logit", "", true, "enable backup history log")
	cmd.Flags().BoolVarP(&check, "check", "", true, uDryRun)
}

func init() {
	rootCmd.AddCommand(backupCmd)
	initBkFlags(backupCmd)
	// viper.BindPFlag("maxPartSize",rootCmd.PersistentFlags().Lookup("maxParSize"))
}

func BackupPns(cmd *cobra.Command, args []string) {

	var (
		nextmarker string
		err        error
	)

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
		/*
			Prepare to Scan the input file
		*/
		if len(inFile) > 0 {
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
				return
			}

			if maxv := viper.GetInt("backup.maximumNumberOfVersions"); maxv > 0 {
				maxVersions = maxv
			}

		}
		incr = true
		// check buckets name - bucket name  must not  have a suffix
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

	// Check  the suffix of both source and target buckets
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
	if len(toDate) > 0 {
		if tDate, err = time.Parse(time.RFC3339, toDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", toDate)
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
		gLog.Error.Printf("Failed to create a session with the source S3")
		return
	}
	// tgtS3 = mosesbc.CreateS3Session("backup", "target")
	if tgtS3 = mosesbc.CreateS3Session("backup", "target"); tgtS3 == nil {
		gLog.Error.Printf("Failed to create a session with the target S3")
		return
	}

	if logit {
		if logS3 = mosesbc.CreateS3Session("backup", "logger"); logS3 == nil {
			gLog.Error.Printf("Failed to create a S3 session with the logger s3")
			return
		} else {
			/*  get the log bucket name  */
			k := "backup.s3.logger.bucket"
			if logBucket = viper.GetString(k); len(logBucket) == 0 {
				gLog.Error.Printf("logger bucket is missing - add %s to the config.yaml file")
				return
			}
		}
	}

	mosesbc.Profiling(profiling)

	reqm := datatype.Reqm{
		SrcS3:       srcS3,
		SrcBucket:   srcBucket,
		TgtS3:       tgtS3,
		TgtBucket:   tgtBucket,
		Incremental: incr,
	}
	// start the backup
	start := time.Now()
	if nextmarker, err = backupPns(reqm); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/*
	func backup_bucket(marker string, srcS3 *s3.S3, srcBucket string, tgtS3 *s3.S3, tgtBucket string) (string, error) {
*/
func backupPns(reqm datatype.Reqm) (string, error) {

	var (
		nextmarker, token               string
		N                               int
		tdocs, tpages, tsizes, tdeletes int64
		terrors                         int
		mu, mt, mu1                     sync.Mutex
		incr                            = reqm.Incremental
		req                             datatype.ListObjV2Request
	)

	req = datatype.ListObjV2Request{
		Service:           reqm.SrcS3,
		Bucket:            reqm.SrcBucket,
		Prefix:            prefix,
		MaxKey:            int64(maxKey),
		Marker:            marker,
		Continuationtoken: token,
	}

	//   list from input-bucket

	if len(iBucket) > 0 {
		req = datatype.ListObjV2Request{
			Service:           reqm.SrcS3,
			Bucket:            iBucket,
			Prefix:            prefix,
			MaxKey:            int64(maxKey),
			Marker:            marker,
			Continuationtoken: token,
		}
	}

	for {
		var (
			result           *s3.ListObjectsV2Output
			versionId        string
			err              error
			ndocs            int   = 0
			npages, ndeletes int   = 0, 0
			docsizes         int64 = 0
			nerrors          int   = 0
			// key, method      string
			start = time.Now()
		)
		N++ // number of loop
		if len(inFile) == 0 {
			gLog.Info.Printf("List documents from bucket %s", reqm.SrcBucket)
			result, err = api.ListObjectWithContextV2(ctimeout, req)
		} else {
			gLog.Info.Printf("List documents from file %s", inFile)
			result, err = ListPn(listpn, int(maxKey))
		}

		if err == nil {
			if l := len(result.Contents); l > 0 {
				var (
					wg1       sync.WaitGroup
					backupLog = []*mosesbc.LogBackup{}
				)
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						key := *v.Key
						service := req.Service
						method := "PUT" /* full backup or --input-bucket */
						buck := req.Bucket
						if len(iBucket) > 0 {
							/*
									check the content of the input bucket
								    key  should be in the form
									yyyymmdd/cc/pn/kc
							*/
							if err, key = mosesbc.ParseInputKey(*v.Key); err != nil {
								gLog.Error.Printf("Error %v in bucket %s", err, iBucket)
								continue
							}
							buck = mosesbc.SetBucketName(key, req.Bucket)
						} else {
							if len(inFile) > 0 {
								if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
									gLog.Error.Printf("%v", err)
									continue
								}
								buck = mosesbc.SetBucketName(key, req.Bucket)
							}
						}
						gLog.Info.Printf("Bucket: %s - Key: %s - Size: %d - LastModified: %v", buck, key, *v.Size, v.LastModified)

						/*  get the s3 metadata */
						request := datatype.StatObjRequest{
							Service: service,
							Bucket:  buck,
							Key:     key,
						}

						if method == "PUT" {
							ndocs += 1
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, method string) {
							defer wg1.Done()
							var (
								rh = datatype.Rh{
									Key: request.Key,
								}
								np, npage int
								err       error
								docsize   int64
								s3md      string
								pubDate   string
								loadDate  string
							)
							gLog.Trace.Printf("Method %s - Key %s ", method, request.Key)
							if method == "PUT" {
								rh.Result, rh.Err = api.StatObject(request)
								if rh.Err == nil {
									versionId = *rh.Result.VersionId
									if s3md, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
										userm := UserMd{}
										if err := json.Unmarshal([]byte(s3md), &userm); err == nil {
											pubDate = userm.PubDate
										}
										pn := rh.Key
										if np, err = strconv.Atoi(userm.TotalPages); err == nil {
											nerr, document := backupPn(pn, np, s3md, versionId, maxPage)
											if nerr > 0 {
												mt.Lock()
												nerrors += nerr
												mt.Unlock()
											} else {
												docsize = document.Size
												npage = (int)(document.NumberOfPages)
											}
											loadDate, _ = getLoadDate(document)
										} else {
											gLog.Error.Printf("Document %s - S3 metadata has an invalid number of pages in %s - Try to get it from the document metadata ", pn, s3md)

											if docmd, err, status := mosesbc.GetDocumentMeta(pn); err == nil {
												np = docmd.TotalPage
												pubDate = docmd.PubDate
												nerr, document := backupPn(pn, np, s3md, versionId, maxPage)
												gLog.Trace.Printf("Time from start %v", time.Since(start))
												if nerr > 0 {
													mt.Lock()
													nerrors += nerr
													mt.Unlock()
												} else {
													docsize = document.Size
													npage = (int)(document.NumberOfPages)
												}
												//  loadDate  is used to for logging
												loadDate, _ = getLoadDate(document)
											} else {
												gLog.Error.Printf(" Error %v - Status Code: %v  - Getting number of pages for %s ", err, status, pn)
												mt.Lock()
												nerrors += 1
												mt.Unlock()
											}
										}
									}
								} else {
									gLog.Error.Printf("Error %v  stat object %s", rh.Err, rh.Key)
									mt.Lock()
									nerrors += 1
									mt.Unlock()
								}
								mu.Lock()
								npages += npage
								docsizes += docsize
								mu.Unlock()
								/*
									Prepare to log backup
								*/
								backupLog = append(backupLog, &mosesbc.LogBackup{Method: method, Incremental: reqm.Incremental, Key: request.Key, Bucket: request.Bucket, Pages: npage, Size: docsize, Pubdate: pubDate, Loaddate: loadDate, Errors: nerrors})
							} // end PUT method

							if method == "DELETE" {
								ndel, nerr := deleteVersions(request)
								if ndel > 0 {
									mu1.Lock()
									ndeletes += ndel
									mu1.Unlock()
								}
								if nerr > 0 {
									mt.Lock()
									nerrors += nerr
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
				gLog.Info.Printf("Number of backup documents: %d  - Number of pages: %d  - Document size: %d - Number of deletes: %d - Number of errors: %d - Elapsed time: %v", ndocs, npages, docsizes, ndeletes, nerrors, time.Since(start))
				tdocs += int64(ndocs)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				tdeletes += int64(ndeletes)
				terrors += nerrors
				/*
					log backup before returning
				*/
				if logit {
					start1 := time.Now()
					logReq := mosesbc.LogRequest{
						Service:   logS3,
						Bucket:    logBucket,
						LogBackup: backupLog,
						Ctimeout:  ctimeout,
					}
					if !check {
						mosesbc.Logit(logReq)
					} else {
						gLog.Info.Printf("Dry run: log to bucket %s - S3 endpoint %s",logBucket,logS3.Endpoint)
					}
					gLog.Info.Printf("Time to log %d entries %v", len(backupLog), time.Since(start1))
				}

			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			gLog.Info.Printf("Total number of backed up documents: %d - total number of pages: %d  - Total document size: %d - Total number of older versions deleted %d - Total number of errors: %d - Elapsed time: %v", tdocs, tpages, tsizes, tdeletes, terrors, time.Since(start))
			break
		}
	}

	return nextmarker, nil
}

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

func backupPn(pn string, np int, usermd string, versionId string, maxPage int) (int, *documentpb.Document) {

	var (
		nerrs    = 0
		document *documentpb.Document
		errs     []error
	)
	if errs, document = mosesbc.BackupBlob(pn, np, maxPage, ctimeout); len(errs) == 0 {

		/*
			Add  s3 moses metadata to the document even if it may be  invalid in the source bucket
			the purpose of the backup is not to fix source data
		*/

		document.S3Meta = base64.StdEncoding.EncodeToString([]byte(usermd))
		document.VersionId = versionId
		document.LastUpdated = timestamppb.Now()
		/*
		var buck1 string
		if incr {
			buck1 = mosesbc.SetBucketName(pn, tgtBucket)
		} else {
			buck1 = tgtBucket
		}
		 */

		buck1:= mosesbc.SetBucketName1(incr,pn,tgtBucket)
		start := time.Now()
		if !check {
			if _, err := writeS3(tgtS3, buck1, maxPartSize, document); err != nil {
				gLog.Error.Printf("Error %v writing document %s to bucket %s", err, document.DocId, buck1)
				nerrs += 1
			} else {
				gLog.Info.Printf("Time to upload the backup document %s to bucket %s : %v ", document.DocId, buck1, time.Since(start))
			}
		}  else {
			gLog.Info.Printf("Dry Run: Writing document %s to bucket %s - S3 endpoint %s",document.DocId,buck1,tgtS3.Endpoint)
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
			gLog.Info.Printf("Multipart uploading of %s - size %d - max part size %d", document.DocId, document.Size, maxPartSize)
			return mosesbc.WriteS3Multipart(service, bucket, maxPartSize, document, ctimeout)
		} else {
			return mosesbc.WriteS3(service, bucket, document, ctimeout)
		}

}

func deleteVersions(request datatype.StatObjRequest) (int, int) {
	// take the oldest version
	var (
		svc               = request.Service
		bucket            = request.Bucket
		key               = request.Key
		versions          []*string
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
			nerrors += 1
		}

	} else {
		gLog.Error.Printf("Error %v  while listing versions of %s in  %s", err, request.Key, request.Bucket)
		nerrors += 1
	}
	return ndeletes, nerrors
}

func getLoadDate(document *documentpb.Document) (string, error) {
	var (
		docmd = meta.DocumentMetadata{}
	)
	if err := json.Unmarshal([]byte(document.GetMetadata()), docmd); err == nil {
		return docmd.LoadDate, err
	} else {
		return "", err
	}
}
