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
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	"github.com/paulmatencio/s3c/moses-bc/db"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

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

	inFile, outDir, dbDir, mBDB, iBucket, logBucket, delimiter string
	maxPartSize, maxKey                                        int64
	marker                                                     string
	maxLoop, maxPage                                           int
	incr, logit, resume                                        bool
	fromDate, toDate                                           string
	maxVersions                                                int
	frDate, tDate                                              time.Time
	ctimeout                                                   time.Duration
	myBdb                                                      *db.BadgerDB
	backupContext                                              *meta.BackupContext
	ec                                                         *meta.ErrorContext
	bInstance                                                  int
	skipInput       int
	bNSpace                                                    string
	keySuffix                                                  = "next_marker"
	errSuffix                                                  = "errors"
)

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", uSrcBucket)
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", uTgtBucket)
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 21, uMaxkey)
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, uMaxPage)
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, uMaxLoop)
	cmd.Flags().StringVarP(&marker, "marker", "M", "", uMaker)
	cmd.Flags().IntVarP(&maxVersions, "max-versions", "", 3, uMaxVerion)
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().StringVarP(&iBucket, "input-bucket", "", "", uInputBucket)
	// cmd.Flags().StringVarP(&outDir, "output-directory", "o", "", "output directory for --backupMedia = File")
	cmd.Flags().StringVarP(&fromDate, "from-date", "", "1970-01-01T00:00:00Z", uFromDate)
	cmd.Flags().StringVarP(&toDate, "to-date", "", "", uToDate)
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 64, uMaxPartSize)
	cmd.Flags().IntVarP(&maxCon, "max-con", "", 5, "maximum concurrent parts uploaded , 0 => all parts for a given document will be concurrently processed")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", uSrcUrl)
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", uSrcDriver)
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", uSrcEnv)
	cmd.Flags().DurationVarP(&ctimeout, "ctimeout", "", 10, uCtimeout)
	cmd.Flags().BoolVarP(&logit, "logit", "", true, uLogit)
	cmd.Flags().BoolVarP(&check, "check", "", true, uDryRun)
	cmd.Flags().StringVarP(&bNSpace, "name-space", "n", "backup", uNameSpace)
	cmd.Flags().StringVarP(&mBDB, "db-name", "d", "moses-bdb", uDatabase)
	cmd.Flags().IntVarP(&bInstance, "instance", "", 1, "backup instance number")
	cmd.Flags().StringVarP(&dbDir, "db-directory", "D", "", "data base directory")
	cmd.Flags().BoolVarP(&resume, "resume", "", false, "resume the process where it was stopped")
}

func init() {
	rootCmd.AddCommand(backupCmd)
	initBkFlags(backupCmd)
	// viper.BindPFlag("maxPartSize",rootCmd.PersistentFlags().Lookup("maxParSize"))
}

func BackupPns(cmd *cobra.Command, args []string) {

	var (
		nextmarker   string
		myContext    *meta.BackupContext
		errorContext *meta.ErrorContext
		err          error
	)
	//  create an error context structure
	ec = errorContext.New()
	keySuffix = keySuffix + "_" + strconv.Itoa(bInstance)
	errSuffix = errSuffix + "_" + strconv.Itoa(bInstance)

	/*
	initialize  the backup context struct and badger db
	*/
	err, myContext, myBdb = initBackup()
	if err != nil {
		gLog.Error.Printf("%v", err)
		if myBdb != nil {
			ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/init-backup"), []byte(err.Error()), myBdb)
		}
		return
	} else {
		defer myBdb.Close()
		myContext.WriteBdb([]byte(bNSpace), []byte(keySuffix), myBdb)
		skipInput = myContext.NextIndex
	}

	/* start  the monitoring in the background if requested  with -P or --profile  */
	mosesbc.Profiling(profiling)

	/*
		create a request for  the backup
	*/

	reqm := datatype.Reqm{
		SrcS3:       srcS3,
		SrcBucket:   srcBucket,
		TgtS3:       tgtS3,
		TgtBucket:   tgtBucket,
		Incremental: incr,
	}
	// start the backup operation of both  moses data and directories
	start := time.Now()
	if nextmarker, err = backupPns(reqm, myContext); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

/*
		Called by BackupPns

        1) List the input-source ( source-bucket or input-bucket or input-file) from a given marker
           2) for each  Moses publication number pn_i on the returned list ( up to --max-key)
 		     	  2.1)	create a empty backup envelop  for pn-i
				  2.2) retrieve the  document pn-i metadata
                  2.3) add the document pn-i metadata to the backup  envelop
                  2.4) if pdf then
                     2.4.1) retrieve the pn-i's pdf document
                     2.4.2) add the pn-i's pdf  object to the backup envlop

                  2.5) for each page of pn-i ( page 0 inclusive if it exists)
                     2.5.1) retrieve the page's object and page 's metadata
                     2.5.2) add page's object and page's metadata  to the backup envelop

                   2.6) send  the backup envelop and its directory to  S3 buckets

           3) Is next-marker or loop > -max-loop ?
            	3.2 yes then Exit
                3.2 No then  Back to List the input-source (1) from the next -marker

         *Note
          2.1 to 2.6 pn-i documents are performed concurrently. The level of concurrency is limited by the value of --max-key
          2.5.1 to 2.5.2  pages of pn-i are performed concurrently. The level  of concurrency is limited by the value of -- max-page
		  2.6 if the backup envelop is bigger > --max-page-size then perform multipart upload
          3) the number of Loop over max-key PN is limited by the value of --max-loop .  Setting --max-loop = 0  will iterate
             over the list until the value next-marker  is empty
*/

func backupPns(reqm datatype.Reqm, c *meta.BackupContext) (string, error) {

	var (
		nextmarker, token               string
		N ,nextIndex                    int
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
			start                  = time.Now()
		)
		N++ // number of loop
		if len(inFile) == 0 {
			gLog.Info.Printf("List documents from bucket %s", reqm.SrcBucket)
			result, err = api.ListObjectWithContextV2(ctimeout, req)
		} else {
			gLog.Info.Printf("List documents from file %s", inFile)
			result, err = mosesbc.ListPn(listpn, int(maxKey))
		}

		if err == nil {
			if l := len(result.Contents); l > 0 {
				var (
					wg1       sync.WaitGroup
					backupLog = []*mosesbc.LogBackup{}
				)

				for _, v := range result.Contents {
					//skip the number of already processed pn taken from the input file
					if len(inFile) > 0 {
						for sk:=1 ; sk<= skipInput; sk++ {
							listpn.Scan()
							continue
						}
					}
					if *v.Key != nextmarker {
						key := *v.Key
						service := req.Service
						/*
							method  is always PUT for full backup( --source-bucket)
							or  incremental backup  if --input-bucket
						*/
						method := "PUT"
						buck := req.Bucket
						if len(iBucket) > 0 {
							/*
								    input-bucket's key  should be in the form
									yyyymmdd/cc/pn/kc
							*/
							if err, key = mosesbc.ParseInputKey(*v.Key); err != nil {
								gLog.Error.Printf("Error %v in bucket %s", err, iBucket)
								ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+*v.Key), []byte(err.Error()), myBdb)
								continue // skip it
							}
							buck = mosesbc.SetBucketName(key, req.Bucket)
						} else {
							if len(inFile) > 0 {
								if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
									gLog.Error.Printf("%v", err)
									ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+*v.Key), []byte(err.Error()), myBdb)
									continue // skip it
								}
								buck = mosesbc.SetBucketName(key, req.Bucket)
							}
						}
						gLog.Info.Printf("Bucket: %s - Key: %s - Size: %d - LastModified: %v", buck, key, *v.Size, v.LastModified)

						/*
							get the s3 metadata
						*/
						request := datatype.StatObjRequest{
							Service: service,
							Bucket:  buck,
							Key:     key,
						}

						if method == "PUT" {
							ndocs += 1
						}

						nextIndex += 1

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
										userm := meta.UserMd{}
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
												err1 := fmt.Sprintf(" Error %v - Status Code: %v  - Getting number of pages for %s ", err, status, pn)
												gLog.Error.Printf("%s", err1)
												ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+pn), []byte(err1), myBdb)
												mt.Lock()
												nerrors += 1
												mt.Unlock()
											}
										}
									}
								} else {
									err1 := fmt.Sprintf("Stat object %s return with error %v ", rh.Key, rh.Err)
									gLog.Error.Printf("%s", err1)
									ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+rh.Key), []byte(err1), myBdb)
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
					c.SetMarker(nextmarker)
					c.SetNextIndex(nextIndex)
					if !incr {
						token = *result.NextContinuationToken
					}
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)

					if myBdb != nil {
						c.WriteBdb([]byte(bNSpace), []byte(keySuffix), myBdb)
					}
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
						gLog.Info.Printf("Dry run: log to bucket %s - S3 endpoint %s", logBucket, logS3.Endpoint)
					}
					gLog.Info.Printf("Time to log %d entries %v", len(backupLog), time.Since(start1))
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/list-input"), []byte(err.Error()), myBdb)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
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
		buck1 := mosesbc.SetBucketName1(incr, pn, tgtBucket)
		start := time.Now()
		if !check {
			if _, err := writeS3(tgtS3, buck1, maxPartSize, document); err != nil {
				err1 := errors.New(fmt.Sprintf("Error %v writing S3 document %s to bucket %s", err, document.DocId, buck1))
				gLog.Error.Printf("%v", err1)
				ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+buck1+"/"+document.DocId), []byte(err1.Error()), myBdb)
				nerrs += 1
			} else {
				gLog.Info.Printf("Time to upload the backup document %s to bucket %s : %v ", document.DocId, buck1, time.Since(start))
			}
		} else {
			gLog.Info.Printf("Dry Run: Writing document %s to bucket %s - S3 endpoint %s", document.DocId, buck1, tgtS3.Endpoint)
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

func SetBackupContext(nextIndex int) (c *meta.BackupContext) {
	c = backupContext.New()
	c.SrcUrl = srcUrl
	c.Env = env
	c.Driver = driver
	c.SrcBucket = srcBucket
	c.TgtBucket = tgtBucket
	c.Marker = marker
	c.ToDate = toDate
	c.FromDate = fromDate
	c.Infile = inFile
	c.IBucket = iBucket
	c.DbDir = dbDir
	c.DbName = mBDB
	c.Maxloop = maxLoop
	c.MaxPage = maxPage
	c.MaxKey = maxKey
	c.Marker = marker
	c.MaxPageSize = maxPartSize
	c.NameSpace = bNSpace
	c.Logit = logit
	c.Check = check
	c.CtimeOut = ctimeout
	c.NextIndex = nextIndex
	c.BackupIntance = bInstance
	return
}

func getBackupContext(c *meta.BackupContext) (nextIndex int){
	// c = context.New()
	srcUrl = c.SrcUrl
	env = c.Env
	driver = c.Driver
	srcBucket = c.SrcBucket
	tgtBucket = c.TgtBucket
	marker = c.Marker
	toDate = c.ToDate
	fromDate = c.FromDate
	inFile = c.Infile
	iBucket = c.IBucket
	dbDir = c.DbDir
	mBDB = c.DbName
	maxLoop = c.Maxloop
	maxPage = c.MaxPage
	maxKey = c.MaxKey
	marker = c.Marker
	maxPartSize = c.MaxPageSize
	bNSpace = c.NameSpace
	logit = c.Logit
	check = c.Check
	ctimeout = c.CtimeOut
	nextIndex = c.NextIndex
	return
}

func openBdb(name string) (myBdb *db.BadgerDB, err error) {

	if len(dbDir) == 0 {
		if home, err := os.UserHomeDir(); err == nil {
			dbDir = home
		} else {
			return nil, err
		}
	}
	dbf := filepath.Join(dbDir, name)
	gLog.Info.Printf("Creating Badger DB name is %s", dbf)
	myBdb, err = db.NewBadgerDB(dbf, nil)
	return

}

func restart(ns []byte, key []byte, myBdb *db.BadgerDB) (err error, nextIndex int ) {
    var (
    	c = backupContext.New()
	)

	gLog.Trace.Printf("Name space %s - Key %s  - BDB %v ", ns, key, myBdb)
	if err = c.ReadBbd(ns, key, myBdb); err == nil {
		nextIndex= getBackupContext(c)
		err = printContext(c)
	}
	return
}

func printContext(c *meta.BackupContext) error {
	var (
		err   error
		cJSON []byte
	)
	if cJSON, err = json.MarshalIndent(c, "", "  "); err == nil {
		fmt.Printf("Resume the process with the following arguments %s\n", string(cJSON))
	}
	return err
}

func initBackup() (err error, myContext *meta.BackupContext, myBdb *db.BadgerDB) {

	/*
			Open badger db ( mBDB = database-name)
		    badger database is used to store the backup's context struct and backup's errors
		    The backup's context is used for restarting a failed backup or continuing  the previous backup with the last next-marker

		    Backup's context and errors are stored in the  namespace "backup"
	*/
	var nextIndex int
	if myBdb, err = openBdb(mBDB); err == nil {
		if resume {
			if err,nextIndex = restart([]byte(bNSpace), []byte(keySuffix), myBdb); err != nil {
				myContext.SetNextIndex(nextIndex)
				return
			}
		}
	} else {
		if resume {
			err = errors.New(fmt.Sprintf("Can't run backup  with --resume=true because of previous error %v", err))
			return
		}
	}

	incr = false //  full backup of moses backup

	if len(srcBucket) == 0 {
		err = errors.New(fmt.Sprintf(missingSrcBucket))
		return
	}

	if len(tgtBucket) == 0 {
		err = errors.New(fmt.Sprintf(missingTgtBucket))
		return
	}

	//  Create the context  of the backup to store backup state
	myContext = SetBackupContext(nextIndex)

	if len(inFile) > 0 || len(iBucket) > 0 {

		if len(inFile) > 0 && len(iBucket) > 0 {
			err = errors.New(fmt.Sprintf("--input-file  and --input-bucket are mutually exclusive", inFile, iBucket))
			return
		}
		/*
			Prepare to Scan the input file
		*/
		if len(inFile) > 0 {
			if listpn, err = utils.Scanner(inFile); err != nil {
				err = errors.New(fmt.Sprintf("Error %v  scanning --input-file  %s ", err, inFile))
				return
			}

			if maxv := viper.GetInt("backup.maximumNumberOfVersions"); maxv > 0 {
				maxVersions = maxv
			}

		}
		incr = true
		/*
				if incremental then check buckets name - bucket name must not  have a suffix
			    The suffix of the bucket will be derived from the country code of the  key
		*/
		if mosesbc.HasSuffix(srcBucket) {
			// gLog.Error.Printf("Source bucket %s must not have a suffix", srcBucket)
			err = errors.New(fmt.Sprintf("Source bucket %s must not have a suffix", srcBucket))
			return
		}
		if mosesbc.HasSuffix(tgtBucket) {
			//gLog.Error.Printf("target bucket %s must not have a suffix", tgtBucket)
			err = errors.New(fmt.Sprintf("Target bucket %s must not have a suffix", srcBucket))
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
				Compute the suffix of the source bucket from the CC of the prefix
			    and append the suffix to the bucket name
		*/

		if err1, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err1 != nil {
			err = errors.New(fmt.Sprintf("%v", err1))
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}

		/*
				Do the same with the target bucket name
				Compute the suffix of the  target bucket from the CC of the prefix
			    and append the suffix to the bucket name
		*/

		if err1, suf := mosesbc.GetBucketSuffix(tgtBucket, prefix); err1 != nil {
			err = errors.New(fmt.Sprintf("%v", err1))
			// gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				tgtBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the target Bucket %s", suf, tgtBucket)
			}
		}
	} else {
		/*
			if it is Full backup then both source and target bucket must have  a suffix
		*/
		if !incr && !mosesbc.HasSuffix(srcBucket) {
			err = errors.New(fmt.Sprintf("Source bucket %s does not have a suffix. It should be  00..05 ", srcBucket))
			return
		}
		if !incr && !mosesbc.HasSuffix(tgtBucket) {
			err = errors.New(fmt.Sprintf("Target bucket %s does not have a suffix. It should be  00..05 ", srcBucket))
			// gLog.Error.Printf("Target bucket %s does not have a suffix. It should be  00..05 ", tgtBucket)
			return
		}
	}

	/*
		Check the suffix of both source and target buckets, they must be equal
	*/
	if err1 := mosesbc.CheckBucketName(srcBucket, tgtBucket); err1 != nil {
		// gLog.Warning.Printf("%v", err)
		err = errors.New(fmt.Sprintf("%v", err1))
		return
	}

	// validate --from-date format
	if len(fromDate) > 0 {
		if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
			err = errors.New(fmt.Sprintf("Wrong --from-date format %s", fromDate))
			return
		}
	}
	// validate --to-date format
	if len(toDate) > 0 {
		if tDate, err = time.Parse(time.RFC3339, toDate); err != nil {
			// gLog.Error.Printf("Wrong date format %s", toDate)
			err = errors.New(fmt.Sprintf("Wrong --to-date format %s", toDate))
			return
		}
	}

	/*
			Setup the source sproxyd url, driver and environment to access the moses data)
		    Create a session to Source S3 cluster to access the moses metadata
			Create a session to target  S3 cluster for the backup of both moses metadata and moses data)
	*/

	if err = mosesbc.SetSourceSproxyd("backup", srcUrl, driver, env); err != nil {
		return
	}
	maxPartSize = maxPartSize * 1024 * 1024
	if srcS3 = mosesbc.CreateS3Session("backup", "source"); srcS3 == nil {
		err = errors.New(fmt.Sprintln("Failed to create a session with the source S3 endpoint"))
		return
	}

	if tgtS3 = mosesbc.CreateS3Session("backup", "target"); tgtS3 == nil {
		//  gLog.Error.Printf("Failed to create a session with the target S3")
		err = errors.New(fmt.Sprintln("Failed to create a session with the target S3 endpoint"))
		return
	}

	/*
		if --logit = true  then
			create a s3 session to the backup  logger bucket
			This logS3 bucket may  be used for quicker  restore  by date
	*/
	if logit {
		if logS3 = mosesbc.CreateS3Session("backup", "logger"); logS3 == nil {
			err = errors.New(fmt.Sprintln("Failed to create a session with the logs3 S3 endpoint"))
			//gLog.Error.Printf("Failed to create a S3 session with the logger s3")
			return
		} else {
			/*  Get the logger  bucket name  */
			k := "backup.s3.logger.bucket"
			if logBucket = viper.GetString(k); len(logBucket) == 0 {
				err = errors.New(fmt.Sprintf("logger bucket is missing - add %s to the config.yaml file", k))
				return
			}
		}
	}

	return
}
