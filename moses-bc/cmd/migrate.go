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
	// "encoding/base64"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	// "github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/viper"
	// "google.golang.org/protobuf/types/known/timestamppb"

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
	migrateCmd = &cobra.Command{
		Use:   "_migrate_",
		Short: "Command to migrate MOSES objects to S3 objects",
		Long: `        
        Command to migrate  Moses data and Moses directories
      
        Moses data are stored in Scality Ring native object storage accessed via sproxyd driver 
        Moses directories are stored in S3 buckets. Each bucket name has a suffix ( <bucket-name>-xx ; xx=00..05)
        
        Usage: 
        moses-bc --help or -h  to list all the commands
        moses-bc  <command> -h or --help to list all the arguments related to a specific <command>
  
        Config file: 
      	The Default config file is located $HOME/.clone/config.file 

        Example of full migrate: 

        Backup all the objects listed in the S3 --source-bucket meta-moses-prod-pn-01  to the S3 bucket meta-moses-prod-bkup-pn-01 
     	
        moses-bc -c $HOME/.clone/config.yaml _migrate_ --source-bucket meta-moses-prod-pn-01 \ 
        --target-bucket meta-moses-prod-migr-pn-01 --max-loop 0
        **  suffix is requird and both source and target bucket must have the same suffix, for instance -01 ** 

        Example of backup of all document name started with a specific --prefix 

        moses-bc -c $HOME/.clone/config.yaml _migrate_ --source-bucket meta-moses-prod-pn --prefix  FR/ 
        --target-bucket meta-moses-prod-migr-pn   --max-loop 0   ** bucket suffix is not required **
		
        Example of an incremental backup of all documents created between  YYY-MM-DDT10:00:00Z and  YYY-MM-DDT012:00:00Z
        
        moses-bc -c $HOME/.clone/config.yaml _migrate_ --input-bucket last-loaded-prod --prefix dd/mm/yy \
        --from-date YYY-MM-DDT00:00:00Z --to-date YYY-MM-DDT00:00:00Z \
        --target-bucket meta-moses-prod-migr-pn  --max-loop 0  ** bucket suffix is not required  **

        Example of an incremental migration of all publication numbers listed in the --input-file

        moses-bc -c $HOME/.clone/config.yaml _migrate_ --input-file <file containing a list of publication numbers>  \
        --target-bucket meta-moses-prod-migr-pn  --max-loop 0   ** bucket suffix is not required **
         
		`,
		Hidden: true,
		Run:    MigratePns,
	}
)

func initMgFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source s3 bucket. Ex: meta-moses-prod-pn-xx, suffix xx is only required for bucket backup")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of the target s3 bucket. Ex: meta-moses-prod-bkp-pn-xx, xx must be the same as source-bucket")
	cmd.Flags().StringVarP(&indBucket, "index-bucket", "", "", "name of the index metadata bucket")
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
	cmd.Flags().BoolVarP(&reIndex, "re-index", "", true, "re-index the target moses documents in the index bucket")
	cmd.Flags().DurationVarP(&ctimeout, "--ctimeout", "", 10, "set context background cancel timeout in seconds")
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	initMgFlags(migrateCmd)

	// viper.BindPFlag("maxPartSize",rootCmd.PersistentFlags().Lookup("maxParSize"))
}

func MigratePns(cmd *cobra.Command, args []string) {

	var (
		nextmarker string
		err        error
	)

	mosesbc.MaxPage = maxPage
	mosesbc.Replace = replace
	mosesbc.MaxPageSize = maxPartSize

	incr = false
	if len(srcBucket) == 0 {
		gLog.Error.Printf(missingSrcBucket)
		return
	}

	if len(tgtBucket) == 0 {
		gLog.Warning.Printf("%s", missingTgtBucket)
		return
	}

	if reIndex && len(indBucket) == 0 {
		gLog.Warning.Printf("%s", missingIndBucket)
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

			if maxv := viper.GetInt("backup.maximumNumberOfVersions"); maxv > 0 {
				maxVersions = maxv
			}

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
		if reIndex {
			if mosesbc.HasSuffix(indBucket) {
				gLog.Error.Printf(" Indexing bucket %s must not have a suffix", indBucket)
				return
			}
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

		if reIndex {
			if err, suf := mosesbc.GetBucketSuffix(indBucket, prefix); err != nil {
				gLog.Error.Printf("%v", err)
				return
			} else {
				if len(suf) > 0 {
					indBucket += "-" + suf
					gLog.Warning.Printf("A suffix %s is appended to the index Bucket %s", suf, indBucket)
				}
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
		if reIndex {
			if !incr && !mosesbc.HasSuffix(tgtBucket) {
				gLog.Error.Printf("Indexing bucket %s does not have a suffix. It should be  00..05 ", indBucket)
				return
			}
		}

	}

	// Check  the suffix of both and target buckets
	if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
		gLog.Warning.Printf("%v", err)
		return
	}

	if reIndex {
		if err := mosesbc.CheckBucketName(srcBucket, indBucket); err != nil {
			gLog.Warning.Printf("%v", err)
			return
		}
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

	if err = mosesbc.SetSourceSproxyd("migrate", srcUrl, driver, env); err != nil {
		gLog.Warning.Printf("Looking for clone.sproxy.source.url .driver and .env in the config file")
		if err = mosesbc.SetSourceSproxyd("clone", srcUrl, driver, env); err != nil {
			return
		}
	}

	maxPartSize = maxPartSize * 1024 * 1024
	// srcS3 = mosesbc.CreateS3Session("backup", "source")
	if srcS3 = mosesbc.CreateS3Session("clone", "source"); srcS3 == nil {
		if srcS3 = mosesbc.CreateS3Session("clone", "source"); srcS3 == nil {
			gLog.Error.Printf("Failed to create a S3 source session")
			return
		}
	}
	// tgtS3 = mosesbc.CreateS3Session("backup", "target")
	if tgtS3 = mosesbc.CreateS3Session("migrate", "target"); tgtS3 == nil {
		gLog.Warning.Printf("Looking for clone.s3.target.url in the config file  ")
		if tgtS3 = mosesbc.CreateS3Session("clone", "target"); tgtS3 == nil {
			gLog.Error.Printf("Failed to create a S3 target session")
			return
		}
	}
	reqm := datatype.Reqm{
		SrcS3:       srcS3,
		SrcBucket:   srcBucket,
		TgtS3:       tgtS3,
		TgtBucket:   tgtBucket,
		Incremental: incr,
	}
	start := time.Now()

	if nextmarker, err = migratePns(marker, reqm); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/*
	func backup_bucket(marker string, srcS3 *s3.S3, srcBucket string, tgtS3 *s3.S3, tgtBucket string) (string, error) {
*/
func migratePns(marker string, reqm datatype.Reqm) (string, error) {
	var (
		nextmarker, token               string
		N                               int
		tdocs, tpages, tsizes, tdeletes int64
		terrors                         int
		mu, mt                          sync.Mutex
		mu1                             sync.Mutex
		incr                            = reqm.Incremental
		req, reql                       datatype.ListObjV2Request
	)
	//   prepare the request to list the source bucket ( document indexes)
	req = datatype.ListObjV2Request{
		Service:           reqm.SrcS3,
		Bucket:            reqm.SrcBucket,
		Prefix:            prefix,
		MaxKey:            int64(maxKey),
		Marker:            marker,
		Continuationtoken: token,
	}
	//  if --input-bucket  then prepare the request to list the last loaded bucket

	if len(iBucket) > 0 {
		reql = datatype.ListObjV2Request{
			Service:           reqm.SrcS3,
			Bucket:            iBucket, //
			Prefix:            prefix,
			MaxKey:            int64(maxKey),
			Marker:            marker,
			Continuationtoken: token,
		}
	}

	for {
		var (
			result                                     *s3.ListObjectsV2Output
			err                                        error
			npages, ndeletes, ndocs, docsizes, nerrors int = 0, 0, 0, 0, 0
			key, method                                string
		)
		N++ // number of loop
		if !incr {
			gLog.Info.Printf("Listing documents from  %s", reqm.SrcBucket)
			result, err = api.ListObjectV2(req)
		} else {
			if len(inFile) > 0 {
				gLog.Info.Printf("Listing documents from file %s", inFile)
				result, err = ListPn(listpn, int(maxKey))
			} else {
				gLog.Info.Printf("Listing documents from bucket  %s", iBucket)
				result, err = api.ListObjectV2(reql)
			}
		}

		/*
			result contains the list of documents to be migrated
		*/
		if err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						gLog.Info.Printf("Key: %s - Size: %d - LastModified: %v", *v.Key, *v.Size, v.LastModified)
						svc := req.Service
						//  if  incremental  migration then the bucket  suffix will depends of the first 2 characters of the prefix
						var buck string
						if incr {
							buck = mosesbc.SetBucketName(*v.Key, req.Bucket)
						} else {
							buck = req.Bucket
						}
						// if --in-file  to retrieve the method  : PUT or DELETE
						if len(inFile) > 0 {
							if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
								gLog.Error.Printf("%v", err)
								continue
							}
						} else {
							//  if no --in-file then full backup : method = PUT
							key = *v.Key
							method = "PUT"
						}
						//  prepare the request to retrieve the S3 metadata of the document
						request := datatype.StatObjRequest{
							Service: svc,
							Bucket:  buck,
							Key:     key,
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, method string) {
							defer wg1.Done()
							gLog.Trace.Printf("Method %s - Key %s ", method, request.Key)
							if method == "PUT" {
								r := migratePn(request, reqm)
								if r.Nerrors > 0 {
									mu.Lock()
									nerrors += r.Nerrors
									mu.Unlock()
								}
								mt.Lock()
								npages += r.Npages
								docsizes += r.Docsizes
								ndocs += r.Ndocs
								mt.Unlock()
							}
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
				gLog.Info.Printf("Number of backed up documents: %d  - Number of pages: %d  - Document size: %d - Number of deletes: %d - Number of errors: %d", ndocs, npages, docsizes, ndeletes, nerrors)
				tdocs += int64(ndocs)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				tdeletes += int64(ndeletes)
				terrors += nerrors
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			gLog.Info.Printf("Total number of backed up documents: %d - total number of pages: %d  - Total document size: %d - Total number of older versions deleted %d -  Total number of errors: %d", tdocs, tpages, tsizes, tdeletes, terrors)
			break
		}
	}
	return nextmarker, nil
}

func migratePn(request datatype.StatObjRequest, reqm datatype.Reqm) datatype.Rm {

	var (
		rh = datatype.Rh{
			Key: request.Key,
		}
		err                        error
		usermd, pn                 string
		npages, ndocs, nerrors, np int
		docsizes                   int64
	)

	if rh.Result, rh.Err = api.StatObject(request); rh.Err == nil {
		//  Extract the number of pages from the user metadata
		if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
			userm := UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
			pn = rh.Key
			if np, err = strconv.Atoi(userm.TotalPages); err == nil {
				start3 := time.Now()
				s3meta := rh.Result.Metadata["Usermd"] //  Metadata map  must contain "usermd" entry encoded 64
				nerr, document := mosesbc.MigrateBlob(reqm, *s3meta, pn, np,ctimeout)
				if nerr == 0 {
					npages = int(document.NumberOfPages)
					docsizes = document.Size
					ndocs = 1
					gLog.Info.Printf("Document id %s is migrated - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))
					start4:= time.Now()
					if reIndex {
						if _, err = mosesbc.IndexDocument(document, indBucket, tgtS3,ctimeout); err != nil {
							gLog.Error.Printf("Error %v while indexing the document id %s iwith the bucket %s", err, document.DocId, indBucket)
							nerrors += 1
						} else {
							gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, indBucket, time.Since(start4))
						}
					}
				} else {
					nerrors = nerr
					ndocs = 0
					gLog.Info.Printf("Document id %s is not fully migrated - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
		}
	} else {
		gLog.Error.Printf("%v", rh.Err)
	}
	r := datatype.Rm{
		nerrors,
		ndocs,
		npages,
		int(docsizes),
	}
	return r
}
