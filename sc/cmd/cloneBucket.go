package cmd

import (
	"bufio"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"strings"

	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sync"
	"time"
)


var (
	clBucket = "Command to clone a bucket"
	cpBucket = "Command to copy a list of objects from a bucket to another bucket "
	clBucketCmd = &cobra.Command{
		Use:   "cloneBucket",
		Short: clBucket,
		Long: ``,
		Run: cloneBucket,
	}
	cpBucketCmd = &cobra.Command{
		Use:   "copyBucket",
		Short: cpBucket,
		Long: ``,
		Run: copyBucket,
	}
	srcBucket,tgtBucket string
	fromDate,inputKeys string
	filter string
	check, overWrite bool
)

func init() {
	RootCmd.AddCommand(clBucketCmd)
	RootCmd.AddCommand(cpBucketCmd)
	RootCmd.MarkFlagRequired("bucket")
	initCbFlags(clBucketCmd)
	initCpFlags(cpBucketCmd)
}

func initCbFlags(cmd *cobra.Command) {
    /*  clone bucket */
	cmd.Flags().StringVarP(&srcBucket,"srcBucket","s","","the name of the source bucket")
	cmd.Flags().StringVarP(&tgtBucket,"tgtBucket","t","","the name of the target bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().StringVarP(&marker,"marker","M","","start key processing from marker")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&fromDate,"fromDate","","2019-01-01T00:00:00Z","clone objects with last modified from <yyyy-mm-ddThh:mm:ss>")
	cmd.Flags().BoolVarP(&check,"check","",true,"check for new objects ,to be used with --fromDate argument")

	// cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
}

func initCpFlags(cmd *cobra.Command) {
	/*  copz bucket */
	cmd.Flags().StringVarP(&srcBucket,"srcBucket","s","","the name of the source bucket")
	cmd.Flags().StringVarP(&tgtBucket,"tgtBucket","t","","the name of the target bucket")
	cmd.Flags().StringVarP(&inputKeys,"input","i","","file of input keys")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&check,"check","",true,"check mode")
	cmd.Flags().BoolVarP(&overWrite,"overWrite","",false,"overwrite existing object")
	// cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
}

func cloneBucket(cmd *cobra.Command,args []string) {

	var (
		start        = utils.LumberPrefix(cmd)
		N= 0
		retryNumber int = 0
		waitTime time.Duration = 0
		total, totalc, size,sizec int64 = 0,0,0,0
		frDate time.Time
		mu      sync.Mutex
   )

	if len(srcBucket) == 0 {
		if  len( viper.GetString("clone.source.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing source bucket")
			utils.Return(start)
			return
		} else {
			srcBucket = viper.GetString("clone.source.bucket")
		}
	}

	if len(tgtBucket) == 0 {
		if  len(viper.GetString("clone.target.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing target bucket")
			utils.Return(start)
			return
		} else {
			tgtBucket = viper.GetString("clone.target.bucket")
		}
	}

	source := datatype.CreateSession{
		Region : viper.GetString("clone.source.region"),
		EndPoint : viper.GetString("clone.source.url"),
		AccessKey : viper.GetString("clone.source.access_key_id"),
		SecretKey : viper.GetString("clone.source.secret_access_key"),
	}

	target := datatype.CreateSession{
		Region : viper.GetString("clone.target.region"),
		EndPoint : viper.GetString("clone.target.url"),
		AccessKey : viper.GetString("clone.target.access_key_id"),
		SecretKey : viper.GetString("clone.target.secret_access_key"),
	}

	if target.EndPoint == source.EndPoint {
		if tgtBucket== srcBucket{
			gLog.Warning.Printf("Reject because source bucket: %s == target bucket: %s",srcBucket,tgtBucket)
			return
		}
	}

	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Wrong date format %s", frDate)
		return
	}


	waitTime = utils.GetWaitTime(*viper.GetViper());
	retryNumber =utils.GetRetryNumber(*viper.GetViper())
	gLog.Info.Printf("Cloning from date %v",frDate)
	gLog.Info.Printf("Source: %v - Target: %v ",source,target)
	gLog.Info.Printf("Retry Options:  Waitime: %v  Retrynumber: %d ",waitTime,retryNumber)
	svc1 := s3.New(api.CreateSession2(source))
	svc3 := s3.New(api.CreateSession2(target))
	/* check source buicket */
	if err:= CheckBucket(svc1,srcBucket); err != nil {
		// gLog.Error.Printf("Error: %v ",err)
		return;
	}
	if err:= CheckBucket(svc3,tgtBucket); err != nil {
		// gLog.Error.Printf("Error: %v ",err)
		return;
	}

	list := datatype.ListObjRequest{
		Service : svc1,
		Bucket: srcBucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	begin:= time.Now()
	for {
		var (
			nextmarker string
			result     *s3.ListObjectsOutput
			err        error
		)
		N++
		if result, err = api.ListObject(list); err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				//wg1.Add(len(result.Contents))
				for _, v := range result.Contents {
					// gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size ,v.LastModified)
					if (v.LastModified.After(frDate) && *v.Key!= nextmarker) && !check {
						svc := list.Service
						wg1.Add(1)
						total++
						size += *v.Size
						get := datatype.GetObjRequest{
							Service: svc,
							Bucket:  list.Bucket,
							Key:     *v.Key,
						}
						objsize:= *v.Size
						go func(request datatype.GetObjRequest,objsize int64) {
							defer wg1.Done()
							for r1 := 0; r1 <= retryNumber; r1++ {
								robj := getObj(request)
								if robj.Err == nil {
									/* check size from list against body size */
									if objsize == int64(robj.Body.Len()) {
										put := datatype.PutObjRequest3{
											Service:  svc3,
											Bucket:   tgtBucket,
											Key:      robj.Key,
											Buffer:   robj.Body,
											Metadata: robj.Metadata,
										}
										gLog.Trace.Printf("Key %s  - Bucket %s - size: %d ", put.Key, put.Bucket, put.Buffer.Len())
										utils.PrintMetadata(put.Metadata)
										for r2 := 0; r2 <= retryNumber; r2++ {
											r, err := api.PutObject3(put)
											if err == nil {
												gLog.Trace.Printf("Retry: %d - Key: %s - Etag: %s- Total cloned: %d",r2, put.Key, *r.ETag,totalc)
												mu.Lock()
												totalc++
												sizec += objsize
												mu.Unlock()
												break /* break r2*/
											} else {
												gLog.Error.Printf("PutObj: %s - Error: %v - Retry: %d", robj.Key, err, r2)
												time.Sleep(waitTime * time.Millisecond)
											}
										}
									} else {
										gLog.Error.Printf("Error reading Object:%s - Size from list:%d  NOT EQUAL body size:%d",robj.Key,objsize,robj.Body.Len())
									}
									break /*  break r1 */
								} else {
									gLog.Error.Printf("GetObj: %s - Error: %v - Retry: %d", robj.Key, err, r1)
									time.Sleep(waitTime * time.Millisecond)
								}
							}
						}(get,objsize)
					} else {
						if check {
							if (v.LastModified.After(frDate)) {
								gLog.Info.Printf("New objet: %s - Last modified date: %v - Size: %d", *v.Key, v.LastModified, *v.Size)
								total++
							}
						}
					}
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					//nextmarker = *result.NextMarker
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				wg1.Wait()
			}
		} else {
			gLog.Error.Printf("ListObj: Error: %v - List argument: %v", err,list)
			gLog.Info.Printf("Exit due to listObj error - Total number of objects cloned so far: %d /size(KB): %.2f",total,float64(size/(1024.0)))
			gLog.Info.Printf("Next marker: %s ",nextmarker)
			return
		}

		if !*result.IsTruncated {
			gLog.Info.Printf("Total number of objects cloned: %d of %d - size %.2f of size(KB): %.2f - Elapsed time: %v",totalc,total,float64(size/(1024.0)),float64(sizec/(1024.0)),time.Since(begin))
			return
		} else {
			list.Marker = nextmarker
		}

		if maxLoop != 0 && N > maxLoop {
			gLog.Info.Printf("Total number of objects cloned: %d of %d - size %.2f of size(KB): %.2f -Elapsed time: %v",totalc,total,float64(size/(1024.0)),float64(sizec/(1024.0)),time.Since(begin))
			return
		}

	}
}




func copyBucket(cmd *cobra.Command,args []string) {

	var (
		start        = utils.LumberPrefix(cmd)
		N= 0
		retryNumber int = 0
		waitTime time.Duration = 0
		total,totalc,sizec  int64 = 0,0,0
		frDate time.Time
		scanner   *bufio.Scanner
		svc1,svc3 *s3.S3
		mu      sync.Mutex
	)
	if scanner, err = utils.Scanner(inputKeys); err != nil {
		gLog.Error.Printf("Error scanning %v file %s",err,inputKeys)
		return
	}

	if len(srcBucket) == 0 {
		if  len( viper.GetString("clone.source.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing source bucket")
			utils.Return(start)
			return
		} else {
			srcBucket = viper.GetString("clone.source.bucket")
		}
	}

	if len(tgtBucket) == 0 {
		if  len(viper.GetString("clone.target.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing target bucket")
			utils.Return(start)
			return
		} else {
			tgtBucket = viper.GetString("clone.target.bucket")
		}
	}

	source := datatype.CreateSession{
		Region : viper.GetString("clone.source.region"),
		EndPoint : viper.GetString("clone.source.url"),
		AccessKey : viper.GetString("clone.source.access_key_id"),
		SecretKey : viper.GetString("clone.source.secret_access_key"),
	}

	target := datatype.CreateSession{
		Region : viper.GetString("clone.target.region"),
		EndPoint : viper.GetString("clone.target.url"),
		AccessKey : viper.GetString("clone.target.access_key_id"),
		SecretKey : viper.GetString("clone.target.secret_access_key"),
	}

	if target.EndPoint == source.EndPoint {
		if tgtBucket== srcBucket{
			gLog.Warning.Printf("Reject because source bucket: %s == target bucket: %s",srcBucket,tgtBucket)
			return
		}
	}

     /* check if source bucket and target bucket  exist   */


	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Wrong date format %s", frDate)
		return
	}

	waitTime = utils.GetWaitTime(*viper.GetViper());
	retryNumber =utils.GetRetryNumber(*viper.GetViper())
	gLog.Info.Printf("Cloning from date %v",frDate)
	gLog.Info.Printf("Source: %v - Target: %v ",source,target)
	gLog.Info.Printf("Retry Options:  Waitime: %v  Retrynumber: %d ",waitTime,retryNumber)

	svc1 = s3.New(api.CreateSession2(source))
	svc3 = s3.New(api.CreateSession2(target))


	if err:= CheckBucket(svc1,srcBucket); err != nil {
		// gLog.Error.Printf("Error: %v ",err)
		return;
	}

	if err:= CheckBucket(svc3,tgtBucket); err != nil {
		// gLog.Error.Printf("Error: %v ",err)
		return;
	}
	begin:= time.Now()
	for {
		N++
		if linea, _ := utils.ScanLines(scanner, int(maxKey)); len(linea) > 0 {
			if l := len(linea); l > 0 {
				start:= time.Now()
				var wg1 sync.WaitGroup
				//wg1.Add(len(result.Contents))
				for _, v := range linea {
					// gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size ,v.LastModified)
					total++
					if !check {
						wg1.Add(1)
						// total++
						get := datatype.GetObjRequest{
							Service: svc1,
							Bucket:  srcBucket,
							Key:     strings.TrimSpace(v),
						}

						go func(request datatype.GetObjRequest) {
							defer wg1.Done()
							for r1 := 0; r1 <= retryNumber; r1++ {

								if result, err := statObj(svc3,request.Key); err == nil {
									if len(*result.ETag) > 0 && !overWrite {
										gLog.Warning.Printf("Object %s already existed in the target Bucket %s",request.Key,tgtBucket)
										break
									}
								}

								robj := getObj(request)
								if robj.Err == nil {
									// write object
									// meta:= robj.Metadata

									put := datatype.PutObjRequest3{
										Service:  svc3,
										Bucket:   tgtBucket,
										Key:      robj.Key,
										Buffer:   robj.Body,
										Metadata: robj.Metadata,
									}
									gLog.Trace.Printf("Key %s  - Bucket %s - size: %d ", put.Key, put.Bucket, put.Buffer.Len())
									// utils.PrintMetadata(put.Metadata) /* debuging */
									for r2 := 0; r2 <= retryNumber; r2++ {
										r, err := api.PutObject3(put)
										if err == nil {
											gLog.Trace.Printf("Object Etag: %v - Version id: %v ", *r.ETag, r.VersionId)
											mu.Lock()
											totalc++
											sizec += int64(robj.Body.Len())
											// gLog.Trace.Println(totalc,sizec)
											mu.Unlock()
											break /* break r2*/
										} else {
											gLog.Error.Printf("PutObj: %s - Error: %v - Retry: %d", robj.Key, err, r2)
											time.Sleep(waitTime * time.Millisecond)
										}
									}
									break /*  break r1 */
								} else {
									notFound:= false
									if aerr, ok := robj.Err.(awserr.Error); ok {
										switch aerr.Code() {
										case s3.ErrCodeNoSuchKey:
											gLog.Warning.Printf("GetObj: %s - Error: [%v] - Retry: %d",robj.Key,aerr.Error(),r1)
											notFound = true
										default:
											gLog.Error.Printf("GetObj: %s - Error: %v - Retry: %d", robj.Key, robj.Err, r1)
											// gLog.Error.Printf("error [%v]",aerr.Error())
										}
									} else {
										// gLog.Error.Printf("[%v]",err.Error())
										gLog.Error.Printf("GetObj: %s - Error: %v - Retry: %d", robj.Key, robj.Err, r1)
									}
									if notFound {
										break
									}
									time.Sleep(waitTime * time.Millisecond)
								}
							}
						}(get)
					} else {
						   gLog.Info.Printf("Copy object key: %s from url/bucket: %s/%s to url/bucket: %s/%s",v,source.EndPoint,srcBucket,target.EndPoint,tgtBucket)
					}
				}
				wg1.Wait()
				gLog.Info.Printf("Elapsed time: %v -Total number of objects copied: %d of %d - size(KB): %.2f",time.Since(start),totalc,total,float64(sizec/(1024.0)))
			}
		} else {
			gLog.Info.Printf("Elapsed time: %v -Total number of objects copied: %d of %d - size(KB): %.2f",time.Since(begin) ,totalc,total,float64(sizec/(1024.0)))
			return
		}

		if maxLoop != 0 && N > maxLoop {
			gLog.Info.Printf("Elapsed time: %v -Total number of objects copied: %d of %d - size(KB): %.2f",time.Since(begin) ,totalc,total,float64(sizec/(1024.0)))
			return
		}
	}
}

func getObj(request datatype.GetObjRequest) (datatype.Robj){

	var (
		// err1 error
		robj = datatype.Robj{
			Key : request.Key,
		}
	)
	gLog.Trace.Printf("Geting Object %s",request.Key)
	if result,err := api.GetObject(request); err == nil {
		b, err := utils.ReadObject(result.Body)
		if err == nil {
			robj.Body= b
			robj.Metadata =result.Metadata
		}
		//err1= errors.New(fmt.Sprintf("Error %v reading object %s",err,request.Key))
		robj.Err = err
		// gLog.Error.Println(err)
	} else {
		// err1 = errors.New(fmt.Sprintf("Error %v getting object %s",err,request.Key))
		robj.Err = err
		// gLog.Error.Println(err)
	}
	return robj
}

func statObj(svc *s3.S3, key string) (*s3.HeadObjectOutput,error) {
	gLog.Trace.Printf("checking object %s",key)
	stat := datatype.StatObjRequest{
		Service: svc,
		Bucket: tgtBucket,
		Key: key,
	}
	return api.StatObject(stat)
}