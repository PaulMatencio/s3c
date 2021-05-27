package st33

import (
	//"github.com/aws/aws-sdk-go/aws/session"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/utils"
	// "path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	// "encoding/json"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"strings"
	"time"
)

type PutS3Response struct {

	Bucket  string
	Key     string
	Size    int
	Error   S3Error
}

//
//
//
//   St33 to S3    version 1
//   Input : ToS3Request [ Infile, bucket, log bucket, .....]
//   Action :  Load control file and  get data file record  sequentially ( st33Reader)
//             For every entry of the control file, upload corresponding data to S3
//   Result > Number of documents and pages  uploaded, number of errors and a list of errors
//

/*
	Process 1 document at a time
    every page of the document is upload serially
 */
func ToS3V1(req *ToS3Request)  (int, int, int,int, []S3Error) {

	var (
		infile   = req.File
		bucket   = req.Bucket
		bucketNumber = req.Number
		sbucket  = req.LogBucket
		reload   = req.Reload
		check    = req.Check
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numrecs,numdocs,S	int		=  0,0,0,0
	)

	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				log.Println("Systenm memory:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation", float64(m.HeapAlloc)/1024/1024)
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,DATval,CONval,1)

	if !utils.Exist(confile) {

		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,0,ErrKey
		}
	}

	conVal := *conval
	svc :=  s3.New(api.CreateSession())

	d,key := filepath.Split(infile)
	p1 := strings.Split(d,"/")
	p := p1[len(p1)-2]
	key = p + "." + key

	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}
		//
		// check if datafile is already fully loaded
		//  if req.Reload  then skip checking
		//  otherwsise check if datafile is already fully loaded
		//

		if !check && !reload && !checkDoLoad(getReq,infile) {
			return 0,0,0,0,ErrKey
		}
	}

	start0:= time.Now()
	putReq  := datatype.PutObjRequest {
		Service: svc,
		// Bucket: req.Bucket,
	}

	/* read ST33  file */
	gLog.Info.Printf("Processing input file %s - control file %s",infile,confile)
	r,err  := NewSt33Reader(infile)
	if err == nil {
		var (
			Numdocs 	int = len(conVal)
		)
		gLog.Info.Printf("Number of documents to upload %d",Numdocs)
		// loop tru the control file
		for e, v := range conVal {
			var (
				recs   int =0
				pages  int = 0
				lp = len(v.PxiId)
				KEY = v.PxiId
			)

			gLog.Info.Printf("Document#: %d - Uploading Key: %s - Number of pages %d / Number of records %d",e, utils.Reverse(v.PxiId),v.Pages,v.Records)
			/* Delete previous stack and  create  a  new stack  to keep track of all record 's address of this document */
			utils.DelStack(r.Stack)
			r.Stack =  utils.NewStack(v.Records)
			// if the last - 1 byte is "P"  then it is  an ST33 TIFF image
			if v.PxiId[lp-2:lp-1] == "P" {
				KEY = utils.Reverse(KEY)
				s := 0
				P := int(v.Pages)
				for p := 0; p < P; p++ {
					// set the key of the s3 object
					cp:= p+1
					putReq.Key = KEY
					putReq.Bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(v.PxiId,bucketNumber))

					putReq.Usermd = map[string]string{} // reset the  user metadata

					if image,nrec, err, err1 := GetPage(r, v,cp); err == nil  && err1 == nil  {

						pages++  // increment the number of pages for this PXI ID
						recs += nrec // increment the number of records for this PXI ID

						if image.Img != nil {
							s += image.Img.Len()
							// update the docsize with the actual image size
							v.DocSize = uint32(image.Img.Len())
							S += int(v.DocSize)
							// build the user metadata for the first page only
							pagenum, _ := strconv.Atoi(string(image.PageNum))

							if pagenum == 1 {
								if usermd, err := BuildUsermd(v); err == nil {
									putReq.Usermd = utils.AddMoreUserMeta(usermd, infile)
								}
							}

							// complete the request of writing to S3

							// putReq.Key = putReq.Key + "." + strconv.Itoa(pagenum)
							if p+1 != pagenum {
								gLog.Warning.Printf("PxiId:%s - Inconsistent page number in st33 header: %d/%d ",v.PxiId,pagenum,p+1)
							}
							// putReq.Key = putReq.Key + "." + strconv.Itoa(p+1)
							putReq.Key = putReq.Key + "." + strconv.Itoa(pagenum)
							putReq.Buffer = image.Img
							gLog.Trace.Printf("Put Key:%s - Bucket: %s ",putReq.Key,putReq.Bucket)


							err = nil
							if !check {
								_, err = writeToS3(putReq)
							}
							if err != nil {
								error := errors.New(fmt.Sprintf("PutObject Key: %s  Error: %v", putReq.Key, err))
								gLog.Error.Printf("%v", error)
								ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
							}

							// reset the image
							image.Img.Reset()
						}
					} else {
						// should never happen unless input data is corrupted
						// gLog.Fatal.Printf("%v",err)
						//  os.Exit(100)
						if err != io.EOF || err1 != nil {
							gLog.Error.Printf("PXIID: %s -  Err: %v  - Err1: %v ",v.PxiId,err,err1)
							inputError = append(inputError, S3Error{Key: v.PxiId, Err: err1})
							break
						}
					}
				}

				numpages += int(v.Pages)
				numrecs += recs

				/*  check records number */

				//  Total number of records of the control file should match the total number of records for this PXIID

				if v.Records != recs {
					gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file ",v.PxiId,v.Records,recs)
					diff := v.Records - recs   // SKIP AND discard extra records
					if diff < 0 {
						RewindST33(v,r,diff)
						recs -= diff
					} else {
						SkipST33(v,r,diff)
						recs += diff
					}
				}


			} else if v.PxiId[lp-2:lp-1] == "B" {  // it is a blob
				pxiblob := NewPxiBlob(KEY, v.Records)
				if nrec, err := pxiblob.BuildPxiBlob(r, v); err == nil {
					if nrec != v.Records {
						// Check number of BLOB record
						error := errors.New(fmt.Sprintf("Key %s - Control file records %d != Blob records %d", v.PxiId, v.Records, nrec))
						gLog.Error.Printf("%v", error)
						ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})

					}
					v.DocSize = uint32(pxiblob.Blob.Len())
					S += int(v.DocSize)
					if usermd, err := BuildUsermd(v); err == nil {
						putReq.Usermd = utils.AddMoreUserMeta(usermd, infile)
					}

					putReq.Buffer = pxiblob.Blob
					putReq.Bucket = bucket+"-"+fmt.Sprintf("%02d",utils.HashKey(v.PxiId,bucketNumber))
					putReq.Key = pxiblob.Key
					gLog.Trace.Printf("Put Key:%s - Bucket: %s ",putReq.Key,putReq.Bucket)

					err = nil
					if !check {
						_, err = writeToS3(putReq)
					}
					if err != nil {
						error := errors.New(fmt.Sprintf("PutObject Key: %s  Error: %v", putReq.Key, err))
						gLog.Error.Printf("%v", error)
						ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
					}

					pxiblob.Blob.Reset()
					numpages++
					numrecs += recs
				} else {
					gLog.Warning.Printf("Control file %s and data file %s do not map for key %s", confile, infile, v.PxiId)
				}

			} else {
				error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s",e,v.PxiId))
				gLog.Error.Printf("%v",error)
				ErrKey= append(ErrKey,S3Error{Key:v.PxiId,Err:error})
			}
			numdocs++
		}
		duration := time.Since(start0)
		status := PartiallyUploaded
		numerrupl := len(ErrKey)      // number of upload with errors
		numerrinp := len(inputError)  // input data error
		if numerrupl == 0   {
			if numerrinp == 0 {
				status = FullyUploaded
			}  else {
				status =  FullyUploaded2
			}
		}

		//  For loging purpose

		resp := ToS3Response {
			Time: time.Now(),
			Duration: fmt.Sprintf("%s",duration),
			Status : status,
			Docs  : numdocs,
			Pages : numpages,
			Size  : S,
			Erroru : numerrupl,
			Errori : numerrinp,
		}

		for _,v := range inputError {             // append input data consistency to ErrKey array
			ErrKey = append(ErrKey,v)
		}
		if !check {
			if _, err = logIt(svc, req, &resp, &ErrKey); err != nil {
				gLog.Warning.Printf("Error logging request to %s : %v", req.LogBucket, err)
			}
		}
		gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))
		return numpages,numrecs,numdocs,S,ErrKey
	}
	gLog.Info.Printf("Number of uploaded documents %d - Number of uploaded pages %d",numdocs,numpages)
	return numpages,numrecs, numdocs,S,ErrKey
}


/*
	Process one document at a time
	N pages of the document  will be uploaded concurrently
 */
func ToS3V1Parallel(req *ToS3Request)  (int, int, int,int, []S3Error) {

	var (
		infile                               = req.File
		bucket                               = req.Bucket
		// bucketNumber                         = req.Number
		sbucket                              = req.LogBucket
		reload                               = req.Reload
		check                                = req.Check
		confile                              string
		conval                               *[]Conval
		err                                  error
		ErrKey, inputError                   []S3Error
		numdocs int = 0
	)
	svc := s3.New(api.CreateSession())
	conval = &[]Conval{}
	confile = strings.Replace(infile, req.DatafilePrefix, req.CrlfilePrefix, 1)
	if !utils.Exist(confile) {
		ErrKey = append(ErrKey, S3Error{
			"",
			errors.New(fmt.Sprintf("Corrresponding datval file %s does not exist for input file %s ", confile, infile)),
		})
		return 0, 0, 0, 0, ErrKey
	} else {
		if conval, err = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey, S3Error{
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ", confile, infile)),
			})
			return 0, 0, 0, 0, ErrKey
		}
	}
	conVal := *conval
	// check the existence of the migration log  Bucket
	// and if  data file was already uploaded
	d, key := filepath.Split(infile)
	p1 := strings.Split(d, "/")
	p := p1[len(p1)-2]
	key = p + "." + key
	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key:     key,
		}
		if !check && !reload && !checkDoLoad(getReq, infile) {
			return 0, 0, 0, 0, ErrKey
		}
	}

	// read ST33  input file
	gLog.Info.Printf("Reading file ... %s", infile)
	r, err := NewSt33Reader(infile)

	if err == nil {
		var (
			Numdocs = len(conVal)
			step    = req.Async
		)
		gLog.Info.Printf("Uploading %d documents to bucket %s ...", Numdocs, bucket)
		// loop until all the requests are completed
		for {
			for e, v := range conVal { // loop tru teh control file
				var (
					KEY = v.PxiId
					lp = len(KEY)
					recs = 0                  // init the number of records for the current document
					pages = 0
					Req  = ToS3GetPages{
						KEY :KEY, Step: step,Req: req, R: r, V:v, Svc: svc,
					}
					numpages, numrecs int
					iError  []S3Error
				)

				if KEY[lp-2:lp-1] == "P" {

					P := int(v.Pages)
					Quot := P / step
					Rest := P % step
					cp := 1	  /*current page */
					gLog.Info.Printf("Uploading docment: %s - number of pages: %d - number of records : %d",KEY,P,v.Records)
					for q:=1; q<= Quot; q++ {
						Req.CP =cp
						numpages,numrecs,iError = GetPages(Req)
						pages += numpages; recs  += numrecs
						for _,e :=  range iError{
							inputError = append(inputError, e)
						}
						cp += step
					}

					Req.CP = cp
					Req.Step = Rest
					numpages,numrecs,iError = GetPages(Req)
					pages += numpages; recs  += numrecs
					for _,e :=  range iError{
						inputError = append(inputError, e)
					}

					numdocs++ // increment the number of processed documents

					if v.Records != recs {
						gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file %s", v.PxiId, v.Records, recs, req.File)
						diff := v.Records - recs
						if diff < 0 {
							RewindST33(v, r, diff)
							recs -= diff

						} else {
							SkipST33(v, r, diff)
							recs += diff

						}
					}
				} else {
					error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s", e, v.PxiId))
					gLog.Error.Printf("%v", error)
					ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
				}
			}

			return 0, 0, 0, 0, ErrKey

		}
	}
	return 0, 0, 0, 0, ErrKey
}


//
// Same as ToS3V1 but concurrent upload data to S3
//


func ToS3V1Async(req *ToS3Request)  (int, int, int,int, []S3Error)  {

	var (
		infile   = req.File
		bucket   = req.Bucket
		bucketNumber = req.Number
		sbucket  = req.LogBucket
		reload   = req.Reload
		check    = req.Check
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numrecs,numdocs,E,S,S1	int		=  0,0,0,0,0,0
	)
	//  monitor storage and free storage if necessary
	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				heap := float64(m.HeapAlloc)/1024/1024
				log.Println("System memory MB:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation MB", heap)
				debug.FreeOSMemory()
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/*
		Create a  S3 session
	*/
	svc :=  s3.New(api.CreateSession())

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,req.DatafilePrefix,req.CrlfilePrefix,1)

	if !utils.Exist(confile) {
		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding datval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,0,ErrKey
		}
	}

	conVal := *conval
	// check the existence of the migration log  Bucket
	// and if  data file was already uploaded
	d,key := filepath.Split(infile)
	p1 := strings.Split(d,"/")
	p := p1[len(p1)-2]
	key = p + "." + key

	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}

		//
		// check if datafile is already  successfully loaded
		//  -r  or --reload will bypass the verification
		//

		if !check && !reload && !checkDoLoad(getReq,infile)  {
			return 0,0,0,0,ErrKey
		}
	}

	// read ST33  input file
	gLog.Info.Printf("Reading file ... %s",infile)
	r,err := NewSt33Reader(infile)
	// Make communication channel for go routine
	ch := make(chan *PutS3Response)
	start0:= time.Now()

	if err == nil {
		var (
			Numdocs       = len(conVal)
			p             = 0
			step          = req.Async
			stop          = false
		)

		gLog.Info.Printf("Uploading %d documents to bucket %s ...", Numdocs,bucket)
		// Set the break of the main loop
		if step > Numdocs {
			step = Numdocs
			stop = true
		}
		q:= step

		// loop until all the requests are completed
		for {
			var (
				N int = 0  // number of concurrent pages to be processed
				T int = 0  // number of processed pages
				start1 = time.Now()
			)
			// for all documents listed in the control file
			for e, v := range conVal[p:q] {   // loop tru teh control file
				KEY := v.PxiId;
				//	if KEY != "E1_______0011444808P1" && KEY != "E1_______001156770LP1"  && v.PxiId != "E1_______0031079902P1" { // lot p00001.00155, p00001.00164 p00002.00171
				lp := len(KEY);
				recs := 0  // init the number of records for the current document
				pages := 0 // init the number of pages for current document
				if KEY[lp-2:lp-1] == "P" {  // Is -it a ST33 document ?
					P := int(v.Pages)   // number of pages of the current  document
					N += P              // total number of pages that are going to be processed
					// Upload all the pages of the current document
					for p := 0; p < P; p++ {
						cp:= p+1
						image, nrec, err, err1 := GetPage(r, v, cp)   // Get the next page
						if err1 != nil { // Broken page, should exit
							inputError = append(inputError, S3Error{Key: v.PxiId, Err: err1})
						}

						if err == nil  && err1 == nil {
							recs += nrec // increment the number of records for this page
							pages++      //  increment the number of pages without error.
							//  increment the number of records for this lot

							if image.Img != nil {
								numpages++ //  increment the number of pages for this lot
								numrecs += nrec
								v.DocSize = uint32(image.Img.Len()) // update the document size (replace the oroginal value)

								go func(key string, p int, image *PxiImg, v Conval) {
									// append the bucket number to the given bucket
									req := datatype.PutObjRequest{ // build a PUT OBject request
										Service: svc,
										Bucket:  bucket + "-" + fmt.Sprintf("%02d", utils.HashKey(v.PxiId, bucketNumber)),
									}
									// Add user metadata to page 1
									pagenum, _ := strconv.Atoi(string(image.PageNum)) // Build user metadata for page 1
									if pagenum == 1 {
										if usermd, err := BuildUsermd(v); err == nil {
											req.Usermd = utils.AddMoreUserMeta(usermd, infile)
										}
									}

									// S3 key is the reverse of the pxi id + '.' + page number
									//  PUT OBJECT

									if p+1 != pagenum {

										error := errors.New(fmt.Sprintf("PxiId:%s - Inconsistent page number in st33 header: %d/%d ", v.PxiId, pagenum, p+1))
										gLog.Warning.Printf("%v", error)
										// inputError = append(inputError, S3Error{Key: v.PxiId, Err: error})

									}
									req.Key = utils.Reverse(key) + "." + strconv.Itoa(pagenum) // add key to request
									//req.Key = utils.Reverse(key) + "." + strconv.Itoa(p+1)
									req.Buffer = image.Img //   add the image
									err = nil
									if !check {
										_, err = writeToS3(req) //  upload the image  to S3
									}
									//  Prepare a response Block
									s3Error := S3Error{Key: req.Key, Err: err}
									image.Img.Reset() // reset the image buffer
									ch <- &PutS3Response{req.Bucket, req.Key, int(v.DocSize), s3Error}

								}(KEY, p, image, v)
							} else {
								N--
							}
						} else {
							// should not happen unless input data is corrupted
							if err == io.EOF {
								N--
							}
							if err != io.EOF  ||   err1 != nil {
								gLog.Error.Printf("Err: %v Err1: %v  building image for Key:%s - Prev/Curr buffer address: X'%x'/ X'%x' ", err,err1, v.PxiId, r.Previous, r.Current)
								gLog.Error.Printf("Exit while processing key: %s  at entry number: %d  of input file: %s",v.PxiId,e, infile)
								os.Exit(100)
							}
						}
					}
					numdocs++ // increment the number of processed documents

					/* Check the total number of records of  this document, they must match */
					if v.Records != recs {
						gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file %s",v.PxiId,v.Records,recs,req.File)
						diff := v.Records - recs
						if diff < 0 {
							RewindST33(v,r,diff)
							recs -=diff

						} else {
							SkipST33(v,r,diff)
							recs +=diff

						}
					}


				} else if KEY[lp-2:lp-1] == "B" { // regular BLOB

					pxiblob := NewPxiBlob(v.PxiId, v.Records) // it is BLOB
					if nrec, err := pxiblob.BuildPxiBlob(r, v); err == nil { //  Build the blob
						if nrec != v.Records { // Check number of BLOB record
							error := errors.New(fmt.Sprintf("Pxiid %s - Control file records %d != Blob records %d", v.PxiId, v.Records, nrec))
							gLog.Warning.Printf("%v", error)
							inputError = append(inputError, S3Error{Key: pxiblob.Key, Err: error})
						}
						N++                                    // Increment the number of requests
						numpages++                             //  increment total number of pages
						numrecs += nrec                        // increment total number of records
						numdocs++                              //  increment total number of documents
						v.DocSize = uint32(pxiblob.Blob.Len()) // Update the original size
						go func(key string, pxiblob pxiBlob, v Conval) {
							req := datatype.PutObjRequest{
								Service: svc,
								Bucket:  bucket+"-"+fmt.Sprintf("%02d",utils.HashKey(v.PxiId,bucketNumber)),
								Key:     pxiblob.Key,
								Buffer:  pxiblob.Blob,
							}

							if usermd, err := BuildUsermd(v); err == nil { // Add  user metadata if build ok
								req.Usermd = utils.AddMoreUserMeta(usermd, infile)
							}
							// build a put object request
							// then Write it to S3 if not in test mode

							if !check {
								_, err = writeToS3(req)
							}
							s3Error := S3Error{Key: pxiblob.Key, Err: err}
							//Reset the Blob Content
							pxiblob.Blob.Reset()
							// Send a message to go routine listener
							ch <- &PutS3Response{req.Bucket, req.Key, int(v.DocSize), s3Error}

						}(KEY, *pxiblob, v)
					} else {
						gLog.Error.Printf("Error %v", err)
						inputError = append(inputError, S3Error{Key: v.PxiId, Err: err})
					}
				} else {
					error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s", e, v.PxiId))
					gLog.Error.Printf("%v", error)
					ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
				}
			}

			/* wait  for goroutine signal*/

			done:= false
			S1= 0
			for ok:=true;ok;ok=!done {
				select {
				case r := <-ch:
					{
						T++           // increment the number of processed requests
						S1 += r.Size  // increment  the size of this upload
						S  += r.Size  // increment the total size
						gLog.Trace.Printf("Upload object Key:%s - Bucket:%s - Completed:%d/%d  - Object size: %d  - Total uploaded size:%d", r.Key, r.Bucket, T,N, r.Size,S1)
						if r.Error.Err != nil {
							E++
							ErrKey = append(ErrKey, r.Error)
						}

						if T == N  {   // All done
							elapsedtm := time.Since(start1)
							avgtime := float64(elapsedtm) / (float64(N) * 1000 *1000)
							gLog.Trace.Printf("%d objects were uploaded to bucket: %s - %.2f MB/sec\n", N, bucket, float64(S1)*1000/float64(elapsedtm) )
							gLog.Trace.Printf("Average object size: %d KB - avg upload time/object: %.2f ms\n", S1/(N*1024), avgtime)
							if len(ErrKey) > 0 {
								gLog.Error.Printf("\nFail to load following objects:\n")
								for _, er := range ErrKey {
									gLog.Error.Printf("Key: %s - Error: %v", er.Key, er.Err)
								}
							}
							done = true
						}
					}
				case <-time.After(200 * time.Millisecond):
					fmt.Printf("w")
				}
			}

			if stop {
				break
			}

			// log status  to a bucket
			if q == Numdocs {

				stop = true
				duration := time.Since(start0)
				status := PartiallyUploaded
				numerrupl := len(ErrKey)    // number of upload with errors
				numerrinp := len(inputError)  // input data error
				if numerrupl == 0   {         // no uploading error
					if numerrinp == 0 {       // no input data error
						status = FullyUploaded
					}  else {
						status =  FullyUploaded2
					}
				}

				resp := ToS3Response {
					Time: time.Now(),
					Duration: fmt.Sprintf("%s",duration),
					Status : status,
					Docs  : numdocs,
					Pages : numpages,
					Size  : S,
					Erroru : numerrupl,
					Errori : numerrinp,

				}
				// append input data consistency to ErrKey array
				for _,v := range inputError {
					ErrKey = append(ErrKey,v)
				}
				if !check {
					if _, err = logIt(svc, req, &resp, &ErrKey); err != nil {
						gLog.Warning.Printf("Error logging request to %s : %v", req.LogBucket, err)
					}
				}
				gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))

				return numpages,numrecs,numdocs,S,ErrKey
			}
			p += step
			q += step
			if q >= Numdocs {
				q = Numdocs
			}
		}
	}

	// Error reading data file
	ErrKey = append(ErrKey,S3Error {
		"",
		errors.New(fmt.Sprintf("Error reading data file %s %v .... ",req.File,err)),
	})
	// return without logging
	return 0,0,0,0,ErrKey

}


func GetPages(Req ToS3GetPages) (int,int,[]S3Error){
	var (
		wg sync.WaitGroup
		recs,pages int
		bucket = Req.Req.Bucket
		bucketNumber = Req.Req.Number
		infile = Req.Req.File
		check = Req.Req.Check
		KEY = Req.KEY
		cp   = Req.CP
		step = Req.Step
		r = Req.R
		v = Req.V
		svc = Req.Svc
		inputError []S3Error
	)
	gLog.Trace.Printf("current page: %d current step: %d",cp,step)
	for p := cp; p < cp+step; p++ {
		image, nrec, err, err1 := GetPage(r, v, p )// Get the next page
		if err1 != nil {                            // Broken page, should exit
			inputError = append(inputError, S3Error{Key: v.PxiId, Err: err1})
		}
		if err == nil && err1 == nil {
			if image.Img != nil {
				wg.Add(1)
				pages++ //  increment the number of pages for this lot
				recs += nrec
				v.DocSize = uint32(image.Img.Len()) // update the document size (replace the oroginal value)
				go func(key string, p int, image *PxiImg, v Conval) {
					defer wg.Done()
					req := datatype.PutObjRequest{ // build a PUT OBject request
						Service: svc,
						Bucket:  bucket + "-" + fmt.Sprintf("%02d", utils.HashKey(v.PxiId, bucketNumber)),
					}
					pagenum, _ := strconv.Atoi(string(image.PageNum)) // Build user metadata for page 1
					if pagenum == 1 {
						if usermd, err := BuildUsermd(v); err == nil {
							req.Usermd = utils.AddMoreUserMeta(usermd, infile)
						}
					}
					if p != pagenum {
						error := errors.New(fmt.Sprintf("PxiId:%s - Inconsistent page number in st33 header: %d/%d ", v.PxiId, pagenum, p))
						gLog.Warning.Printf("%v", error)
					}
					req.Key = utils.Reverse(key) + "." + strconv.Itoa(pagenum) // add key to request
					req.Buffer = image.Img //   add the image
					err = nil
					if !check {

						if _, err = writeToS3(req); err != nil {
							gLog.Error.Printf("Error %v - Writing req.Key  %s to bucket %s",err,req.Key,req.Bucket)
						}

					}
					image.Img.Reset() // reset the image buffer
				}(KEY, p, image, v)
			} else {
				err:= errors.New(fmt.Sprintf("No image of PxiId: %s",v.PxiId))
				inputError = append(inputError, S3Error{Key: v.PxiId, Err: err})
			}
		} else {
			// critical error => just exit
			if err != io.EOF || err1 != nil {
				gLog.Error.Printf("Err: %v Err1: %v  building image for Key:%s - Prev/Curr buffer address: X'%x'/ X'%x' ", err, err1, v.PxiId, r.Previous, r.Current)
				gLog.Error.Printf("Exit while processing key: %s - input file: %s", v.PxiId,  infile)
				os.Exit(100)
			}
		}
	}
	//  Wait for all go routine to be completed
	wg.Wait()
	return pages,recs,inputError
}
