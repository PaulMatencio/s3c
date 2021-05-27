
package st33

import (
	// "bytes"
	"errors"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"path/filepath"
	"strconv"
	"strings"
)



func ToFilesV1(ifile string,  odir string, bdir string, test bool)  (int,int,int, int,error){

	var (

		confile   		string
		conval			*[]Conval
		err 			error
		numdocs	        int=0
		numpages		int=0
		numrecs         int=0
		numerrors       int=0
	)

	/* read  the control  file  */

	conval = &[]Conval{}
	confile = strings.Replace(ifile,DATval,CONval,1)
	if !utils.Exist(confile) {
		return 0,0,0,0,errors.New(fmt.Sprintf("Corrresponding control file %s does not exist for input data file %s ",confile,ifile))
	} else {
		if conval,err  = BuildConvalArray(confile); err != nil {
			return 0,0,0,0,errors.New(fmt.Sprintf("Error %v  reading %s ",err,confile))
		}
	}
	conVal := *conval

	/* read ST33  file */
	gLog.Info.Printf("Processing input file %s - control file %s",ifile,confile)
	r,err  := NewSt33Reader(ifile)


	if err == nil {

		for _, v := range conVal {

			var (
				recs int = 0
				pages int = 0
				pathname  string
				lp = len(v.PxiId)
				KEY = v.PxiId;
			)

			if v.PxiId[lp-2:lp-1] == "P" {  // TIFF IMAGE

				gLog.Trace.Printf("Processing ST33 Key %s Number of Pages/Records: %d/%d",v.PxiId,v.Pages,v.Records)
				s:= 0
				KEY = utils.Reverse(KEY)
				P := int(v.Pages)    // Total number of pages for this document
				for p:= 0; p < P; p++ {
					cp:= p+1
					if image, nrec , err, err1 := GetPage(r,v,cp); err == nil && err1==nil   {


						pages++       // increment the number of pages of the PXIID
						recs += nrec  // increment the number of records of the PXIID

						if image.Img!= nil {
							s += image.Img.Len()
							gLog.Trace.Printf("==> PixId:%s - ST33 Key:%s - # Pages:%d - PxiId from image:%s  - Page number  from image/iteration:%s/%d - ImageLength:%d", v.PxiId, KEY, v.Pages, string(image.PxiId), string(image.PageNum), p+1,image.Img.Len())
							pagenum, _ := strconv.Atoi(string(image.PageNum))
							 	if !test {
							 		if pagenum != p + 1 {
										gLog.Warning.Printf("PxiId:%s - Inconsistent page number in st33 header: %d/%d ",v.PxiId,pagenum,p+1)
									}

									outdir := filepath.Join(odir,KEY)
									utils.MakeDir(outdir)

									//pathname = filepath.Join(outdir, KEY+"."+strconv.Itoa(p+1))
									pathname = filepath.Join(outdir, KEY+"."+strconv.Itoa(pagenum))
									if err := utils.WriteFile(pathname, image.Img.Bytes(), 0644); err != nil {
										gLog.Error.Printf("Error %v writing image %s ", err, pathname)
									}
								}

								/* if pagenum == 1 { */
								if p == 0 {
									if usermd, err := BuildUsermd(v); err == nil {
										utils.AddMoreUserMeta(usermd, ifile)
										pathname += ".md"
										if !test {
											if err = WriteUsermd(usermd, pathname); err != nil {
												gLog.Error.Printf("Error writing user metadata %v", err)
											}
										}
									} else {
										gLog.Error.Printf("Error building user metadata %v", err)
									}
								}

							image.Img.Reset() /* reset the previous image buffer */
						}

					} else {
							// gLog.Error.Printf("Err: %v - Err1: %v - getting  page number %d ", err,err1, p+1)
							gLog.Error.Printf("PXIID: %s -  Err: %v  - Err1: %v ",v.PxiId,err,err1)
							numerrors++
							if err1 != nil {
								return numpages,numrecs,numdocs,numerrors, err1
							}
						}
				}

				numpages += P
				numrecs  += recs
				numdocs++

				//  Total number of records of the control file should match the total number of records for this PXIID

				if v.Records != recs {
					gLog.Warning.Printf("PXIID %s - Total number of records in the control file (%d)  != Total number of records (%d) in the data file ",v.PxiId,v.Records,recs)
					diff := v.Records - recs
					if diff < 0 {
						RewindST33(v,r,diff)
						recs -= diff
					} else {
						SkipST33(v,r,diff)
						recs += diff
					}
				}

				gLog.Trace.Printf("PXIID: %s - Key %s - Number of records: %d/%d - #pages: %d/%d - Total number of pages: %d ",v.PxiId,utils.Reverse(v.PxiId),v.Records,recs,v.Pages,pages,numpages)

			} else if v.PxiId[lp-2:lp-1] == "B"  {            // Regular BLOB
                pxiblob := NewPxiBlob(KEY,v.Records)
				if nrec,err  := pxiblob.BuildPxiBlob(r,v); err == nil {
					if nrec != v.Records {                                // Check number of BLOB record
						gLog.Warning.Printf("Key %s - Control file records %d != Blob records %d",v.PxiId,v.Records,nrec)
					}
					// Write to Files
					if !test {
						pathname = filepath.Join(bdir, pxiblob.Key)   // Only one page
						err = WriteImgToFile(pathname, pxiblob.Blob); // Save the image to file
						if err != nil {
							gLog.Error.Printf("%v", err)
						}
					}
					if usermd, err := BuildUsermd(v); err == nil { // build
						utils.AddMoreUserMeta(usermd, ifile) // and save user metadata
						pathname += ".md"
						if !test {
							if err = WriteUsermd(usermd, pathname); err != nil {
								gLog.Error.Printf("Error writing user metadata %v", err)
							}
						}
					} else {
						gLog.Error.Printf("Error building user metadata %v", err)
					}
					pxiblob.Blob.Reset()  /* reset the previous blob buffer */
					numpages++            // increment number of processed pages
					numrecs +=nrec        //  increment the number of processed records
				} else {
					numerrors++          // increment the number of errors
					gLog.Error.Printf("Error %v",err)
				}
				numdocs++   // increment number of processed docs
			} else {
				gLog.Warning.Printf("Oop! Control file contains a wrong Key:%s",v.PxiId)
			}
		}
	} else {
		gLog.Error.Printf("%v",err)
	}

	return numpages,numrecs,numdocs,numerrors, err
}
