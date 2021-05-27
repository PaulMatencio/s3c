package st33

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"strconv"
)

type PxiImg struct {
	PxiId   	[]byte
	PageNum 	[]byte
	RefNum  	[]byte
	Img     	*bytes.Buffer
	DataType 	[]byte
	NumPages 	[]byte
}

//  return the address of aPxiImg structure
func NewPxiImg() (*PxiImg) {

	return &PxiImg{}
}


//  Return a TIFF image
func (image *PxiImg) GetImage() *bytes.Buffer {
	return image.Img
}

func (image *PxiImg) GetPageNum() []byte {
	return image.PageNum
}

func (image *PxiImg) GetPxiId() []byte{
	return image.PxiId
}

func (image *PxiImg) SetPxiId(p []byte) {
	image.PxiId = p
}

func (image *PxiImg) GetRefNum() []byte {
	return image.RefNum
}

func (image *PxiImg) GetDataType() []byte {
	return image.DataType
}

func (image *PxiImg) BuildTiffImage(r *St33Reader, v Conval,cp int) (int,error,error) {

	var (
		totalRec       uint16
		totalLength    uint32
		recs           uint16
		imgl           uint16
		err, err1   		   error
		nrec           int = 0
		st33,buf       []byte
		id             string

	)
	gLog.Trace.Printf("PxiId: %s - Building image - Page number %d  of %d pages",v.PxiId,cp,v.Pages)
	Little	:= binary.LittleEndian
	Big		:= binary.BigEndian
	enc 	:= Big

	buf,err = r.Read()   // Read  the First record
	if err != nil {
		return nrec,err,err1
	}
	nrec++

	/* check if record  length = length  of the ST33 record   */
	err1 = CheckST33Length(&v,r,buf)

	if err1 != nil {
		gLog.Error.Println(err)
		gLog.Info.Printf("%s", hex.Dump(buf[0:255]))
		return nrec,err,err1
	}

	if len(buf) <= 256 {
		if buf,err = r.Read(); err != nil {
			return nrec,err,err1
		} else {
			nrec++
		}
	}
	st33 = utils.Ebc2asci(buf[0: 214])
	id = string(st33[9:17]) + string(st33[7:9])
	//k:= strings.Replace(v.PxiId,"_","",-1)[2:]
	l := len(v.PxiId)
	k := v.PxiId[9:l-2]
	if ( k != id) {
		// RewindST33(v,r,1)
		gLog.Warning.Printf("Reading a wrong record  which does not belong to the current page %d - PxiID: %s - IageId: %s  k: %s",cp,v.PxiId,id,k)
	}

	err 		= 	binary.Read(bytes.NewReader(buf[25 : 27]), Big, &recs)
	err 		= 	binary.Read(bytes.NewReader(buf[84 : 86]), Big, &totalRec)
	err 		= 	binary.Read(bytes.NewReader(buf[214 : 218]), Big, &totalLength)
	err			= 	binary.Read(bytes.NewReader(buf[250 : 252]), Big, &imgl)

	/*
		convert St33 encoded Big Endian input data ( EBCDIC) to  Little Endian (ASCII)
	*/

	// long, _ := 	strconv.Atoi(string(st33[0:5]))
	// image.PxiId		= st33[5:17]   //  PXI ID
	image.PxiId = []byte(id)
	image.PageNum	= st33[17:21]  // page nunber
	image.RefNum	= st33[34:45]  // was 41
	image.NumPages 	= st33[76:80]  // Total number of pages
	image.DataType	= st33[180:181]  // data type

	/*
		comp_meth := st33[181:183]
		k_fac := st33[183:185]
		Resolution := st33[185:187]
	*/
	Resolution := st33[185:187]
	sFrH 	:= st33[187:190]
	sFrW 	:= st33[190:193]
	nlFrH 	:= st33[193:197]
	nlFrW 	:= st33[197:201]
	rotationCode := st33[201:202]

	// fr_x := st33[202:206]
	// fr_y := st33[206:210]
	// fr_stat := st33[210:211]

	version := st33[211:214]

	buf1	 := bytes.NewReader(buf[214 : 218]) // get  the ST33 version number

	if string(version) == "V30" {

		//	buf1 = bytes.NewReader(buf[k+214 : k+218])
		//	 some V30 total_length are encoded with big Endian byte order

		_ = binary.Read(buf1, Little, &totalLength)

		if int(totalLength) > 16777215 {
			buf1 = bytes.NewReader(buf[214 : 218])
			_ = binary.Read(buf1, Big, &totalLength)
		}

	} else {
		_ = binary.Read(buf1, Big, &totalLength) // get  total length of the image
	}

	//  Build the tiff image header

	var img = new(bytes.Buffer)

	_ = SetTiffMagicNumber(img, enc)                    // Magic number   6 bytes
	_ = SetTiffImageWidth(img, enc, nlFrW)              //  image WIDTH   12 bytes
	_ = SetTiffImageLength(img, enc, nlFrH)             //  image HEIGHT  12 bytes
	_ = SetTiffImageCompression(img, enc)               // image compression cG4 12 bytes
	_ = SetTiffImagePhotometric(img, enc)               //  image Photometric 12 bytes
	_ = SetTiffImageStripOffset(img, enc)               //  image Stripoffsets  12 bytes
	_ = SetTiffImageOrientation(img, enc, rotationCode) //  image Orientation   12 bytes

	//   computing the image length is done after every records containing the image  are read
	//   continue to create the  TIFF header before the image length attribute

	var img2 = new(bytes.Buffer)


	_ = SetTiffImageXresolution(img2,enc)    //  image X resolution
	_ = SetTiffImageYresolution(img2, enc)    //  image Y resolution
	_ = SetTiffImageResolutionUnit(img2, Resolution, enc) //  image resolution Unit

	_ = binary.Write(img2, enc, uint32(0)) // next IFD = 0

	_ = binary.Write(img2, enc, Getuint32(nlFrW)*25) // Xresolution value
	_ = binary.Write(img2, enc, Getuint32(sFrW))

	_ = binary.Write(img2, enc, Getuint32(nlFrH)*25) // Yresoluton value
	_ = binary.Write(img2, enc, Getuint32(sFrH))


	imageL := 0    // Total length of the image
	// build the image with the St33 first record
	_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)


    rec := 1
	imgL,err1 := writeImg(img2,buf)
	if err1 == nil {
		imageL += imgL
	} else {
		gLog.Error.Printf("Record:%d of %d - Error: %v ",rec,int(totalRec),err1)
		return nrec,err,err1
	}

	// read all the records for this image.
	// the number of records are extracted from the image header
	//  if the total number of the records is >  the control records  the take the total number of records from the
	//  control file


	for rec := 2; rec <= int(totalRec); rec++ {
		if buf,err = r.Read();err == nil  {
			nrec++ // increment the number of read records
			imgL, err1 = writeImg(img2, buf)
			if err1 == nil {
				imageL += imgL
			} else {
				gLog.Error.Printf("Record:%d of %d -  Error:%v ", rec, int(totalRec), err1)
				return nrec, err, err1
			}
		} else {
			break
		}
	}

	gLog.Trace.Printf("PixId: %s - Total number of records from ST33 header: %d - Total number of read records: %d ",v.PxiId,totalRec,nrec)

	//	Check if the input header of the first record we read is an ST33 record
	//	totalLength is the length of the TIFF image extracted from the first record  of the image
	//	imageL is the sum of the image length of all records = true length of the image

	var img3 = new(bytes.Buffer)
	if int(totalLength) < imageL {
		totalLength = uint32(imageL)
	}

	//  set the TIFF image length */
	_ = SetTiffImageStripByteCount(img3, enc, uint32(imageL)) //  image Strip Byte Counts

	//		Append  img3 and img2 into img to form the final TIFF image
	//	    img2 and  img3  bytes buffer willbe reset when this function is exited

	img.Write(img3.Bytes())
	img.Write(img2.Bytes())
	defer img2.Reset()
	defer img3.Reset()

	// return the final image in the image struct
	//  It is recommended to reset the the buffer of the image when it is consummed  by the client  */
	image.Img = img

	// The caller must check if number of records from the control file match the number of records in the data file
	// If not the caller must skip all the remaining records in the data file
	return  nrec,err,err1
}


func writeImg(img2 *bytes.Buffer, buf []byte) (int, error){

	var (
		imgl uint16
		err error
		Big		= binary.BigEndian
	)
	_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)
	err = nil
	if int64(imgl) <= int64(len(buf)-252) {
		img2.Write(buf[252 : 252+int64(imgl)]) // append  the image length found in this record  to the  image
		                //  Compute the total  image length
	} else {
		gLog.Error.Printf("Dumping ST33 header of the record: %s", hex.Dump(buf[0:252]))
		error := fmt.Sprintf("Invalid ST33 image length: %d - Buffer length: %d",int64(imgl), int64(len(buf)-252))
		err = errors.New(error)
		/* gLog.Error.Printf("%v",err)*/
	}
	return int(imgl),err
}


func (image *PxiImg) BuildTiffImageV2(r *St33Reader, v Conval,cp int) (int,error,error) {

	var (
		totalRec       uint16
		totalLength    uint32
		recs           uint16
		imgl           uint16
		err, err1   		   error
		nrec           int = 0
		st33,buf       []byte
		id             string

	)
	gLog.Trace.Printf("PxiId: %s - Building image - Page number %d  of %d pages",v.PxiId,cp,v.Pages)
	Little	:= binary.LittleEndian
	Big		:= binary.BigEndian
	enc 	:= Big
	// Skip records if the  page number of st33 header <  the current page
	for {
		buf, err = r.Read()
		if err != nil {
			return nrec, err, err1
		}
		nrec++     // increment the number of total record read
		err1 = CheckST33Length(&v, r, buf)
		if err1 != nil {
			gLog.Error.Println(err)
			gLog.Info.Printf("Dump of the ST33 header of the current record: %s", hex.Dump(buf[0:255]))
			/*
			buf, err = r.Read()
			m:= "Dump of the ST33 header of the next record"
			if len(buf) > 252 {
				gLog.Info.Printf("%s: %s", m,hex.Dump(buf[0:255]))
			} else {
				gLog.Info.Printf("%s: %s", m,hex.Dump(buf[0:len(buf)-1]))
			}
			 */
			return nrec, err, err1
		}
		st33 = utils.Ebc2asci(buf[0:214])
		// get the  id of the image
		id = string(st33[9:17]) + string(st33[7:9])
		pn, _ := strconv.Atoi(string(st33[17:21]))  /* get the page number of the image */
		k := v.PxiId[9:len(v.PxiId)-2]
		/*
			Compare the page number of the image with the current page
		    if cp > pn  then  skip the record
		    if less or equal just

		 */
		if cp > pn {  //  I am reading a record of the previous image- Just reas the next record
			gLog.Warning.Printf("PxiId: %s - Skipping a record ( %d bytes) - current page number:%d > page number of the image: %d - image id/pxiId %s/%s ",v.PxiId,len(buf),cp,pn,id ,k)
			if id != k  {
				/*  overflow */
				RewindST33(v,r,-1)
				nrec--
				return nrec,nil,nil
			}
		} else {
			break
		}
	}


	/* check if record  length = length  of the ST33 record   */
	err 		= 	binary.Read(bytes.NewReader(buf[25 : 27]), Big, &recs)
	err 		= 	binary.Read(bytes.NewReader(buf[84 : 86]), Big, &totalRec)
	err 		= 	binary.Read(bytes.NewReader(buf[214 : 218]), Big, &totalLength)
	err			= 	binary.Read(bytes.NewReader(buf[250 : 252]), Big, &imgl)
	/*
		convert St33 encoded Big Endian input data ( EBCDIC) to  Little Endian (ASCII)
	*/
	// st33 	:= utils.Ebc2asci(buf[0: 214])

	// long, _ := 	strconv.Atoi(string(st33[0:5]))

	// image.PxiId		= st33[5:17]   //  PXI ID
	image.PxiId = []byte(id)
	image.PageNum	= st33[17:21]  // page nunber
	image.RefNum	= st33[34:45]  // was 41
	image.NumPages 	= st33[76:80]  // Total number of pages
	image.DataType	= st33[180:181]  // data type

	/*
		comp_meth := st33[181:183]
		k_fac := st33[183:185]
		Resolution := st33[185:187]
	*/
	Resolution := st33[185:187]
	sFrH 	:= st33[187:190]
	sFrW 	:= st33[190:193]
	nlFrH 	:= st33[193:197]
	nlFrW 	:= st33[197:201]
	rotationCode := st33[201:202]

	// fr_x := st33[202:206]
	// fr_y := st33[206:210]
	// fr_stat := st33[210:211]

	version := st33[211:214]

	buf1	 := bytes.NewReader(buf[214 : 218]) // get  the ST33 version number

	if string(version) == "V30" {

		//	buf1 = bytes.NewReader(buf[k+214 : k+218])
		//	 some V30 total_length are encoded with big Endian byte order

		_ = binary.Read(buf1, Little, &totalLength)

		if int(totalLength) > 16777215 {
			buf1 = bytes.NewReader(buf[214 : 218])
			_ = binary.Read(buf1, Big, &totalLength)
		}

	} else {
		_ = binary.Read(buf1, Big, &totalLength) // get  total length of the image
	}

	//  Build the tiff image header

	var img = new(bytes.Buffer)

	_ = SetTiffMagicNumber(img, enc)                    // Magic number   6 bytes
	_ = SetTiffImageWidth(img, enc, nlFrW)              //  image WIDTH   12 bytes
	_ = SetTiffImageLength(img, enc, nlFrH)             //  image HEIGHT  12 bytes
	_ = SetTiffImageCompression(img, enc)               // image compression cG4 12 bytes
	_ = SetTiffImagePhotometric(img, enc)               //  image Photometric 12 bytes
	_ = SetTiffImageStripOffset(img, enc)               //  image Stripoffsets  12 bytes
	_ = SetTiffImageOrientation(img, enc, rotationCode) //  image Orientation   12 bytes

	//   computing the image length is done after every records containing the image  are read
	//   continue to create the  TIFF header before the image length attribute

	var img2 = new(bytes.Buffer)


	_ = SetTiffImageXresolution(img2,enc)    //  image X resolution
	_ = SetTiffImageYresolution(img2, enc)    //  image Y resolution
	_ = SetTiffImageResolutionUnit(img2, Resolution, enc) //  image resolution Unit

	_ = binary.Write(img2, enc, uint32(0)) // next IFD = 0

	_ = binary.Write(img2, enc, Getuint32(nlFrW)*25) // Xresolution value
	_ = binary.Write(img2, enc, Getuint32(sFrW))

	_ = binary.Write(img2, enc, Getuint32(nlFrH)*25) // Yresoluton value
	_ = binary.Write(img2, enc, Getuint32(sFrH))


	imageL := 0    // Total length of the image
	// build the image with the St33 first record
	_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)

	/*
		img2.Write(buf[252 : 252+int64(imgl)])       // append  the image length found in this record  to the  image
		imageL += int(imgl)
	*/
	rec := 1
	imgL,err1 := writeImg(img2,buf)
	if err1 == nil {
		imageL += imgL
	} else {
		gLog.Error.Printf("Record:%d of %d - Error: %v ",rec,int(totalRec),err1)
		return nrec,err,err1
	}

	// read all the records for this image.
	// the number of records are extracted from the image header
	//  if the total number of the records is >  the control records  the take the total number of records from the
	//  control file

	/*
		for rec := 2; rec <= int(totalRec); rec++ {

			if buf,err = r.Read();err == nil  {

					nrec++ // increment the number of read records
					imgL, err1 = writeImg(img2, buf)

					if err1 == nil {
						imageL += imgL
					} else {
						gLog.Error.Printf("Record:%d of %d -  Error:%v ", rec, int(totalRec), err1)
						return nrec, err, err1
					}

			} else {
				break
			}
		}

	 */


	tRecord := int(totalRec)
	for {
		if rec >= tRecord {
			break
		}
		if buf,err = r.Read();err == nil  {
			rec++  //increment the number of records for this page
			nrec++ // increment the total  number of read records for this image
			imgL, err1 = writeImg(img2, buf)
			if err1 == nil {
				imageL += imgL
			} else {
				// dump the next record for trouble shooting and exit
				if buf,err = r.Read();err == nil && len(buf) > 255 {
					gLog.Info.Printf("Dump of the next record : %s", hex.Dump(buf[0:255]))
				}
				gLog.Error.Printf("Record:%d of %d -  Error:%v ", rec, int(totalRec), err1)
				return nrec, err, err1
			}
		} else {
			break
		}
	}

	gLog.Trace.Printf("PixId: %s - Total number of records from ST33 header: %d - Total number of read records: %d ",v.PxiId,totalRec,nrec)

	//	Check if the input header of the first record we read is an ST33 record
	//	totalLength is the length of the TIFF image extracted from the first record  of the image
	//	imageL is the sum of the image length of all records = true length of the image

	var img3 = new(bytes.Buffer)

	if int(totalLength) < imageL {
		totalLength = uint32(imageL)
	}

	//  set the TIFF image length */
	_ = SetTiffImageStripByteCount(img3, enc, uint32(imageL)) //  image Strip Byte Counts


	//		Append  img3 and img2 into img to form the final TIFF image
	//	    img2 and  img3  bytes buffer willbe reset when this function is exited

	img.Write(img3.Bytes())
	img.Write(img2.Bytes())

	defer img2.Reset()
	defer img3.Reset()

	// return the final image in the image struct
	//  It is recommended to reset the the buffer of the image when it is consummed  by the client  */
	image.Img = img

	// The caller must check if number of records from the control file match the number of records in the data file
	// If not the caller must skip all the remaining records in the data file

	return  nrec,err,err1

}