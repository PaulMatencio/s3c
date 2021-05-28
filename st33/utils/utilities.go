package st33

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	// imaging "github.com/desintegration/imaging"
)




/*
	image orientation
*/
func GetOrientation(rotation_code []byte) uint16 {
	orientation, _ := strconv.Atoi(string(rotation_code))
	switch orientation {
	case 1:
		return uint16(1)
	case 2:
		return uint16(6)
	case 3:
		return uint16(3)
	case 4:
		return uint16(8)
	default:
		return uint16(1)
	}
}

func Getuint32(in []byte) uint32 {
	out, _ := strconv.Atoi(string(in))
	return uint32(out)
}

func Getuint16(in []byte) uint16 {
	out, _ := strconv.Atoi(string(in))
	return uint16(out)
}

/*
		6 bytes
 */

func SetTiffMagicNumber(buffer *bytes.Buffer,enc binary.ByteOrder  )  error {
	_, err := io.WriteString(buffer, beHeader)          // magic number
	err = binary.Write(buffer, enc, uint32(ifdOffset)) // IFD offset
	err = binary.Write(buffer, enc, uint16(ifdLen))    // number of IFD entries
	return err
}
/*
		12  bytes
 */
func SetTiffImageWidth(buffer *bytes.Buffer, enc binary.ByteOrder, width []byte) error{
	err := binary.Write(buffer, enc, uint16(tImageWidth)) //  image Width
	err = binary.Write(buffer, enc, uint16(dtLong))      //  long
	err = binary.Write(buffer, enc, uint32(1))           //  value
	err = binary.Write(buffer, enc, Getuint32(width))
	return err
}
/*
			12  bytes
 */
func SetTiffImageLength(buffer *bytes.Buffer, enc binary.ByteOrder, width []byte) error {
	err := binary.Write(buffer, enc, uint16(tImageLength)) //  image lenght
	err = binary.Write(buffer, enc, uint16(dtLong))        //  long
	err = binary.Write(buffer, enc, uint32(1))             //  value
	err = binary.Write(buffer, enc, Getuint32(width))
	return err
}
/*
		12  bytes
 */
func SetTiffImageCompression(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tCompression)) //  Compression
	err = binary.Write(buffer, enc, uint16(dtShort))      //  short
	err = binary.Write(buffer, enc, uint32(1))            //  value
	err = binary.Write(buffer, enc, uint16(cG4))          //  CCITT Group 4
	err = binary.Write(buffer, enc, uint16(0))            //  CCITT Group 4
	return err
}

/*
		12 bytes
 */
func SetTiffImagePhotometric(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tPhotometricInterpretation)) //  Photometric
	err = binary.Write(buffer, enc, uint16(dtShort))                    //  short
	err = binary.Write(buffer, enc, uint32(1))                          //  value
	err = binary.Write(buffer, enc, uint32(0))                          //  white
	return err
}
/*
		 12 bytes
 */
func SetTiffImageStripOffset(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tStripOffsets)) //  StripOffsets
	err = binary.Write(buffer, enc, uint16(dtLong))        //  long
	err = binary.Write(buffer, enc, uint32(1))             //  value
	err = binary.Write(buffer, enc, uint32(150))           //  0xA0
	return err
}
/*

 */
func SetTiffImageOrientation(buffer *bytes.Buffer,enc binary.ByteOrder,rotationCode []byte) error {
	err := binary.Write(buffer, enc, uint16(tOrientation)) // Orientation
	err = binary.Write(buffer, enc, uint16(dtShort))      //  short
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, GetOrientation(rotationCode)) // rotation code
	err = binary.Write(buffer, enc, uint16(0))
	return err
}
/*
			12 bytes
 */
func SetTiffImageStripByteCount(buffer *bytes.Buffer,enc binary.ByteOrder, totalLength uint32) error{
	err := binary.Write(buffer, enc, uint16(tStripByteCounts)) //  StripbyteCounts
	err = binary.Write(buffer, enc, uint16(dtLong))           //  long
	err = binary.Write(buffer, enc, uint32(1))
	// imageLPos := buffer
	err = binary.Write(buffer, enc, uint32(totalLength))      //  image size
	return err
}

/*
		12 bytes
 */

func SetTiffImageXresolution(buffer *bytes.Buffer, enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tXResolution)) // Xresolution
	err = binary.Write(buffer, enc, uint16(dtRational))   // rational
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint32(xoffset)) //
	return err
}

/*
		12 bytes
 */

func SetTiffImageYresolution(buffer *bytes.Buffer,enc binary.ByteOrder) error {
	err := binary.Write(buffer, enc, uint16(tYResolution)) // Yresolution
	err = binary.Write(buffer, enc, uint16(dtRational))   // rational
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint32(yoffset))
	return err
}

/*
			12 bytes
 */
func SetTiffImageResolutionUnit(buffer *bytes.Buffer,resolution []byte,enc binary.ByteOrder) error {
	err := binary.Write(buffer, enc, uint16(tResolutionUnit)) // resolution Unit
	err = binary.Write(buffer, enc, uint16(dtShort))         //  value
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint16(2)) //  2 instead of 3
	err = binary.Write(buffer, enc, uint16(0))
	return err
}

/*
	Generate a magic number for bucket location
 */
func GetMagic( id string) (string){
	var total uint = 0
	for i,r := range id {
		if r & 1 == 1 {
			total += uint(r)*uint(i)
		} else {
			total += uint(r) << 1
		}
	}
	total = total & 0xFF
	return  "pxi"+fmt.Sprint(total)
}


/*
	write image to a file
	create the file path if directories don't exist
 */


func WriteImgToFile(pathname string ,img *bytes.Buffer) (error) {
	// log.Println("write",filePath)
	dir,_ := filepath.Split(pathname)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	return  utils.WriteFile(pathname, img.Bytes(), 0644);
}

func BuildUsermd(v Conval) (map[string]string,error)  {

	var (
		err 	error
		metad = make(map[string]string)
	)

	if jsonB,err  := json.Marshal(v); err == nil {
		json :=base64.StdEncoding.EncodeToString(jsonB)
		pages:= strconv.Itoa(int(v.Pages))
		lp := len(v.PxiId)
		if v.PxiId[lp-2:lp-1] == "B" {
			pages = "1"
		}
		metad["Usermd"] = json
		metad["Pages"] = pages
	}

	return metad,err
}


func WriteUsermd(metad map[string]string,pathname string)  (error){

	if usermd,err := json.Marshal(metad) ; err == nil {
		return ioutil.WriteFile(pathname,[]byte(usermd),0644)
	} else {
		return err
	}
}

func writeToS3( r datatype.PutObjRequest) (*s3.PutObjectOutput,error){

	gLog.Trace.Println("Write to ", r.Bucket, r.Key,r.Buffer.Len())
	return api.PutObject2(r)
}


//
//  log to the migration log  bucket
//
func logIt(svc *s3.S3, req *ToS3Request,resp *ToS3Response,errors *[]S3Error) (*s3.PutObjectOutput,error){

	var (
		d,key = filepath.Split(req.File)
		buffer string
	)

	p1 := strings.Split(d,"/")
	p := p1[len(p1)-2]
	key = p + "." + key

	st33toS3 := St33ToS3 {
		Request : *req,
		Response: *resp,
	}

	// Build meta
	meta,_ := json.Marshal(&st33toS3)
	metad:= map[string]string{}
	metad["Migration-log"] = string(meta)

	// add data only if there are some errors
	if len(*errors) > 0 {
		for _,v := range *errors {
			buffer = buffer + fmt.Sprintf("Key: %s - Error: %v\n",v.Key,v.Err)
		}
	}

	pr := datatype.PutObjRequest{
		Service: svc,
		Bucket: req.LogBucket,
		Key: key,
		Buffer: bytes.NewBuffer([]byte(buffer)),
		Usermd: metad,
	}
	return api.PutObject2(pr)

}

func checkDoLoad(getRequest datatype.GetObjRequest, infile string) (bool) {


	// if the object exist then
	//  datafile was already uploaded
	//  	return
	//     		- fully uploaded  and reload  false ->  false
	//    		 - fully upoloaded and reload true -> True
	//     		- otherwise true
	//  if bucket or object do not exist -->  true


	do := true
	if result, err := api.GetObject(getRequest); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				gLog.Warning.Printf("Warning : [%s] does not exist, please use command <sc mkBucket> to create it", getRequest.Bucket)
			case s3.ErrCodeNoSuchKey:
				gLog.Info.Printf("datafile  %s was not yet uploaded", getRequest.Key)
			default:
			}

		}
	} else {

		// check if the datafile <infile> was already loaded without error

		metad := result.Metadata
		if meta,ok :=  metad["Migration-Log"] ; ok {
			m := St33ToS3{}
			if err := json.Unmarshal([]byte(*meta),&m); err == nil {
				switch m.Response.Status {
				case FullyUploaded:
					gLog.Info.Printf("Data file %s was already %s, use --reload to reload it",infile, m.Response.Status)
					do = false

				case FullyUploaded2:
					gLog.Info.Printf("Data file %s was already %s, use --reload to reload it",infile,m.Response.Status)
					do = false

				case PartiallyUploaded:
					do = true
				}

			} else {
				gLog.Error.Printf("%v",err)
			}
		}
	}
	return do
}



func IsST33Blob(buf []byte, k int64) (bool) {

	if len(buf) < int(k+182) {
		return false
	}
	l1 := utils.Ebc2asci(buf[0: 5])              // convert EBCDIC to Ascii
	long, _ := strconv.Atoi(string(l1))

	if im :=  string(utils.Ebc2asci(buf[k+180 : k+181])); im == "I"  && long== len(buf) {
		return true
	} else {
		return false}
}


func CheckST33Length(v *Conval , r *St33Reader, buf []byte) (error) {

	var err error

	l1 := utils.Ebc2asci(buf[0: 5])              // convert EBCDIC to Ascii
	long, _ := strconv.Atoi(string(l1))

	if long != len(buf) {
		err = errors.New(fmt.Sprintf("Inavlid ST33 record %s/%s at input byte address prev: X'%x'/ cur:X'%x' - Buffer length %d != ST33 record length %d ", v.PxiId, utils.Reverse(v.PxiId), r.Previous, r.Current, len(buf), long))
		return err
	} else {
		return nil
	}
}

func RewindST33(v Conval, r *St33Reader,diff int) {
	diff = -diff
	gLog.Warning.Printf("PXIID %s - %s - rewinding by %d record from address X'%x'", v.PxiId,r.File.Name(), diff, r.Current)
	for sk:= 1; sk <= diff; sk ++ {
		r.Current = r.Stack.Pop().Address
		gLog.Trace.Printf("PXIID %s -%s - Rewinding to address X'%x'",v.PxiId,r.File.Name(),r.Current)
	}
}


func SkipST33(v Conval,r *St33Reader,diff int) {
	for m:=1; m <= diff; m++ { // SKIP missing records
		if buf,err := r.Read(); err == nil {
			ST33 := utils.Ebc2asci(buf[0:214])
			pagenum, _ := strconv.Atoi(string(ST33[17:21]))
			/* ST33[5:17] */
			section := ST33[9:17]
			cc := ST33[7:9]
			id := string(section) + string(cc)
			gLog.Warning.Printf("PXIID: %s/%s - Skip %d record of page number: %d - Length of the record: %d", v.PxiId,id, m, pagenum,len(buf))
		}
	}
}



