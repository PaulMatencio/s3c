package bns

import (
	"encoding/json"
	"errors"
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	base64 "github.com/paulmatencio/ring/user/base64j"
	goLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	// "github.com/bradfitz/slice"
)

func ST33toFiles(inputFile string, outputusermdDir string, outputTiffDir string, outputContainerDir string, combine bool) error {

	//   EXTRACT ST33 Files
	//
	//  FOR EACH DOCUMENT in ST33 {
	// 		CREATE  page metadata			 ( ST33 HEADER  ++)
	// 		CREATE  page data  (Tiff)      ( ST33 TIFF RECORDS )
	//
	// 		FOR EACH PAGE of a document {
	// 			 if combine  {
	//      		  COMBINE  page data (TIFF) and metadata => PAGE Struct
	//      		  WRITE PAGE struct
	//       	 }
	//  		else {
	//     		  	  WRITE data ( TIFF)
	//	     		  WRITE Metadata  ( user metadata)
	//       	}
	//      }
	//      CREATE DOCUMENT metadata
	//      WRITE DOCUMENT  metadata
	//  }
	//

	//   REMOVED  =>  Check old bns for how to do
	error := errors.New("Function has been removed")
	return error
}

func ST33toFiles_p(inputFile string, outputusermdDir string, outputTiffDir string, outputContainerDir string, combine bool) error {

	//   EXTRACT ST33 Files
	//
	//  FOR EACH DOCUMENT in ST33 {
	// 		CREATE  page metadata			 ( ST33 HEADER  ++)
	// 		CREATE  page data  (Tiff)      ( ST33 TIFF RECORDS )
	//
	// 		FOR EACH PAGE of a document {
	// 			 if combine  {
	//      		  COMBINE  page data (TIFF) and metadata => PAGE Struct
	//      		  WRITE PAGE struct
	//       	 }
	//  		else {
	//     		  	  WRITE data ( TIFF)
	//	     		  WRITE Metadata  ( user metadata)
	//       	}
	//      }
	//      CREATE DOCUMENT metadata
	//      WRITE DOCUMENT  metadata
	//  }
	//

	//   REMOVED  =>  Check old bns for how to do
	error := errors.New("Function has been removed")
	return error
}

func ParseDate(str string) (dd Date, err error) {
	str = strings.TrimSpace(str)
	var (
		y, m, d int
	)
	if len(str) != 8 {
		goto invalid
	}
	if y, err = strconv.Atoi(str[0:4]); err != nil {
		return
	}
	if m, err = strconv.Atoi(str[4:6]); err != nil {
		return
	}
	if m < 1 || m > 12 {
		goto invalid
	}
	if d, err = strconv.Atoi(str[6:8]); err != nil {
		return
	}
	if d < 1 || d > 31 {
		goto invalid
	}
	dd.Year = int16(y)
	dd.Month = byte(m)
	dd.Day = byte(d)
	return
invalid:
	err = errors.New("Invalid metadata Date string: " + str)
	return
}

func (dd Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", dd.Year, dd.Month, dd.Day)
}

func noDate() Date {
	//return Date{int16(0),byte(0),byte(0)}
	return Date{}
}

func getuint16(in []byte) uint16 {
	out, _ := strconv.Atoi(string(in))
	return uint16(out)

}

func getuint32(in []byte) uint32 {
	out, _ := strconv.Atoi(string(in))
	return uint32(out)

}

func getConfig(configfile string) (Configuration, error) {
	cfile, err := os.Open(configfile)
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(cfile)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	_ = cfile.Close()
	return configuration, err
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

/* image orientation */

func getOrientation(rotation_code []byte) uint16 {
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

func Tiff2Png(tiffile, pngfile string) error {
	// cmd := exec.Command("convert", "-resize", "950x", tiffile, pngfile)
	cmd := exec.Command("convert", tiffile, pngfile)
	return cmd.Run()

}

func RemoveSlash(input string) string {
	output := ""
	ar := strings.Split(input, "/")
	for _, word := range ar {
		output = output + word
	}
	return output
}

//  Used to PUT BLOB

func CopyBlob(bnsRequest *HttpRequest, url string, buf []byte, header map[string]string) {

	pid := os.Getpid()
	hostname, _ := os.Hostname()
	action := "CopyBlob"
	result := AsyncHttpPutBlob(bnsRequest, url, buf, header)
	if sproxyd.Test {
		goLog.Trace.Printf("URL => %s \n", result.Url)
		return
	}
	if result.Err != nil {
		goLog.Trace.Printf("%s %d %s status: %s\n", hostname, pid, result.Url, result.Err)
		return
	}

	resp := result.Response

	if resp != nil {
		goLog.Trace.Printf("%s %d %s status: %s\n", hostname, pid, url,
			result.Response.Status)
	} else {
		goLog.Error.Printf("%s %d %s %s %s", hostname, pid, url, action, "failed")
	}

	switch resp.StatusCode {
	case 200:
		goLog.Trace.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Key"])

	case 412:
		goLog.Warning.Println(hostname, pid, url, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], "already exist")

	case 422:
		goLog.Error.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Status"])
	default:
		goLog.Warning.Println(hostname, pid, url, resp.Status)
	}
	resp.Body.Close()
}

/*
func CopyBlobTest(bnsRequest *HttpRequest, url string, buf []byte, header map[string]string) {

	result := AsyncHttpPutBlobTest(bnsRequest, url, buf, header)
	goLog.Trace.Printf("URL => %s \n", result.Url)
}
*/

func BuildBnsResponse(resp *http.Response, contentType string, body *[]byte) BnsResponse {

	bnsResponse := BnsResponse{}
	if body != nil {
		if _, ok := resp.Header["X-Scal-Usermd"]; ok {
			bnsResponse.Usermd = resp.Header["X-Scal-Usermd"][0]
			if pagemd, err := base64.Decode64(bnsResponse.Usermd); err == nil {
				bnsResponse.Pagemd = pagemd
				goLog.Trace.Println("page meata=>", string(pagemd))
			}
		} else {
			goLog.Warning.Println("X-Scal-Usermd is missing in the resp header", resp.Status, resp.Header)
		}
		bnsResponse.Image = *body
	}
	patha := strings.Split(resp.Request.URL.Path, "/")
	page := patha[len(patha)-1]
	bnsResponse.PageNumber = page
	bnsResponse.Page, _ = strconv.Atoi(page[1:])
	bnsResponse.BnsId = strings.Join(patha[len(patha)-4:len(patha)-1], "/")
	bnsResponse.ContentType = contentType
	bnsResponse.HttpStatusCode = resp.StatusCode

	// defer resp.Body.Close()
	return bnsResponse
}

func BuildBnsResponseLi(resp *http.Response, contentType string, body *[]byte) BnsResponseLi {

	bnsResponse := BnsResponseLi{}
	if body != nil {
		if _, ok := resp.Header["X-Scal-Usermd"]; ok {
			Usermd := resp.Header["X-Scal-Usermd"][0]
			if pagemd, err := base64.Decode64(Usermd); err == nil {
				bnsResponse.Pagemd = pagemd
				goLog.Trace.Println("page meata=>", string(pagemd))
			}
		} else {
			goLog.Warning.Println("X-Scal-Usermd is missing in the resp header", resp.Status, resp.Header)
		}
		bnsResponse.Image = body /* just get the pointer */
	}
	patha := strings.Split(resp.Request.URL.Path, "/")
	page := patha[len(patha)-1]
	bnsResponse.Page, _ = strconv.Atoi(page[1:])
	bnsResponse.BnsId = strings.Join(patha[len(patha)-4:len(patha)-1], "/")
	bnsResponse.ContentType = contentType
	// bnsResponse.HttpStatusCode = resp.StatusCode
	// defer resp.Body.Close()
	return bnsResponse
}

func BuildPagesRanges(pagesranges string) ([]string, error) {

	var (
		out        string
		Start, End int
		err        error
	)
	PagesRangesa := strings.Split(pagesranges, ",")
	if len(PagesRangesa) == 0 {

		goLog.Error.Printf("Missing Pages ranges %s", pagesranges)
		return PagesRangesa, errors.New("Invalid pages ranges")
	}
	for _, pagesrange := range PagesRangesa {
		pa := strings.Split(pagesrange, ":")
		if Start, err = strconv.Atoi(pa[0]); err != nil {
			// fmt.Printf("Invalid Start pages ranges %s", pa[0])
			goLog.Error.Printf("Invalid start pages ranges %s", pa[0])
		}
		if End, err = strconv.Atoi(pa[1]); err != nil {
			goLog.Error.Printf("Invalid end  pages ranges %s", pa[1])
		}
		for k := Start; k <= End; k++ {
			out += fmt.Sprintf("%d,", k)
		}

	}
	return strings.Split(out[0:len(out)-1], ","), err

}

func SetCPU(cpu string) error {
	var numCPU int

	availCPU := runtime.NumCPU()

	if strings.HasSuffix(cpu, "%") {
		// Percent
		var percent float32
		pctStr := cpu[:len(cpu)-1]
		pctInt, err := strconv.Atoi(pctStr)
		if err != nil || pctInt < 1 || pctInt > 100 {
			return errors.New("Invalid CPU value: percentage must be between 1-100")
		}
		percent = float32(pctInt) / 100
		numCPU = int(float32(availCPU) * percent)
	} else {
		// Number
		num, err := strconv.Atoi(cpu)
		if err != nil || num < 1 {
			return errors.New("Invalid CPU value: provide a number or percent greater than 0")
		}
		numCPU = num
	}

	if numCPU > availCPU {
		numCPU = availCPU
	}

	runtime.GOMAXPROCS(numCPU)
	return nil
}
