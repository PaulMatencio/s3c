package utils

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	"io"
	"io/ioutil"
	"os"
)

//  object is  io reader
func ReadObject( object io.Reader)  (*bytes.Buffer, error){
	buffer:= make([]byte,32768)
	buf := new(bytes.Buffer)
	for {

		n, err := object.Read(buffer)
		if err == nil || err == io.EOF {
			buf.Write(buffer[:n])
			if err == io.EOF {
				buffer = buffer[:0] // clear the buffer fot the GC
				return buf,nil
			}
		} else {
			buffer = buffer[:0] // clear the buffer for the GC
			return buf,err
		}
	}
}

func SaveObject(result *s3.GetObjectOutput, pathname string) (error) {

	var (
		err error
		f  *os.File
	)

	if f,err = os.Create(pathname); err == nil {
		_,err = io.Copy(f, result.Body);
	}

	return err
}

func WriteObj(b *bytes.Buffer,pathname string) {

	if err:= ioutil.WriteFile(pathname,b.Bytes(),0644); err == nil {
		gLog.Info.Printf("object downloaded to %s",pathname)
	} else {
		gLog.Info.Printf("Error %v downloading object",err)
	}
}

