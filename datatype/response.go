package datatype

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
)

type  Rh struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}


type  Ro struct {
	Key string
	Result   *s3.GetObjectOutput
	Err error
}

type Robj struct {
	Key string
	Body *bytes.Buffer
	Metadata map[string]*string
	Err  error
}

type  Rp struct {

	Idir string
	Key string
	Result   *s3.PutObjectOutput
	Err error
}

type  Rb struct {
	Key      string
	Object   *bytes.Buffer
	Result   *s3.GetObjectOutput
	Err      error
}

type  Rc struct {

	Key string
	Result   *s3.CopyObjectOutput
	Err error

}

type Rlb struct {

	StatusCode   int
	Contents     string
	Err          error
}

/*
type PutS3Response struct {

	Bucket  string
	Key     string
	Size    int
	Error   st33.S3Error

}
*/

type Rm struct {
	Nerrors int
	Ndocs   int
	Npages  int
	Docsizes int
}