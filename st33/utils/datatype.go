package st33

import (
	"bytes"
	"encoding/binary"
	"time"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Tiff struct {
	Enc          binary.ByteOrder
	SFrH         []byte
	SFrW         []byte
	NlFrH        []byte
	NlFrW        []byte
	RotationCode []byte
	TotalLength  int
}


type Request struct {
	InputFile  string
	WriteTo    string
	OutputDir  string   /* Directory an S3 bucket  */
}

type Response struct {
	Bucket  string
	Key   	string
	Data    bytes.Buffer
	Number	int
	Error 	S3Error

}

type S3Error struct {
	Key 	string
	Err	    error
}

// import "github.com/paulmatencio/s3c/st33/utils"

type St33ToS3      struct {
	Request  		 ToS3Request       `json:"request"`
	Response         ToS3Response      `json:"response"`
}


type ToS3Request struct {

	File 		      string     `json:"input-file"`
	Bucket 		      string     `json:"target-bucket"`
	Number            int        `json:"number-of-bucket"`
	LogBucket         string    `json:"logging-bucket"`
	DatafilePrefix    string     `json:"data-file-prefix"`
	CrlfilePrefix     string     `json:"control-file-prefix"`
	Profiling 	      int        `json:"run-with-profiling"`
	Reload            bool       `json:"reload"`
	Async 		      int        `json:"run-with-concurrent-number"`
	Check             bool       `json:"check"`

}

type ToS3Response struct {

	Time   time.Time  `json:"ended-time"`
	Status string     `json:"uploaded-status"` // upload status
	Duration  string  `json:"time-to-uploaded"` // duration of the process
	Docs  int         `json:"number-of-documents"` // number of documenst
	Pages int         `json:"number-of-pages"`  // number of pages
	Size  int         `json:"total-uploaded-size"` // total uploaded size
	Erroru int		  `json:"number-of-uploaded-errors"` // number of uploaded errors
	Errori int        `json:"number-of-other-errors"` // number of uploaded errors
	// Error  []S3Error  `json:"array-of-errors,omitempty"` // key value
}

type ToS3GetPages struct {
	KEY string
	Req *ToS3Request
	CP int
	Step int
	R *St33Reader
	V Conval
	Svc *s3.S3
}