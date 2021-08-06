package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/paulmatencio/s3c/datatype"
	"io"
	"os"
)

func GetMultipart(req datatype.GetMultipartObjRequest ) (int64,error) {

	input := s3.GetObjectInput{
		Key:    aws.String(req.Key),
		Bucket: aws.String(req.Bucket),
	}
    if req.PartNumber > 0 {
		input.PartNumber =  aws.Int64(req.PartNumber)
	}

	downLoader := s3manager.NewDownloaderWithClient(req.Service, func(d *s3manager.Downloader) {
		d.PartSize = req.PartSize
		d.Concurrency = req.Concurrency
	})

	if fd, err := os.OpenFile(req.OutputFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666); err == nil {
		return downLoader.Download(io.WriterAt(fd), &input)
	}  else {
		return 0, err
	}
}

func GetMultipartToBuffer(req datatype.GetMultipartObjRequest) (int64,*aws.WriteAtBuffer,error)  {

	buff := &aws.WriteAtBuffer{}
	input := s3.GetObjectInput{
		Key:    aws.String(req.Key),
		Bucket: aws.String(req.Bucket),
	}

    if req.PartNumber > 0 {
		input.PartNumber =  aws.Int64(req.PartNumber)
	}

	downLoader := s3manager.NewDownloaderWithClient(req.Service, func(d *s3manager.Downloader) {
		d.PartSize = req.PartSize
		d.Concurrency = req.Concurrency
	})
	n,err := downLoader.Download(buff,&input)

	return n, buff,err

}
