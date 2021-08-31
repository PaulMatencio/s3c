package lib

import (
	"bufio"
	"github.com/aws/aws-sdk-go/service/s3"
	"time"
)

func ListPn(buf *bufio.Scanner, num int) (*s3.ListObjectsV2Output, error) {

	var (
		T            = true
		Z      int64 = 0
		D            = time.Now()
		result       = &s3.ListObjectsV2Output{
			IsTruncated: &T,
		}
		// next   int = 0
		err     error
		objects []*s3.Object
	)

	for k := 1; k <= num; k++ {
		var object s3.Object
		if buf.Scan() {
			if text := buf.Text(); len(text) > 0 {
				object.Key = &text
				object.Size = &Z
				object.LastModified = &D
				objects = append(objects, &object)
				result.StartAfter = &text
			} else {
				T = false
				result.IsTruncated = &T
			}
		} else {
			T = false
			result.IsTruncated = &T
		}
	}
	result.Contents = objects
	return result, err
}