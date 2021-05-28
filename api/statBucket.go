
package api
import (
"github.com/aws/aws-sdk-go/service/s3"
"github.com/aws/aws-sdk-go/aws"
"github.com/paulmatencio/s3c/datatype"
)
func StatBucket(req datatype.StatBucketRequest) ( *s3.HeadBucketOutput,error){

	input := &s3.HeadBucketInput{
		Bucket: aws.String(req.Bucket),
	}

	return req.Service.HeadBucket(input)

}
