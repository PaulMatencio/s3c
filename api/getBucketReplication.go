package api
import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func GetBucketReplication(req datatype.GetBucketReplicationRequest) (*s3.GetBucketReplicationOutput,error){
	input := &s3.GetBucketReplicationInput{
		Bucket: aws.String(req.Bucket),
	}
	return req.Service.GetBucketReplication(input)
}


