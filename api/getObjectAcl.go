

package api

import (
"github.com/aws/aws-sdk-go/aws"
"github.com/aws/aws-sdk-go/service/s3"
"github.com/paulmatencio/s3c/datatype"
)

func GetObjectAcl(req datatype.GetObjAclRequest) (*s3.GetObjectAclOutput,error) {

	input := &s3.GetObjectAclInput{
		Bucket: aws.String(req.Bucket),
		Key :aws.String(req.Key),
	}
	return req.Service.GetObjectAcl(input)
}