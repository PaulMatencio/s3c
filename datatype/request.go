package datatype

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"time"

	// "github.com/aws/aws-sdk-go/aws"
	"net/http"
)

type CreateSession struct {
	Region 		string
	EndPoint 	string
	AccessKey   string
	SecretKey   string
}

type GetObjRequest struct {
	Service 	*s3.S3
	Bucket 		string
	Key    		string
	VersionId   string
}


type GetMultipartObjRequest struct {

	Service     *s3.S3
	Bucket       string
	Key          string
	PartNumber   int64
	PartSize     int64
	Concurrency  int
	OutputFilePath  string
	Discard      bool
}

type CopyObjRequest struct {

	Service 	*s3.S3
	Sbucket 	string
	Tbucket     string
	Skey   		string
	Tkey        string

}

type CopyObjsRequest struct {

	Service 	*s3.S3
	Sbucket 	string
	Tbucket     string
	Skey   		[]string
	Tkey        []string

}

type PutObjRequest struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	Buffer       *bytes.Buffer
	Usermd      map[string]string
	Meta        []byte
	VersionId   string
}

type PutObjRequest3 struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	Buffer       *bytes.Buffer
	Metadata     map[string]*string
	ContentType  string
}


type FputObjRequest struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	Inputfile     string
	Usermd       map[string]string
	Meta         []byte
}

type ListObjRequest struct {
	Service 	*s3.S3
	Bucket       string
	Prefix       string
	MaxKey	      int64
	Marker        string
	Delimiter     string
}
type ListObjV2Request struct {
	Service 	*s3.S3
	Bucket       string
	Prefix       string
	MaxKey	      int64
	Continuationtoken	 string
	Marker        string
	Delimiter     string
}
type ListObjVersionsRequest struct {
	Service 	*s3.S3
	Bucket       string
	Prefix       string
	MaxKey	      int64
	KeyMarker       string
	VersionIdMarker string
	Delimiter     string
}

type ListMultipartObjRequest struct {
	Service 	*s3.S3
	Bucket       string
	Prefix       string
	MaxUploads	 int64
	UploadIdmarker string
	KeyMarker        string
	Delimiter     string
}

type ListBucketRequest struct {
	Service 	*s3.S3
}

type ListObjLdbRequest struct {
	Url          string
	Bucket       string
	Prefix       string
	MaxKey	      int64
	Marker        string
	ListMaster    bool
	Delimiter     string
}

type MakeBucketRequest struct {
	Service 	*s3.S3
	Bucket string
}


type DeleteBucketRequest struct {
	Service 	*s3.S3
	Bucket string
}

type DeleteObjRequest struct {
	Service 	*s3.S3
	Bucket       string
	Key          string
	VersionId	string
}

type StatObjRequest struct {
	Service  *s3.S3
	Bucket    string
	Key       string
}

type StatObjRequestV2 struct {
	Client      *http.Client
	Request      string
	AccessKey    string
	SecretKey    string
	Bucket   	 string
	Key          string
}

type StatBucketRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetBucketPolicyRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetBucketReplicationRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetBucketAclRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetObjAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	Key    		string

}

type PutBucketAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	ACL  		 Acl

}

type PutObjectAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	Key         string
	ACL  		Acl

}

type MultiPartUploadRequest struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	PartNumber   int64
	UploadId     string
	Buffer       *bytes.Buffer
	// Metadata     map[string]*string
}

type CreateMultipartUploadRequest struct {
	 Service     *s3.S3
	 Bucket      string
	 Key         string
	 Metadata    map[string]*string
	 VersionId   string
	 ContentType string
}

type CompleteMultipartUploadRequest struct {
	Service  *s3.S3
	Resp     *s3.CreateMultipartUploadOutput
	CompletedParts []*s3.CompletedPart
}

type AbortMultipartUploadRequest struct {
	Service  *s3.S3
	Resp     *s3.CreateMultipartUploadOutput
}

type UploadPartRequest struct {
	Service  *s3.S3
	Resp     *s3.CreateMultipartUploadOutput
	Content  []byte
	PartNumber int
	RetryNumber int
	WaitTime  time.Duration
}

type Reqm struct {
	SrcS3 *s3.S3
	SrcBucket string
	TgtS3 *s3.S3
	TgtBucket string
	Incremental bool
}