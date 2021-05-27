package datatype

import (
	"log"
	"reflect"
	"time"
)

type S3Metadata struct {
	CommonPrefixes []interface{} `json:"CommonPrefixes"`
	Contents       []Contents    `json:"Contents"`
	IsTruncated    bool          `json:"IsTruncated"`
}


type LevelDBMetadata struct {  //same as LevelDBMetadata  returned by GET
	Bucket BucketInfo `json:"bucket"`
	Object Value      `json:"obj,omitempty"`
}

type	BucketInfo struct {
		Name                     string      `json:"name"`
		Owner                    string      `json:"owner"`
		OwnerDisplayName         string      `json:"ownerDisplayName"`
		CreationDate             time.Time   `json:"creationDate"`
		MdBucketModelVersion     int         `json:"mdBucketModelVersion"`
		Transient                bool        `json:"transient"`
		Deleted                  bool        `json:"deleted"`
		ServerSideEncryption     interface{} `json:"serverSideEncryption"`
		VersioningConfiguration  interface{} `json:"versioningConfiguration"`
		LocationConstraint       string      `json:"locationConstraint"`
		Cors                     interface{} `json:"cors"`
		ReplicationConfiguration interface{} `json:"replicationConfiguration"`
		LifecycleConfiguration   interface{} `json:"lifecycleConfiguration"`
	}

// structure returned by LevelDB  GET
type LevelDBMeta struct {  //same as LevelDBMetadata  returned by GET
	Bucket struct {
		Name                     string      `json:"name"`
		Owner                    string      `json:"owner"`
		OwnerDisplayName         string      `json:"ownerDisplayName"`
		CreationDate             time.Time   `json:"creationDate"`
		MdBucketModelVersion     int         `json:"mdBucketModelVersion"`
		Transient                bool        `json:"transient"`
		Deleted                  bool        `json:"deleted"`
		ServerSideEncryption     interface{} `json:"serverSideEncryption"`
		VersioningConfiguration  interface{} `json:"versioningConfiguration"`
		LocationConstraint       string      `json:"locationConstraint"`
		Cors                     interface{} `json:"cors"`
		ReplicationConfiguration interface{} `json:"replicationConfiguration"`
		LifecycleConfiguration   interface{} `json:"lifecycleConfiguration"`
	} `json:"bucket"`
	Obj struct {
		OwnerDisplayName                          string `json:"owner-display-name"`
		OwnerID                                   string `json:"owner-id"`
		ContentLength                             int    `json:"content-length"`
		ContentMd5                                string `json:"content-md5"`
		XAmzVersionID                             string `json:"x-amz-version-id"`
		XAmzServerVersionID                       string `json:"x-amz-server-version-id"`
		XAmzStorageClass                          string `json:"x-amz-storage-class"`
		XAmzServerSideEncryption                  string `json:"x-amz-server-side-encryption"`
		XAmzServerSideEncryptionAwsKmsKeyID       string `json:"x-amz-server-side-encryption-aws-kms-key-id"`
		XAmzServerSideEncryptionCustomerAlgorithm string `json:"x-amz-server-side-encryption-customer-algorithm"`
		XAmzWebsiteRedirectLocation               string `json:"x-amz-website-redirect-location"`
		ACL                                       struct {
			Canned      string        `json:"Canned"`
			FULLCONTROL []interface{} `json:"FULL_CONTROL"`
			WRITEACP    []interface{} `json:"WRITE_ACP"`
			READ        []interface{} `json:"READ"`
			READACP     []interface{} `json:"READ_ACP"`
		} `json:"acl"`
		Key            string      `json:"key"`
		Location       interface{} `json:"location"`
		IsDeleteMarker bool        `json:"isDeleteMarker"`
		Tags           struct {
		} `json:"tags"`
		ReplicationInfo struct {
			Status             string        `json:"status"`
			Backends           []interface{} `json:"backends"`
			Content            []interface{} `json:"content"`
			Destination        string        `json:"destination"`
			StorageClass       string        `json:"storageClass"`
			Role               string        `json:"role"`
			StorageType        string        `json:"storageType"`
			DataStoreVersionID string        `json:"dataStoreVersionId"`
		} `json:"replicationInfo"`
		DataStoreName  string    `json:"dataStoreName"`
		LastModified   time.Time `json:"last-modified"`
		MdModelVersion int       `json:"md-model-version"`
		XAmzMetaUsermd string    `json:"x-amz-meta-usermd"`
	} `json:"obj"`
}


//structure returned by LevelDB Get List

type ACL struct {
	Canned      string        `json:"Canned"`
	FULLCONTROL []interface{} `json:"FULL_CONTROL"`
	WRITEACP    []interface{} `json:"WRITE_ACP"`
	READ        []interface{} `json:"READ"`
	READACP     []interface{} `json:"READ_ACP"`
}

type Tags struct {
}
/*
type ReplicationInfo struct {
	Status             string        `json:"status"`
	Backends           []interface{} `json:"backends"`
	Content            []interface{} `json:"content"`
	Destination        string        `json:"destination"`
	StorageClass       string        `json:"storageClass"`
	Role               string        `json:"role"`
	StorageType        string        `json:"storageType"`
	DataStoreVersionID string        `json:"dataStoreVersionId"`
}
*/

type ReplicationInfo struct {
	Status   string `json:"status"`
	Backends []struct {
		Site               string `json:"site"`
		Status             string `json:"status"`
		DataStoreVersionID string `json:"dataStoreVersionId"`
	} `json:"backends"`
	Content      []string `json:"content"`
	Destination  string   `json:"destination"`
	StorageClass string   `json:"storageClass"`
	Role         string   `json:"role"`
	StorageType  string   `json:"storageType"`
}

type Location []struct {
	Key           string `json:"key"`
	Size          int    `json:"size"`
	Start         int    `json:"start"`
	DataStoreName string `json:"dataStoreName"`
	DataStoreType string `json:"dataStoreType"`
	DataStoreETag string `json:"dataStoreETag"`
}


type Value struct {
	OwnerDisplayName                          string          `json:"owner-display-name"`
	OwnerID                                   string          `json:"owner-id"`
	ContentLength                             int             `json:"content-length"`
	ContentMd5                                string          `json:"content-md5"`
	XAmzVersionID                             string          `json:"x-amz-version-id"`
	XAmzServerVersionID                       string          `json:"x-amz-server-version-id"`
	XAmzStorageClass                          string          `json:"x-amz-storage-class"`
	XAmzServerSideEncryption                  string          `json:"x-amz-server-side-encryption"`
	XAmzServerSideEncryptionAwsKmsKeyID       string          `json:"x-amz-server-side-encryption-aws-kms-key-id"`
	XAmzServerSideEncryptionCustomerAlgorithm string          `json:"x-amz-server-side-encryption-customer-algorithm"`
	XAmzWebsiteRedirectLocation               string          `json:"x-amz-website-redirect-location"`
	ACL                                       ACL             `json:"acl"`
	Key                                       string          `json:"key"`
	Location                                  Location        `json:"location"`
	IsDeleteMarker                            bool            `json:"isDeleteMarker"`
	Tags                                      Tags            `json:"tags"`
	ReplicationInfo                           ReplicationInfo `json:"replicationInfo"`
	DataStoreName                             string          `json:"dataStoreName"`
	LastModified                              time.Time       `json:"last-modified"`
	MdModelVersion                            int             `json:"md-model-version"`
	XAmzMetaUsermd                            string          `json:"x-amz-meta-usermd"`
}

/* check  is  value ovject is empty */
func (v Value ) IsEmpty() bool {
	return reflect.DeepEqual(Value{}, v)
}

type Contents struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}


func (v Value) PrintRepInfo(key string, log *log.Logger) {
	    repStatus := &v.ReplicationInfo.Status
		log.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", key, v.LastModified, v.ContentLength,*repStatus)
}