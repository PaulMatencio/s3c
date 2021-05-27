package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"os"
	"path/filepath"
	"strings"
)

func procStatResult(rd *datatype.Rh) {
    var versionId string
	if rd.Err != nil {
		procS3Error(rd.Err)
	} else {
		if len(odir) == 0 {
			if rd.Result.VersionId != nil {
				versionId = *rd.Result.VersionId
			}
			gLog.Trace.Printf("Key: %s - ContentLength: %v - LastModified: %v - Version Id: %v" ,rd.Key ,*rd.Result.ContentLength,*rd.Result.LastModified,versionId)
		}
		procS3Meta(rd.Key,rd.Result.Metadata)
	}
	rd = &datatype.Rh{}
}


func procS3Meta(key string, metad map[string]*string) {

	if len(odir) == 0 {
		if len(metad) >0 {
			utils.PrintUsermd(key, metad)
		}
	} else {
		if len(metad) > 0 {
			pathname := filepath.Join(pdir, strings.Replace(key, string(os.PathSeparator), "_", -1)+".md")
			utils.WriteUserMeta(metad, pathname)
		}
	}
}

func CheckBucket(svc *s3.S3, bucket string )(error) {
	var (
		req = datatype.StatBucketRequest{
			Service:  svc,
			Bucket: bucket,
		}
		_, err = api.StatBucket(req)
	)
	/* handle error */
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			gLog.Info.Printf("Bucket %s %v ",bucket,aerr.Code())
		} else {
			gLog.Error.Println(err.Error())
		}
	}
	return err
}

func procGetResult(rd *datatype.Ro) {

	if rd.Err != nil {
		procS3Error(rd.Err)
	} else {
		procS3Object(rd)
	}
	rd = &datatype.Ro{} // reset the request
}


func procPutResult(rd *datatype.Rp) {

	if rd.Err != nil {
		procS3Error(rd.Err)
	} else {
		gLog.Trace.Printf("file %s from %s has been sucessfully uploaded to bucket %s",rd.Key,rd.Idir, bucket)
	}

	rd = &datatype.Rp{}
}


func procS3Error(err error) {

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
		default:
			gLog.Error.Printf("error [%v]",aerr.Error())
		}
	} else {
		gLog.Error.Printf("[%v]",err.Error())
	}
}


func procS3Object(rd *datatype.Ro) {

	if len(odir) == 0 {

		utils.PrintUsermd(rd.Key,rd.Result.Metadata)
		b, err := utils.ReadObject(rd.Result.Body)
		if err == nil {
			gLog.Info.Printf("Key: %s - Content length: %d - Object length : %d", rd.Key, *rd.Result.ContentLength, b.Len())
		}

	} else {

		pathname := filepath.Join(pdir,strings.Replace(rd.Key,string(os.PathSeparator),"_",-1))

		if err := utils.SaveObject(rd.Result,pathname); err == nil {
			gLog.Trace.Printf("Object %s is downloaded  from %s to %s",key,bucket,pathname)
		} else {
			gLog.Error.Printf("Saving %s Error %v ",key,err)
		}

		if len(rd.Result.Metadata)  > 0 {
			pathname += ".md"
			utils.WriteUsermd(rd.Result.Metadata, pathname)
		}

	}
}

