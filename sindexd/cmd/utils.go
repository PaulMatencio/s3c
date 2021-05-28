package cmd

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	directory "github.com/paulmatencio/ring/directory/lib"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"strings"
	"sync"
)

// transform content returned by the bucketd API into JSON string
func ContentToJson(contents []byte ) string {
	result:= strings.Replace(string(contents),"\\","",-1)
	result = strings.Replace(result,"\"{","{",-1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result,"\"}\"}","\"}}",-1)
	result = strings.Replace(result,"}\"","}",-1)
	gLog.Trace.Println(result)
	return result
}


func GetUsermd(req datatype.ListObjRequest , result *s3.ListObjectsOutput, wg sync.WaitGroup){

	for _, v := range result.Contents {
		gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size,v.LastModified)
		svc := req.Service
		head := datatype.StatObjRequest{
			Service: svc,
			Bucket:  req.Bucket,
			Key:     *v.Key,
		}
		go func(request datatype.StatObjRequest) {
			rh := datatype.Rh{
				Key : head.Key,
			}
			defer wg.Done()
			rh.Result, rh.Err = api.StatObject(head)
			//procStatResult(&rh)
			utils.PrintUsermd(rh.Key, rh.Result.Metadata)
		}(head)
	}
}

func usage(cmdname string) {
	gLog.Info.Printf("sindexd %s -h for more options",cmdname)

}

type CCidx struct {
	CC string
	Idx string
}

type S3Oper struct {
	Oper string
	Bucket string
	Key string
	Value string
}

func parseSindexdLog(linei string,idxMap map[string]CCidx, bucket string,bucketNumber int) (error,S3Oper){

	var (
		value,buck string
		line = strings.Split(linei, " ")
		oper = line[3]
		idxId = line[5]
		key = line[7]
		err error
	)
	gLog.Trace.Println(linei)
	switch(oper){
	case "ADD": {
			value= line[9]
	}
	case "DELETE": {
		value=""
	}
	default:
		gLog.Error.Printf("wrong operation %s in line %s",oper,linei)
	}

	if key[0:2] == "20" {
		buck = bucket+"-last-loaded"
	} else {
		if v, ok := idxMap[idxId]; ok {
			cc := v.CC
			if cc == "NP" {
				cc = "XP"
			}
			buck = setBucketName(cc, bucket, v.Idx)
			err =nil
		} else {
			err = errors.New(fmt.Sprintf("Index-id %s is not found in the indexes Map",idxId))
		}
	}
	return err,S3Oper {
		Oper: oper,
		Bucket : buck,
		Key: key,
		Value: value,
	}
}

func buildIdxMap() (map[string]CCidx){

	map1 := directory.GetCountrySpec(directory.GetIndexSpec("PN") )
	map2 := directory.GetCountrySpec(directory.GetIndexSpec("PD"))
	map3 := directory.GetCountrySpec(directory.GetIndexSpec("BN"))

	var idxMap = make(map[string]CCidx)
	for k,v := range map1 {
		idxMap[k] = CCidx{CC: v,Idx:"PN"}
	}
	for k,v := range map2 {
		idxMap[k] = CCidx{CC: v,Idx:"PD"}
	}
	for k,v := range map3 {
		idxMap[k] = CCidx{CC: v,Idx:"BN"}
	}

	return idxMap
}
