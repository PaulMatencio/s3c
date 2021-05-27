package st33

import (
	"fmt"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/aws/aws-sdk-go/service/s3"
	"strconv"
	"time"
)

func CopyObject(key string,  svc *s3.S3, frombucket string, tobucket string) (error) {

	KEYx := key + ".1"

	req := datatype.CopyObjRequest{
		Service: svc,
		Sbucket: frombucket,
		Tbucket: tobucket,
		Skey:    KEYx,
		Tkey:    KEYx,
	}

	if resp, err := api.CopyObject(req); err == nil {
		gLog.Trace.Printf("Object Key: %s has been copied - New ETag: %s ", key, *resp.CopyObjectResult.ETag)
		return err
	} else {
		gLog.Error.Printf("Error copying %s %v",key,err)
		return err
	}

}


//
// Copy PXI's St33 images
//
// Input
//		Key : PXI id
//		Pages: Number of pages to be copied
//      svc : S3 service
//

func CopyObjects(key string, pages int,svc *s3.S3, frombucket string, tobucket string ) {

	var (
		N = pages
		T = 0
		ch  = make(chan *datatype.Rc)
	)

	for p:=1 ; p <= pages; p++ {

		KEYx := key + "." + strconv.Itoa(p)
		go func(KEYx string) {

			req := datatype.CopyObjRequest {
				Service: svc,
				Sbucket:  frombucket,
				Tbucket:  tobucket,
				Skey:     KEYx,
				Tkey:     KEYx,
			}

			resp, err := api.CopyObject(req)

			ch <- &datatype.Rc {
				Key: KEYx,
				Result: resp,
				Err: err,
			}

			req = datatype.CopyObjRequest{}

		}(KEYx)
	}

	for  {
		select {
		case rc := <-ch:
			T++
			if rc.Err == nil {
				gLog.Trace.Printf("Object Key: %s has been copied - New ETag: %s ", rc.Key, *rc.Result.CopyObjectResult.ETag)
			} else {
				gLog.Error.Printf("Error copying object key %s: %v",rc.Key,rc.Err)
			}
			if T == N {
				gLog.Info.Printf("PxiId %s - %d objects was copied from bucket %s  to bucket %s",utils.Reverse(key),N+1,frombucket,tobucket)
				return
			}
		case <-time.After(500 * time.Millisecond):
			fmt.Printf("w")
		}
	}
}
