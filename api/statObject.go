package api
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	awsauth "github.com/smartystreets/go-aws-auth"
	"github.com/spf13/viper"
	"net/http"
)
func StatObject(req datatype.StatObjRequest) (*s3.HeadObjectOutput,error){

	input := &s3.HeadObjectInput{

		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}

	return req.Service.HeadObject(input)
}

/*
    authentication v2
    just for testing V2 authentication
*/

func StatObjectV2(request datatype.StatObjRequestV2) (*http.Response, error) {
	var (
		resp http.Response
		err error
		url = viper.GetString("s3.url")
	)
	// method := "GET "+ request.Bucket.
	method := "HEAD"
	gLog.Info.Printf("%s %s",url,method)

	url = url + "/"+request.Bucket+"/"+ request.Key
	if req, err := http.NewRequest(method, url, nil); err == nil {
		gLog.Info.Printf("%v",req)
		awsauth.Sign2(req) // Automatically chooses the best signing mechanism for the service
		if resp, err := request.Client.Do(req); err == nil {
			gLog.Info.Printf("%v",resp)
		} else {
			gLog.Error.Printf("%v",err)
		}

	} else {
		gLog.Error.Printf("%v",err)
	}
    return &resp,err


}

