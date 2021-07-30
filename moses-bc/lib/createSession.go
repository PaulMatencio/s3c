package lib

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/viper"
)

func CreateS3Session(op string, location string) *s3.S3 {

	var (
		url       string
		accessKey string
		secretKey string
		region    string
		session   datatype.CreateSession
	)

	c := op + ".s3." + location + ".url"
	if url = viper.GetString(c); len(url) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file", c)))
		return nil
	}
	c = op + ".s3." + location + ".credential.access_key_id"
	if accessKey = viper.GetString(c); len(accessKey) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file", c)))
		return nil
	}
	c = op + ".s3." + location + ".credential.secret_access_key"
	if secretKey = viper.GetString(c); len(secretKey) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file", c)))
		return nil
	}

	// gLog.Info.Println(metaUrl,metaAccessKey,metaSecretKey)
	c = op + ".s3." + location + ".region"
	if region = viper.GetString(c);len(region) == 0{
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file", c)))
		return nil
	}

	gLog.Trace.Printf("Creating s3 session with following attributes: Region: %s - Endpoint: %s - Access key: %s - Secret key: %s",region,url,accessKey,secretKey)

	session = datatype.CreateSession{
		Region:    region,
		EndPoint:  url,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	return s3.New(api.CreateSession2(session))
}
