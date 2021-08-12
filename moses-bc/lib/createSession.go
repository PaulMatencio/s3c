// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
