// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// listObjectCmd represents the listObject command

var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Command to compare Moses objects",
		Long:  `Check a restored blobs against its source blobs`,
		Run:   check,
	}

	np, status, srcUrl, targetUrl string
	err                           error
	driver, targetDriver         string

)

func initCkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&pn, "pn", "k", "", "Publication number(document key)")
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the metadata bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 40, "maximum number of documents (keys) to be backed up concurrently -Check --maxpage for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "prefix  delimiter")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages per document. check  --maxKey  for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-url", "s", "http://10.12.202.10:81/proxy,http://10.12.202.20:81/proxy,", "source URL http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-driver", "", "bpchord", "source driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-driver", "", "bparc", "target driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetUrl, "target-url", "t", "http://10.12.201.170:81/proxy,http://10.12.201.173:81/proxy", "target URL http://xx.xx.xx.xx:81/proxy,http:// ...")

}
func init() {
	rootCmd.AddCommand(checkCmd)
	initCkFlags(checkCmd)
}

func check(cmd *cobra.Command, args []string) {

	if len(pn) > 0 {
		chekBlob1(pn)
	} else if len(prefix) > 0 {
		if len(bucket) == 0 {
			gLog.Error.Printf("Metadata bucket is missing")
			return
		}
		fmt.Printf("Bucket %s  prefix %s marker %s maxKey %d  maxPage %d maxLoop %d",bucket,prefix,marker,maxKey,maxPage,maxLoop)
		checkBlobs(bucket,prefix,marker,maxKey,maxPage,maxLoop)
	} else {
		gLog.Error.Printf("Both publication number (pn)  and  prefix are missing")
		return
	}
}

func chekBlob1(pn string) {

	setSproxydHost()
	if np, err, status := mosesbc.GetPageNumber(pn); err == nil && status == 200 {
		if np > 0 {
			mosesbc.CheckBlob1(pn, np, maxPage)
		} else {
			gLog.Error.Printf("The number of pages is %d ", np)
		}
	} else {
		gLog.Error.Printf("Error %v getting  the number of pages  run  with  -l 4  (trace)", err)
	}
}

func checkBlobs(bucket string, marker string,prefix string,maxKey int64,maxPage int,maxLoop int) {

	setSproxydHost()

	if metaUrl = viper.GetString("meta.s3.url"); len(metaUrl) == 0 {
		gLog.Error.Println(errors.New(missingMetaurl))
		return
	}

	if metaAccessKey = viper.GetString("meta.credential.access_key_id"); len(metaAccessKey) == 0 {
		gLog.Error.Println(errors.New(missingMetaak))
		return
	}

	if metaSecretKey = viper.GetString("meta.credential.secret_access_key"); len(metaSecretKey) == 0 {
		gLog.Error.Println(errors.New(missingMetask))
		return
	}
	gLog.Info.Println(metaUrl,metaAccessKey,metaSecretKey)
	meta = datatype.CreateSession{
		Region:    viper.GetString("meta.s3.region"),
		EndPoint:  metaUrl,
		AccessKey: metaAccessKey,
		SecretKey: metaSecretKey,
	}

	svcm = s3.New(api.CreateSession2(meta))

	request := datatype.ListObjRequest{
		Service : svcm,
		Bucket:    bucket,
		Prefix:    prefix,
		MaxKey:    maxKey,
		Marker:    marker,
		// Delimiter: delimiter,
	}
	mosesbc.CheckBlobs(request, maxLoop,maxPage)
}

func setSproxydHost() {
	/*

	 */
	if len(srcUrl) > 0 {
		sproxyd.Url = srcUrl
	} else {
		gLog.Error.Printf("Source URL is missing")
		return
	}
	if len(targetUrl) > 0 {
		sproxyd.TargetUrl = targetUrl
	} else {
		gLog.Error.Printf("Target URL is missing")
		return
	}
	if len(driver) > 0 {
		sproxyd.Driver = driver
	} else {
		gLog.Error.Printf("Source driver is missing")
		return
	}

	if len(targetDriver) > 0 {
		sproxyd.TargetDriver = targetDriver
	} else {
		gLog.Error.Printf("Target URL is missing")
		return
	}

	sproxyd.SetNewProxydHost1(srcUrl, driver)
	sproxyd.SetNewTargetProxydHost1(targetUrl, targetDriver)
	gLog.Trace.Printf("Source Host Pool: %v - Source Env: %s - Source Driver: %s", sproxyd.HP.Hosts(), sproxyd.Env, sproxyd.Driver)
	gLog.Trace.Printf("Target Host Pool: %v -  Source Env: %s - Source Driver: %s", sproxyd.TargetHP.Hosts(), sproxyd.TargetEnv, sproxyd.TargetDriver)

}