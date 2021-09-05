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
	"encoding/base64"
	"encoding/json"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
	Check "clone" and "restore"
		Compare the cloned or restored objects with the source objects
*/

var (
	catchUpCmd = &cobra.Command{
		Use:   "catch-up",
		Short: "Command to copy missing sproxyd  objects",
		Long: `Command to copy missing sproxyd  objects`,
		Run: CatchUp,
	}
	levelDBUrl string
)

func initCatFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source S3 bucket without its suffix 00..05")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of documents (keys) to be backed up concurrently -Check --max-page for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent pages per document. check  --max-key for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&levelDBUrl, "levelDB-url", "", "http://10.147.68.133:9000", "levelDB Url - moses s3 index (directory)")

}

func init() {
	rootCmd.AddCommand(catchUpCmd)
	initCatFlags(catchUpCmd)
}

func CatchUp(cmd *cobra.Command, args []string) {

	var err error
	if err = mosesbc.SetSourceSproxyd("check", srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	if err = mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}



	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)
	gLog.Info.Printf("Target Env: %s - Target Driver: %s - Target Url: %s", sproxyd.TargetEnv, sproxyd.TargetDriver, sproxyd.TargetUrl)
	gLog.Info.Printf("Level DB URL: %s",levelDBUrl)

	if len(srcBucket) == 0 {
		gLog.Error.Printf("Source bucket is missing")
		return
	}

	//  check only documents metadata
	if len(prefix) == 0 {
		if !mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("if --prefix is missing then please provide a suffix [00..05]to the bucket %s", srcBucket)
			return
		}
	} else {
		if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}
	}

	var (
		client    = &http.Client{}
		transport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(CONTIMEOUT) * time.Millisecond, // connection timeout
				KeepAlive: time.Duration(KEEPALIVE) * time.Millisecond,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
			// MaxIdleConnsPerHost: 100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	)
	client.Transport = transport
	CatchUpSproxyd(client,srcBucket)

}

func CatchUpSproxyd(client *http.Client,bucket string) {
	var (
		s3Meta = datatype.S3Metadata{}
		nextMarker string
		N          = 0
	)
	for {
		if err, result := listS3bPref(client,bucket, prefix, marker); err != nil || len(result) == 0 {
			if err != nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result is empty")
			}
		} else {

			if err = json.Unmarshal([]byte(result), &s3Meta); err == nil {
				l := len(s3Meta.Contents)
				wg1 := sync.WaitGroup{}
				for _, c := range s3Meta.Contents {
					//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
					m := &c.Value.XAmzMetaUsermd
					usermd, _ := base64.StdEncoding.DecodeString(*m)
					// gLog.Info.Printf("Key: %s - Usermd: %s", c.Key, string(usermd))
					s3meta := meta.UserMd{}  //  moses s3 index
					if err = json.Unmarshal(usermd,&s3meta); err == nil {
						wg1.Add(1)
						go func(s3meta *meta.UserMd) {
							gLog.Info.Printf("CheckTargetPages( %s, %d, %d)", s3meta.DocID, s3meta.TotalPages, maxPage)
							wg1.Done()
						} (&s3meta)

					}  else {
						gLog.Error.Printf("%v",err)
					}

				}
				wg1.Wait()
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker, s3Meta.IsTruncated)
				}
				N++
			} else {
				gLog.Info.Println(err)
			}
		}
		if !s3Meta.IsTruncated {
			return
		} else {
			marker = nextMarker
			gLog.Info.Printf("marker %s", marker)
		}
		if N >= maxLoop {
			return
		}
	}

}

func listS3bPref(client *http.Client,bucket string, prefix string, marker string) (error, string) {

	var (
		err      error
		result   string
		contents []byte
		delim    string
	)


	request := "/default/bucket/" + bucket + "?listingType=DelimiterMaster&prefix="
	limit := "&maxKeys=" + strconv.Itoa(int(maxKey))

	if len(delimiter) > 0 {
		delim = "&delimiter=" + delimiter
	}

	keyMarker := "&marker=" + marker
	// url := Host +":"+Port+request+prefix+limit+keyMarker+delim
	url := levelDBUrl + request + prefix + limit + keyMarker + delim
	gLog.Info.Println("URL:", url)
	if response, err := client.Get(url); err == nil {
		if response.StatusCode == 200 {
			defer response.Body.Close()
			if contents, err = ioutil.ReadAll(response.Body); err == nil {
				return err, ContentToJson(contents)
			}
		} else {
			gLog.Warning.Printf("Get url %s - Http status: %d", url, response.Status)
		}
	}
	return err, result
}

// transform content returned by the bucketd API into JSON string
func ContentToJson(contents []byte) string {
	result := strings.Replace(string(contents), "\\", "", -1)
	result = strings.Replace(result, "\"{", "{", -1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result, "\"}\"}", "\"}}", -1)
	result = strings.Replace(result, "}\"", "}", -1)
	gLog.Trace.Println(result)
	return result
}
