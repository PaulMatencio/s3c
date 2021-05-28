package cmd

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Svc 	*s3.S3
	Bucket 	string
	Key 	string
}

type Response struct {
	Status	int
	Content string
	Usermd  string
	Err 	error
}


var (
	statObjbCmd = &cobra.Command{
		Use:   "headb",
		Short: "Retrieve S3 user metadata using Scality levelDB API",
		Long: `Retrieve S3 user metadata using Scality levelDB API
Example: sindexd headb -i pn -k AT/000648/U1,AT/000647/U3,AT/,FR/500004/A,FR/567812/A`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket);
					os.Exit(2)
				}
			}
			stat3b(cmd)
		},
	}
	statObjCmd = &cobra.Command{
		Use:   "head",
		Short: "Retrieve S3 user metadata using Amazon S3 SDK",
		Long: `Retrieve S3 user metadata using Amazon S3 SDK
Example: sindexd  head -i pn -k AT/000648/U1,AT/000647/U3,AT/,FR/500004/A,FR/567812/A`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket);
					os.Exit(2)
				}
			}
			stat3(cmd)
		},
	}
	keys string
	keya []string
	resp Response

)

func init() {
	rootCmd.AddCommand(statObjCmd)
	rootCmd.AddCommand(statObjbCmd)
	initStatbFlags(statObjCmd)
	initStatbFlags(statObjbCmd)
}


func initStatbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&keys,"keys","k","","list of input keys separated by commma")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","override the config file bucket prefix")
	cmd.Flags().StringVarP(&index,"index","i","pn","bucket group [pn|pd|bn]")
}

func stat3b(cmd *cobra.Command) {

	if len(keys) == 0 {
		usage(cmd.Name())
		return
	}
	keya := strings.Split(keys,",")
	if len(keya) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		for _, key := range keya {
			wg.Add(1)
			go func(key string, bucket string) {
				defer wg.Done()
				gLog.Trace.Printf("key: %s - bucket: %s",key, bucket)
			 	if resp := stat_3b(key); resp.Err == nil {
			 		if resp.Status == 200 {
						gLog.Info.Printf("Key %s - Usermd: %s", key, resp.Usermd)
					} else {
						gLog.Info.Printf("Key %s - status code %d", key, resp.Status)
					}
				} else {
					gLog.Error.Printf("key %s - Error: %v ",key,resp.Err)
				}
			}(key, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func stat3(cmd *cobra.Command) {
	var (
		keya = strings.Split(keys,",")
		resp Response
		err error
		svc   *s3.S3
	)

	if len(keys) == 0 {
		usage(cmd.Name())
		return
	}

	if len(keya) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		svc =  s3.New(api.CreateSession())
		for _, key := range keya {
			cc := strings.Split(key, "/")[0]
			if len(cc) != 2 {
				err = errors.New(fmt.Sprintf("Wrong country code: %s", cc))
				gLog.Error.Printf("key: %s - Error : %v",key,err)
			} else {
				if len(index) > 0 {
					buck = setBucketName(cc, bucket, index)
				} else {
					buck = bucket
				}
				wg.Add(1)
				go func(key string, bucket string) {
					defer wg.Done()
					gLog.Trace.Printf("key: %s - bucket: %s ", key, bucket)

					request := Request {
						Svc : svc,
						Bucket : bucket,
						Key :key,

					}
					if resp = stat_3(request); resp.Err == nil {
						gLog.Info.Printf("Key: %s - Usermd: %s\n", key, resp.Usermd)
					} else {
						gLog.Info.Printf("Key: %s - err: %v\n", key, resp.Err)
					}
				}(key, buck)
			}
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}


func stat_3b (key string) (Response) {
	var (
		err error
		buck,result string
		usermd []byte
		lvDBMeta = datatype.LevelDBMetadata{}
		resp Response
	)
	cc := strings.Split(key, "/")[0]
	if len(cc) != 2  {
		resp.Err =  errors.New(fmt.Sprintf("Wrong country code: %s", cc))
	} else {
		if len(index) >0 {
			buck = setBucketName(cc, bucket, index)
		} else {
			buck = bucket
		}
		req := Request {
			Svc :nil,
			Bucket : buck,
			Key : key,
		}

		resp = StatObjectLevelDB(req)

		if resp.Err == nil  {
			if resp.Status == 200 {
				if resp.Err = json.Unmarshal([]byte(resp.Content), &lvDBMeta); resp.Err == nil {
					/* lvDBMeta structure is defined is datatype.metadata.go

					 type LevelDBMetadata struct {
						Bucket BucketInfo `json:"bucket"`
						Object Value      `json:"obj,omitempty"`
					}
  				     IsEmpty is true if  LevelDBMetadata.Object: {} is empty -> return 404

					*/

					if !lvDBMeta.Object.IsEmpty() {
						m := &lvDBMeta.Object.XAmzMetaUsermd
						if usermd, resp.Err = base64.StdEncoding.DecodeString(*m); resp.Err == nil {
							resp.Usermd = string(usermd)
						} else {
							resp.Usermd = fmt.Sprintf("Key: %s - error: %v\n",key,err)
						}
					} else {
						resp.Status = 404
						// result = fmt.Sprintf("%s","Object not found")
					}
				}
			} else {
				resp.Usermd= fmt.Sprintf("Key: %s - Status code: %d\n",key,resp.Status)
				gLog.Warning.Printf("%s",result)
			}
		}
	}
	return resp
}


/* head request using AWS SDK  */
/* called by stat3  */
func stat_3 (req Request) ( Response) {
	var (
		resp = Response {
			Err: nil,
			Usermd: "",
		}
		usermd []byte
		head = datatype.StatObjRequest{
			Service: req.Svc,
			Bucket:  req.Bucket,
			Key:  req.Key,
		}
	)
	 if result,err := api.StatObject(head) ; err == nil {
		 if v, ok := result.Metadata["Usermd"]; ok {
			 if usermd, resp.Err = base64.StdEncoding.DecodeString(*v); resp.Err == nil {
				 gLog.Trace.Printf("key:%s - Usermd: %s", req.Key, string(usermd))
				 resp.Usermd = string(usermd)
			 }
		 } else {
			 resp.Usermd = fmt.Sprintf("Missing user metadata")
		 }
	 } else {
	 	resp.Err = err
	 }
	return resp
}

/*
    to be moved to api later
     api.StatObjectLevelDB

*/
func StatObjectLevelDB( req Request) (Response){
	/*
			build the request
		    curl -s '10.12.201.11:9000/default/parallel/<bucket>/<key>?verionId='
	*/
    var (
    	request = "/default/parallel/"+req.Bucket+"/"+req.Key+"?versionId="
    	resp = Response  {}
    )
	url := levelDBUrl+request
	if response,err := http.Get(url); err == nil {
		gLog.Trace.Printf("Request url : %s - Key %s - status code:  %d",url, req.Key,response.StatusCode)

		/*
		   should  return 200 or 50x
		   if an object does not exist , status code is 200
             the resp.Content  returns  { bucket= { } , object ={} }
		     the client must check is the returned object= {} is not empty
		*/

		resp.Err = err
		if response.StatusCode == 200 {
			defer response.Body.Close()
			if contents, err := ioutil.ReadAll(response.Body); err == nil {
				resp.Content = ContentToJson(contents)
				resp.Status = response.StatusCode
			} else {
				resp.Err = err /* Read fail */
			}
		} else {
			resp.Status = response.StatusCode
		}
	}  else {
		resp.Err = err
	}
	return resp
}

