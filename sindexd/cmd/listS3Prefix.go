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
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type UserMd struct {
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

var (
	listS3Cmd = &cobra.Command{
		Use:   "ls3",
		Short: "List S3 metadata with prefix using the AMAZON S3 SDK API ",
		Long: `List S3 metadata with prefix using the AMAZON S3 SDK API
               Examples:
               sindexd ls3 -i pd -p US/2020/ -m 50  (List the first 50 objects in the S3 virtual folder US/2020/) 
               sindexd ls3 -i pd -p US/2020/ -d /  -m 20 (List the first 20  S3 virtual folders below US/2020)
               sindexd ls3 -i pd -p US -k US/2020/01 -m 20 (List the US virtual folder starting at virtual folder US/2020/01)
               sindexd ls3 -i pd -p US/2020/,CN/2019/01/,JP/2019/12/ -d / ( list virtual folders of the given virtual folders
               sindexd ls3 -i pn -p US/63,FR/5000,GB/725 -m 50  ( list all objects with the given prefixes)`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket)
					os.Exit(2)
				}
			}

			listS3(cmd, args)
		},
	}
	listS3Cmdb = &cobra.Command{
		Use:   "ls3b",
		Short: "List S3 metadata with prefix using the Scality levelDB API",
		Long: `List S3 metadata with prefix using the Scality levelDB API
               Examples:
               sindexd ls3b -i pd -p US/2020/ -m 50  (List the first 50 objects in the S3 virtual folder US/2020/) 
               sindexd ls3b -i pd -p US/2020/ -d /  -m 20 (List the first 20  S3 virtual folders below US/2020)
               sindexd ls3b -i pd -p US -k US/2020/01 -m 20 (List the US virtual folder starting at virtual folder US/2020/01)
               sindexd ls3b -i pd -p US/2020/,CN/2019/01/,JP/2019/12/ -d / ( list virtual folders of the given virtual folders
               sindexd ls3b -i pn -p US/63,FR/5000,GB/725 -m 50  ( list all objects with the given prefixes`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket)
					os.Exit(2)
				}
			}
			listS3b(cmd, args)
		},
	}

	checkS3Cmd = &cobra.Command{
		Use:   "checkS3",
		Short: "Check S3 metadata with prefix using the Scality levelDB API",
		Long: `Check S3 metadata with prefix using the Scality levelDB API
               Examples:
               sindexd ls3b -i pd -p / -m 100`,

		Run: func(cmd *cobra.Command, args []string) {
			if len(bucket) == 0 {
				gLog.Error.Printf("Missing bucket ")
				return
			}
			checkS3(cmd, args)
		},
	}
	prefixs, buck, index string
	prefixa              []string
	maxS3Key, total      int64
	loop                 int
)

func init() {
	rootCmd.AddCommand(listS3Cmd)
	initListS3Flags(listS3Cmd)
	rootCmd.AddCommand(listS3Cmdb)
	initListS3Flags(listS3Cmdb)
	rootCmd.AddCommand(checkS3Cmd)
	initCheckS3Flags(checkS3Cmd)
}

func initListS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&prefixs, "prefix", "p", "", "prefix of the keys separated by a commma")
	cmd.Flags().StringVarP(&marker, "marker", "k", "", "Start with this marker (Key) for the Get Prefix")
	cmd.Flags().Int64VarP(&maxS3Key, "maxKey", "m", 20, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "delimiter character")
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the S3  bucket")
	cmd.Flags().IntVarP(&loop, "loop", "L", 1, "Number of loop using the next marker if there is one")
	cmd.Flags().StringVarP(&index, "index", "i", "pn", "bucket group [pn|pd|bn]")
}

func initCheckS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&prefixs, "prefix", "p", "", "prefix of the keys separated by a commma")
	cmd.Flags().StringVarP(&marker, "marker", "k", "", "Start with this marker (Key) for the Get Prefix")
	cmd.Flags().Int64VarP(&maxS3Key, "maxKey", "m", 20, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the S3  bucket")
	// cmd.Flags().IntVarP(&loop, "loop", "L", 1, "Number of loop using the next marker if there is one")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
}

func listS3(cmd *cobra.Command, args []string) {
	var (
		prefixa    = strings.Split(prefixs, ",")
		nextmarker string
		err        error
	)

	if len(prefixs) == 0 {
		usage(cmd.Name())
		return
	}

	if len(prefixa) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(prefixa))
		for _, prefix := range prefixa {
			go func(prefix string, bucket string) {
				defer wg.Done()
				gLog.Info.Println(prefix, bucket)
				if len(delimiter) > 0 {
					if nextmarker, err = listS3CommonPrefix(prefix, marker, bucket); err != nil {
						gLog.Error.Println(err)
					}
				} else {
					if nextmarker, err = listS3Pref(prefix, marker, bucket); err != nil {
						gLog.Error.Println(err)
					}
				}
			}(prefix, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

/* S3 API list user metadata  function */
func listS3Pref(prefix string, marker string, bucket string) (string, error) {

	var (
		nextmarker string
		N          int
		cc         = strings.Split(prefix, "/")[0]
	)

	// buck = bucket + "-" + index
	if len(cc) != 2 {
		return nextmarker, errors.New(fmt.Sprintf("Wrong country code: %s", cc))
	} else {

		if len(index) > 0 {
			buck = setBucketName(cc, bucket, index)
		} else {
			buck = bucket
		}
		req := datatype.ListObjRequest{
			Service:   s3.New(api.CreateSession()),
			Bucket:    buck,
			Prefix:    prefix,
			MaxKey:    maxS3Key,
			Marker:    marker,
			Delimiter: delimiter,
		}
		for {
			var (
				// nextmarker string
				result *s3.ListObjectsOutput
				err    error
			)
			N++
			if result, err = api.ListObject(req); err == nil {
				gLog.Info.Println(cc, buck, len(result.Contents))

				if l := len(result.Contents); l > 0 {
					total += int64(l)
					var wg1 sync.WaitGroup
					wg1.Add(len(result.Contents))
					for _, v := range result.Contents {
						gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
						svc := req.Service
						head := datatype.StatObjRequest{
							Service: svc,
							Bucket:  req.Bucket,
							Key:     *v.Key,
						}
						go func(request datatype.StatObjRequest) {
							rh := datatype.Rh{
								Key: head.Key,
							}
							defer wg1.Done()
							rh.Result, rh.Err = api.StatObject(head)
							utils.PrintUsermd(rh.Key, rh.Result.Metadata)
						}(head)
					}
					if *result.IsTruncated {
						nextmarker = *result.Contents[l-1].Key
						gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
					}
					wg1.Wait()
				}
			} else {
				gLog.Error.Printf("%v", err)
				break
			}

			if N < loop && *result.IsTruncated {
				req.Marker = nextmarker
			} else {
				gLog.Info.Printf("Total number of objects returned: %d", total)
				break
			}
		}
	}
	return nextmarker, nil
}

func listS3CommonPrefix(prefix string, marker string, bucket string) (string, error) {

	var (
		nextmarker string
		N          int
		cc         = strings.Split(prefix, "/")[0]
		buck       string
	)
	if len(cc) == 2 {
		buck = setBucketName(cc, bucket, index)
	} else {
		buck = bucket
	}

	gLog.Trace.Printf("bucket %s - Prefix %s - Delimiter %s - Maxkey %d ", buck, prefix, delimiter, maxS3Key)
	req := datatype.ListObjRequest{
		Service:   s3.New(api.CreateSession()),
		Bucket:    buck,
		Prefix:    prefix,
		MaxKey:    maxS3Key,
		Marker:    marker,
		Delimiter: delimiter,
	}

	for {
		var (
			result *s3.ListObjectsOutput
			err    error
		)
		N++
		if result, err = api.ListObject(req); err == nil {
			gLog.Info.Println(cc, buck, len(result.Contents))
			if l := len(result.Contents); l > 0 {
				total += int64(l)
				for _, v := range result.Contents {

					gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
			}
			// list the common prefixes
			if len(result.CommonPrefixes) > 0 {
				gLog.Info.Println("List Common prefix:")
				for _, v := range result.CommonPrefixes {
					gLog.Info.Printf("%s", *v.Prefix)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		if N < loop && *result.IsTruncated {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}
	return nextmarker, nil
}

/* level DB API list function */
func listS3b(cmd *cobra.Command, args []string) {

	if len(prefixs) == 0 {
		usage(cmd.Name())
		return
	}
	prefixa = strings.Split(prefixs, ",")
	if len(prefixa) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(prefixa))

		for _, prefix := range prefixa {
			go func(prefix string, bucket string) {
				defer wg.Done()
				var (
					s3Meta = datatype.S3Metadata{}
					// marker string
					nextMarker string
					N          = 0
				)
				for {
					if err, result := listS3bPref(prefix, marker); err != nil || len(result) == 0 {
						if err != nil {
							gLog.Error.Println(err)
						} else {
							gLog.Info.Println("Result is empty")
						}
					} else {

						if err = json.Unmarshal([]byte(result), &s3Meta); err == nil {
							//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
							//num := len(s3Meta.Contentss3Meta.Contents)
							l := len(s3Meta.Contents)
							for _, c := range s3Meta.Contents {
								//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
								m := &c.Value.XAmzMetaUsermd
								usermd, _ := base64.StdEncoding.DecodeString(*m)
								gLog.Info.Printf("Key: %s - Usermd: %s", c.Key, string(usermd))
							}
							//* print  common prefix if any
							ListCommonPrefix(s3Meta.CommonPrefixes)
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
					if N >= loop {
						return
					}
				}

			}(prefix, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func listS3bPref(prefix string, marker string) (error, string) {
	var (
		err          error
		result, buck string
		contents     []byte
		delim        string
	)
	cc := strings.Split(prefix, "/")[0]
	if len(cc) != 2 && len(delimiter) == 0 {
		err = errors.New(fmt.Sprintf("Wrong country code: %s", cc))
	} else {
		if len(index) > 0 {
			buck = setBucketName(cc, bucket, index)
		} else {
			buck = bucket
		}
		/*
				build the request
			    curl -s '10.12.201.11:9000/default/bucket/moses-meta-02?listingType=DelimiterMaster&prefix=FR/&maxKeys=2&delimiter=/'
				 curl -s '10.12.201.11:9000/default/bucket/moses-meta-02?listingType=DelimiterMaster&prefix=FR/&maxKeys=2&delimiter=/'
		*/
		request := "/default/bucket/" + buck + "?listingType=DelimiterMaster&prefix="
		limit := "&maxKeys=" + strconv.Itoa(int(maxS3Key))

		if len(delimiter) > 0 {
			delim = "&delimiter=" + delimiter
		}

		keyMarker := "&marker=" + marker
		// url := Host +":"+Port+request+prefix+limit+keyMarker+delim
		url := levelDBUrl + request + prefix + limit + keyMarker + delim
		gLog.Info.Println("URL:", url)
		if response, err := http.Get(url); err == nil {
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err = ioutil.ReadAll(response.Body); err == nil {
					return err, ContentToJson(contents)
				}
			} else {
				gLog.Warning.Printf("Get url %s - Http status: %d", url, response.Status)
			}
		}
	}
	return err, result
}

func ListCommonPrefix(cp []interface{}) {
	gLog.Info.Println("List Common prefix:")
	for _, p := range cp {
		gLog.Info.Printf("Common prefix %s", p)
	}
}

/* level DB API list function */
func checkS3(cmd *cobra.Command, args []string) {

	if len(prefixs) == 0 {
		usage(cmd.Name())
		return
	}
	prefixa = strings.Split(prefixs, ",")
	if len(prefixa) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(prefixa))

		for _, prefix := range prefixa {
			go func(prefix string, bucket string) {
				defer wg.Done()
				var (
					s3Meta = datatype.S3Metadata{}
					// marker string
					nextMarker string
					N          = 0
				)
				for {
					if err, result := listS3cPref(prefix, marker); err != nil || len(result) == 0 {
						if err != nil {
							gLog.Error.Println(err)
						} else {
							gLog.Info.Println("Result is empty")
						}
					} else {
						if err = json.Unmarshal([]byte(result), &s3Meta); err == nil {
							//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
							//num := len(s3Meta.Contentss3Meta.Contents)
							l := len(s3Meta.Contents)
							for _, c := range s3Meta.Contents {
								//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
								m := &c.Value.XAmzMetaUsermd
								usermd, _ := base64.StdEncoding.DecodeString(*m)
								userm := UserMd{}
								if err := json.Unmarshal([]byte(usermd), &userm); err != nil {
									gLog.Error.Printf("Key %s  has invalid  User metadata %s",c.Key,string(usermd))
								}
							}
							//* print  common prefix if any
							ListCommonPrefix(s3Meta.CommonPrefixes)
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
					if maxLoop != 0  && N >= maxLoop {
						return
					}
				}

			}(prefix, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func listS3cPref(prefix string, marker string) (error, string) {
	var (
		err      error
		result   string
		contents []byte
		delim    string
	)

	/*
			build the request
		    curl -s '10.12.201.11:9000/default/bucket/moses-meta-02?listingType=DelimiterMaster&prefix=FR/&maxKeys=2&delimiter=/'
			 curl -s '10.12.201.11:9000/default/bucket/moses-meta-02?listingType=DelimiterMaster&prefix=FR/&maxKeys=2&delimiter=/'
	*/
	request := "/default/bucket/" + bucket + "?listingType=DelimiterMaster&prefix="
	limit := "&maxKeys=" + strconv.Itoa(int(maxS3Key))

	if len(delimiter) > 0 {
		delim = "&delimiter=" + delimiter
	}

	keyMarker := "&marker=" + marker
	// url := Host +":"+Port+request+prefix+limit+keyMarker+delim
	url := levelDBUrl + request + prefix + limit + keyMarker + delim
	gLog.Info.Println("URL:", url)
	if response, err := http.Get(url); err == nil {
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
