package cmd

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

const (
	ISOLayout = "2006-01-02"
	dayToAdd = 7
)
var (
	lrishort = "Command to list object replication info in given bucket using levelDB API"
	lonshort = "Command to list objects between from <from-date> to <to-date> in a given bucket using levelDB API"
	lriCmd = &cobra.Command{
		Use:   "lsObjsRepInfo",
		Short: lrishort,
		Long: `Default Config file $HOME/.sc/config.yaml
        Example:
        sc  lisObjsRepInfo -b pxi-prod.00 -m  300 -maxLoop 10 -p <prefix-key> -m <marker>
        sc  -c <full path of config file> -b <bucket> -m <number> -maxLoop <number> --listMaster=false`,
		// Hidden: true,
		Run: ListObjRepInfo,
	}
	lriCmdh = &cobra.Command{
		Use:   "lsObjsRep",
		Short: lrishort,
		Long: `Default Config file $HOME/.sc/config.yaml
        Example:
        sc  lisObjsRepInfo -b pxi-prod.00 -m  300 -maxLoop 10 -p <prefix-key> -m <marker>
        sc  -c <full path of config file> -b <bucket> -m <number> -maxLoop <number> --listMaster=false`,
		Hidden: true,
		Run: ListObjRepInfo,
	}
	lonCmd = &cobra.Command {
		Use:   "lsObjsNew",
		Short: lonshort,
		Long: `Default Config file $HOME/.sc/config.yaml
        Example:
        sc  lisObjn -b pxi-prod.00 -m  300 -maxLoop 10 -p <prefix-key> -m <marker> 
        sc  -c <full path of config file> -b <bucket> -m <number> -maxLoop <number> --listMaster=false`,
		// Hidden: true,
		Run: ListObjNew,
	}
	done , listMaster, rBackend, count bool
	toDate ,endMarker string
	lastDate, frDate time.Time
	err error
)

func initLriFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().StringVarP(&marker,"marker","M","","start key processing from marker")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&listMaster,"listMaster","",true,"list the current version only")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cmd.Flags().BoolVarP(&done,"completed","",false,"print objects with COMPLETED/REPLICA status,by default only PENDING or FAILED are printed out ")
	cmd.Flags().BoolVarP(&rBackend,"rback","",false,"print report of both S3 metadata and backend replication info (sproxyd)")
	cmd.Flags().StringVarP(&toDate,"toDate","","","List replication info up to this given date <yyyy-mm-dd>")

}

func initLoNFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().StringVarP(&marker,"marker","M","","start key processing from this marker")
	cmd.Flags().StringVarP(&endMarker,"endMarker","","","stop key processing after this marker")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&listMaster,"listMaster","",true,"list the current version only")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cmd.Flags().StringVarP(&fromDate,"fromDate","","2000-01-01T00:00:00Z","clone objects after last modified from <yyyy-mm-ddThh:mm:ssZ>")
	cmd.Flags().StringVarP(&toDate,"toDate","","","List objects modified before date <yyyy-mm-dd>")
	cmd.Flags().BoolVarP(&count,"count","",true,"Count only the number of  objects between --fromDate to --toDate")

}

func init() {
	RootCmd.AddCommand(lriCmd)
	RootCmd.AddCommand(lriCmdh)
	RootCmd.AddCommand(lonCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLriFlags(lriCmd)
	initLoNFlags(lonCmd)
}

func ListObjRepInfo(cmd *cobra.Command,args []string) {

	var url string
	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
		gLog.Warning.Printf("levelDB url is missing")
		return
	}

    if len(toDate) > 0 {
		if lastDate, err = time.Parse(ISOLayout, toDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", toDate)
			return
		}
	} else {
		lastDate = time.Now().AddDate(0,0,dayToAdd)
	}
	gLog.Info.Printf("Counting objects from last modified date %v",lastDate)

	var (
		nextMarker string
		repStatus, backendStatus *string
		p,r,f,o,t,cl,ol,sl int64
		cp,cf,cc, skip int64
		compSize , othSize, skipSize float64
		req        = datatype.ListObjLdbRequest{
			Url:       url,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			ListMaster: listMaster,
			Delimiter: delimiter,
		}
		report Report

		s3Meta = datatype.S3Metadata{}
		N = 1 /* number of loop */
	)
	gLog.Info.Printf("%v",req)

	begin := time.Now()
	for {
		start := time.Now()
		if result, err := api.ListObjectLdb(req); err != nil {
			if err == nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result is empty")
			}
		} else {
			if err = json.Unmarshal([]byte(result.Contents), &s3Meta); err == nil {
				//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
				//num := len(s3Meta.Contentss3Meta.Contents)
				l := len(s3Meta.Contents)
				for _, c := range s3Meta.Contents {
					//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
					value := c.Value
					repStatus = &value.ReplicationInfo.Status
					if  value.LastModified.Before(lastDate) {
						t++
						switch *repStatus {
						case "PENDING":
							{
								p++
								value.PrintRepInfo(c.Key,gLog.Warning)
							}
						case "FAILED":
							{
								f++
								value.PrintRepInfo(c.Key,gLog.Warning)
							}
						case "COMPLETED":
							{
								r++
								backendStatus = &c.Value.ReplicationInfo.Backends[0].Status
								switch *backendStatus {
								case "PENDING":
									{
										cp++
									}
								case "COMPLETED":
									{
										cc++
										cl += int64(c.Value.ContentLength)
									}
								case "FAILED":
									{
										cf++
									}
								}
								if done {
									value.PrintRepInfo(c.Key,gLog.Info)
								}
							}
						case "REPLICA":
							{
								r++
								cl += int64(c.Value.ContentLength)
								if done {
									value.PrintRepInfo(c.Key,gLog.Info)
								}
							}
						default:
							o++
							ol += int64(c.Value.ContentLength)
						}
					} else {

						skip++  /* number of objecys skipped after last modified date > toDate */
						gLog.Info.Printf("Skip Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,c.Value.LastModified,c.Value.ContentLength,*repStatus)
					}
					value.PrintRepInfo(c.Key,gLog.Trace)

				}
				N++
				if l > 0 {
					//
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
				}
			} else {
				gLog.Info.Println(err)
			}
			compSize = float64(cl)/(1024.0*1024.0*1024.0)  //  expressed in GB
			othSize = float64(ol)/(1024.0*1024.0*1024.0)  //  expressed in GB
			skipSize = float64(sl)/(1024.0*1024.0*1024.0)

			/*
			report.Total= t
			report.ReportMeta.Completed=r
			report.ReportMeta.Pending =p
			report.ReportMeta.Failed = f
			report.ReportMeta.Other= o
			report.ReportBackend.Completed= cc
			report.ReportBackend.Pending= cp
			report.ReportBackend.Pending= cf
			*/

			report  = Report {
				Total: t,
				CompSize: compSize,
				OthSize: othSize,
				SkipSize: skipSize,
				ReportMeta: ReportNumber {
					Completed: r,
					Pending: p,
					Failed: f,
					Other: o,
			},
				ReportBackend: ReportNumber{
					Completed: cc,
					Pending: cp,
					Failed: cf,
				},
				Skipped : skip,
			}

			if !s3Meta.IsTruncated {
				report.Elapsed= time.Since(begin)
				report.Print(gLog.Warning,rBackend)
				return
			} else {
				// marker = nextMarker, nextMarker could contain Keyu00 if  bucket versioning is on
				Marker := strings.Split(nextMarker,"u00")
				req.Marker = Marker[0]
				report.Elapsed= time.Since(start)
				report.Print(gLog.Warning,rBackend)
			}
			if maxLoop != 0 && N > maxLoop {
				report.Elapsed= time.Since(begin)
				report.Print(gLog.Warning,rBackend)
				return
			}
		}
	}

}

func ListObjNew(cmd *cobra.Command,args []string) {

	var url string
	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
		gLog.Warning.Printf("levelDB url is missing")
		return
	}

	if len(toDate) > 0 {
		if lastDate, err = time.Parse(ISOLayout, toDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", toDate)
			return
		}
	} else {
		lastDate = time.Now().AddDate(0,0,dayToAdd)
	}

	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Wrong date format %s", frDate)
		return
	}

	gLog.Info.Printf("Counting objects from last modified date %v",frDate)

	var (
		nextMarker string
		req        = datatype.ListObjLdbRequest{
			Url:       url,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			ListMaster: listMaster,
			Delimiter: delimiter,
		}
		counter Counter
		s3Meta = datatype.S3Metadata{}
		N = 1 /* number of loop */
		t,n int64 = 0,0
	)
	gLog.Info.Printf("%v",req)
	begin:= time.Now()
	for {
		if result, err := api.ListObjectLdb(req); err != nil {
			if err == nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result is empty")
			}
		} else {
			if err = json.Unmarshal([]byte(result.Contents), &s3Meta); err == nil {
				l := len(s3Meta.Contents)
				for _, c := range s3Meta.Contents {
					value := c.Value
					if  value.LastModified.Before(lastDate) && value.LastModified.After(frDate){
						if !count {
							gLog.Info.Printf("Key:%s - Last modidied date: %v - Size: %d", c.Key, c.Value.LastModified, c.Value.ContentLength)
						}
						n++
					}
					t++
				}
				N++
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Warning.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
				}
			} else {
				gLog.Info.Println("Error passing content:",err)
			}
			counter = Counter{
				Elapsed: time.Since(begin),
				Date : frDate,
				Total : t,
				New : n,
				Marker : marker,
			}

			if !s3Meta.IsTruncated {
				counter.Print()
				return
			} else {
				Marker := strings.Split(nextMarker,"u00")
				req.Marker = Marker[0]
				if len(endMarker) > 0 && req.Marker[0:len(endMarker)] == endMarker {
					gLog.Info.Println("End marker matched- Req: %s / endMarker: %s",req.Marker,endMarker)
					counter.Print()
					return
				}
			}
			if maxLoop != 0 && N > maxLoop {
                counter.Print()
				return
			}
		}
	}

}

type Counter struct {
	Elapsed time.Duration
	Marker  string
	Date    time.Time
	Total    int64
	New      int64
}

func ( c Counter) Print() {
	gLog.Info.Printf("Elapsed time: %v - From Marker: %s - From Date: %v - Total number of objects listed: %d - Total number of new object: %d",c.Elapsed,c.Marker,c.Date,c.Total,c.New)

}

type Report struct {
	Elapsed time.Duration
	Total    int64
	CompSize      float64
	OthSize  float64
	SkipSize  float64
	ReportMeta	 ReportNumber
	ReportBackend  ReportNumber
	Skipped  int64
}

type ReportNumber struct {
	Pending  int64
	Failed   int64
	Completed int64
	Other     int64
}

func (r Report) Print (log *log.Logger,back bool) {
	if back {
		log.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d/size(GB):%.2f - skipped:%d/size(GB):%.2f",
			r.Elapsed, r.Total, r.ReportMeta.Pending,
			r.ReportMeta.Failed, r.ReportMeta.Completed,r.CompSize,
			r.ReportBackend.Completed,r.ReportBackend.Failed,r.ReportMeta.Other,r.OthSize,r.OthSize,r.Skipped,r.SkipSize)
	} else {
		log.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d/size(GB):%.2f - other:%d/size:%.2f - skipped:%d/size:%.2f",
			r.Elapsed, r.Total, r.ReportMeta.Pending,
			r.ReportMeta.Failed, r.ReportMeta.Completed,r.CompSize,
			r.ReportMeta.Other,r.OthSize,r.Skipped,r.SkipSize)
	}

}
