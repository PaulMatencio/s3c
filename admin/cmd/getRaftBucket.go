
package cmd

import (
	"fmt"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net"
	"net/http"
	"time"
)

var (
	getRaftBucketCmd = &cobra.Command{
		Use:   "getRaftBucket",
		Short: "Get Raft Bucket Info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			getRaftBucket(cmd,args)
		},
	}
	grbiCmd = &cobra.Command{
		Use:   "grbi",
		Short: "Get Raft Bucket Info",
		Long: ``,
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			getRaftBucket(cmd,args)
		},
	}

)
func init() {
	rootCmd.AddCommand(getRaftBucketCmd)
	rootCmd.AddCommand(grbiCmd)
	initGrbFlags(getRaftBucketCmd)
	initGrbFlags(grbiCmd)
}

func initGrbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&url,"url","u","","bucketd url <htp://ip:port>")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","bucket name")
}


func getRaftBucket(cmd *cobra.Command, args []string){

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

	if len(url) == 0 {
		if url = utils.GetBucketdUrl(*viper.GetViper()); len(url) == 0 {
			if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
				gLog.Warning.Printf("The url of metadata server is missing")
				return
			}
		}
	}

	gLog.Info.Printf("Url: %s",url)

	if err,rb := api.GetRaftBucket(client,url,bucket); err == nil {
		printBucket(*rb)
		if err,rs := api.GetRaftSession(client,url,rb.RaftSessionID); err == nil {
			fmt.Printf("Leader\n")
			printMember(rs.Leader)
			fmt.Printf("Connected\n")
			for _,m := range rs.Connected {
				printMember(m)
			}
			fmt.Printf("Disconnected\n")
			for _,m := range rs.Disconnected {
				printMember(m)
			}
		}
	} else {
		fmt.Printf("%v\n",err)
	}
}

func printBucket(rb datatype.RaftBucket) {
	fmt.Printf("Bucket:\t%s", bucket)
	fmt.Printf("\tSession ID:%d",rb.RaftSessionID)
	fmt.Printf("\tLeader IP:%s\tPort:%d\n",rb.Leader.IP,rb.Leader.Port)
}


func printMember(v datatype.RaftMembers){
	fmt.Printf("\tId: %d\tName: %s\tHost: %s\tPort: %d\tSite: %s\tAdmin port: %d\n", v.ID, v.Name, v.Host, v.Port, v.Site,v.AdminPort)
}