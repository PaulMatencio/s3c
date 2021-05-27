
package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	URL "net/url"
	"path/filepath"
	"strings"
)

var (
	healthCheckCmd = &cobra.Command{
		Use:   "healthCheck",
		Short: "Health Checker",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			healthCheck(cmd,args)
		},
	}

	hcCmd = &cobra.Command{
		Use:   "hc",
		Short: "Health Checker",
		Long: ``,
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			healthCheck(cmd,args)
		},
	}

	PORT  string

)
func init() {
	rootCmd.AddCommand(healthCheckCmd)
	rootCmd.AddCommand(hcCmd)
	initHcFlags(healthCheckCmd)
	initGrbFlags(hcCmd)
}

func initHcFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&url,"url","u","","bucketd url <htp://ip:port>")
	cmd.Flags().StringVarP(&topoLogy, "topoLogy", "i", ".admin/topology.json","path to the S3 metadata configuration file")
}

func healthCheck(cmd *cobra.Command,args []string) {

	var cl datatype.Clusters
	if len(url) == 0 {
		if url = utils.GetBucketdUrl(*viper.GetViper()); len(url) == 0 {
			if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
				gLog.Warning.Printf("The url of metadata server is missing")
				return
			}
		}
	}
	/* check URL validity */
	if U, err := URL.Parse(url); err != nil {
		gLog.Error.Printf("Invalid URL, valid syntax URL =>  http://<ip>:<port>")
		return

	} else {
		HOST = U.Host
		PORT = U.Port()
		SUBNET = strings.Split(HOST, ".")[2]
	}
	if home, err := homedir.Dir(); err == nil {
		filePath = filepath.Join(home, topoLogy)

		if err, c := cl.New(topology); err == nil {
			if err, s3Host := c.GetHost(); err == nil {
				for host, _ := range *s3Host {
					if strings.Split(host, ".")[2] != SUBNET {
						gLog.Warning.Printf("Wrong toplogy file: %s\n", filePath)
						return
					}
					checkHealth(host,PORT)
				}
			}
		} else {
			gLog.Error.Println(err)
		}
	} else {
		gLog.Error.Println(err)
	}
}


func checkHealth(host string,port string ){
	url := http + host +":" + port
	fmt.Printf("Host:\t%s\n",host)
	if err,hc := api.HeathCheck(url); err == nil {
		fmt.Printf("Sproxyd:\tCode:%s\tMessage:%s\n",hc.Sproxyd.Code,hc.Sproxyd.Message)
		fmt.Printf("Bucket-Client:\tCode:%s\tMessage:%s\tBody:%v\n",hc.Bucketclient.Code,hc.Bucketclient)
		fmt.Printf("Sproxyd:\tCode:%s\tMessage:%s\n",hc.Sproxyd.Code,hc.Sproxyd.Message)
	}
}

