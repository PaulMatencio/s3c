package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"path/filepath"
	"strings"
)

var (
	listConfigCmd = &cobra.Command{
		Use:   "listConfig",
		Short: "list bucket info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			listConfig(cmd,args)
		},
	}
	topoLogy string
)

func init() {
	rootCmd.AddCommand(listConfigCmd)
	initaLcFlags(listConfigCmd)
}

func initaLcFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&topoLogy, "topoLogy", "i", ".admin/topology.json","path to the S3 metadata configuration file")
}


func listConfig(cmd *cobra.Command,args []string) {
	var (
		filePath string
		c  datatype.Clusters
		cluster,meta  string
	)

	if home, err := homedir.Dir(); err == nil {
		filePath = filepath.Join(home,topoLogy)
		if err,cl:= c.New(filePath); err == nil {
			for _,r := range cl.GetCluster() {
				for _,v := range r.GetRepds(){
					cluster = strings.Split(v.Name,"-")[1]
					meta = strings.Split(v.Name,"-")[0]
					fmt.Printf("Repd: %d\tCluster: %s\t Meta: %s\tHost: %s\tPort: %d\tSite: %s\tAdmin port: %d\n", r.Num, cluster,meta,v.Host,v.Port,v.Site,v.AdminPort)
				}
				for _,v := range r.GetWsbs() {
					cluster = strings.Split(v.Name,"-")[1]
					meta = strings.Split(v.Name,"-")[0]
					fmt.Printf("Wsb : %d\tCluster: %s\t Meta: %s\tHost: %s\tPort: %d\tSite: %s\tAdmin port: %d\n", r.Num, cluster,meta,v.Host,v.Port,v.Site,v.AdminPort)
				}
				fmt.Printf("\n")

			}
		} else {
			gLog.Error.Printf("%v",err)
		}
	} else {
		gLog.Error.Printf("Error opening topology file: %v - filepath",err)
	}
}

func getS3Host(topology string) *map[string]bool {

	s3Host := make(map[string]bool)
	if home, err := homedir.Dir(); err == nil {
		filePath = filepath.Join(home,topology)
		if err,c := c.New(filePath); err == nil {

			for _,r := range c.GetCluster() {
				for _,v := range r.GetRepds(){
					s3Host[v.Host] = true
				}
				for _,v := range r.GetWsbs(){
					s3Host[v.Host] = true
				}
			}
		} else {
			gLog.Error.Printf("%v",err)
		}
	} else {
		gLog.Error.Printf("Error opening topology file: %v - filepath",err)
	}
	return &s3Host

}

func PrettyPrint(i interface{}) (string) {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
