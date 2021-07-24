
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
	gLog "github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/cobra"
	"net/url"
)

// listObjectCmd represents the listObject command

var (

	getPathNameCmd = &cobra.Command{
		Use:   "get-path-name",
		Short: "Command to get path name for a given path id",
		Long:  `Command to get path name for a given path id`,
		Run:   GetPathByName,
	}
    pathId string

)



func initGpnFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&pathId, "path-id", "p", "", "Path id of the object")
	cmd.Flags().StringVarP(&srcUrl, "sproxyd-url", "s", "", "sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "sproxyd-driver", "", "", "sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "sproxyd-env", "", "", "sproxyd environment [prod|osa]")

}
func init() {
	rootCmd.AddCommand(getPathNameCmd)
	initGpnFlags(getPathNameCmd)
}



func GetPathByName(cmd *cobra.Command, args []string) {
	if len(srcUrl)  == 0 {
		gLog.Error.Println("missing --spoxyd-url [http://xx.xx.xx.xx:81/proxy]")
		return
	}
	if _, err := url.ParseRequestURI(srcUrl); err != nil {
		gLog.Error.Printf("Error %v parsing url %s",err,srcUrl)
		return
	}
	if len(driver) == 0 {
		gLog.Error.Println("missing --spoxyd-driver [chord|arc]")
		driver ="chord"
		gLog.Info.Printf("using --sproxyd-driver %s",driver)

	} else {
		if driver[0:2] == "bp" {
			gLog.Error.Printf("Driver %s  should be [chord|arc]",driver)
			return
		}
	}
	if len(env) == 0 {
		gLog.Error.Println("missing --sproxyd-env [prod|osa]")
		env = "prod"
		gLog.Info.Printf("using --sproxyd-env %s ",env)
	}

	if len(pathId)== 0 {
		gLog.Error.Println("missing --path-id")
		return
	}
	mosesbc.SetSourceSproxyd("check",srcUrl,driver,env)
	if err,pathName,status := mosesbc.GetPathName(pathId); err == nil {
		gLog.Info.Printf("Get Path Name for path Id %s  -> path name %s ",pathId,pathName)
	} else {
		gLog.Error.Printf("Get Path Name for path id %s ->  Err %v - Status Code %d",pathId, err,status)
	}
}