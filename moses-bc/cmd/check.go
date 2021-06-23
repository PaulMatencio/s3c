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
	goLog "github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"

	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command
var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Command to check  MOSES backup",
		Long:  `Check the restored blobs against the current blobs`,
		Run:   check,
	}
	np,status int
	err error
	driver, targetDriver string

)

func initCkFlags(cmd *cobra.Command) {

	cmd.Flags().IntVarP(&maxPage, "maxPage", "m", 50, "maximum number of concurrent pages to be checked")
	cmd.Flags().StringVarP(&pn, "pn", "k", "", "Publication number(document key)")
	cmd.Flags().StringVarP(&source, "source-url", "s", "http://10.12.202.10:81", "source URL http://xx.xx.xx.xx:81")
	cmd.Flags().StringVarP(&driver, "source-driver", "", "bpchord", "source driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-driver", "", "bparc", "target driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&target, "target-url", "t", "http://10.12.210.170:81", "target URL http://xx.xx.xx.xx:81")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "M", 20, "Maximum part size (MB)")

}

func init() {
	rootCmd.AddCommand(checkCmd)
	initCkFlags(checkCmd)
}

func check(cmd *cobra.Command, args []string) {

	if len(source) > 0 {
		sproxyd.Url = source
	} else {
		goLog.Error.Printf("Source URL is missing")
		return
	}
	if len(driver) > 0 {
		sproxyd.Driver = driver
	} else {
		goLog.Error.Printf("Source driver is missing")
		return
	}

	if len(targetDriver) > 0 {
		sproxyd.TargetDriver = targetDriver
	} else {
		goLog.Error.Printf("Target URL is missing")
		return
	}

	sproxyd.SetNewProxydHost()
	sproxyd.SetNewTargetProxydHost()
	if _, err, status := mosesbc.GetPageNumber(pn); err != nil && status == 200 {
		mosesbc.CheckBlob1(pn, np, maxPage)
	}
}