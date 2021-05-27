// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
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
	"fmt"
	directory "github.com/paulmatencio/ring/directory/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strings"
)

// hashCCCmd represents the hashCC command
var (
	modulo int
	ccode     string
	hashCCcmd= &cobra.Command{
		Use:   "hcc",
		Short: "check hash country code",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			hashCmd(cmd)
		},
	}
	getCCcmd = &cobra.Command{
		Use:   "getCC",
		Short: "get CC",
		Long: `List Scality Sindexd entries with prefix: 
            There are 3 set of index-ids: 
            PN => Publication number index-ids
            PD => Publication date index-ids
            BN => Legacy BNS id index-ids`,
		Run: func(cmd *cobra.Command, args []string) {
			getCountry(cmd,args)
		},
	}
)

func init() {
	rootCmd.AddCommand(hashCCcmd)
	rootCmd.AddCommand(getCCcmd)
	hashCCcmd.Flags().StringVarP(&ccode,"cp","p","US","country code separated by c")
	hashCCcmd.Flags().IntVarP(&modulo, "modulo", "m", 5,"modulo")
	getCCcmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table [pn|pd|bn]")
}


func hashCmd(cmd *cobra.Command){
	if modulo >0 {
		cc := strings.Split(ccode,",")
		for _,c := range cc {
			if len(c) == 2 {
				fmt.Printf("Hashkey country code %s  - modulo %d : %d \n", c, modulo, utils.HashKey(c, modulo))
			} else {
				fmt.Printf("Country %s must have 2 characters \n",c)
			}
		}
	} else {
		fmt.Printf("modulo %d  must > 0\n",modulo)
	}

}

func getCountry(cmd *cobra.Command,args []string) {
	if len(iIndex) == 0 {
		usage(cmd.Name())
		return
	}
	indSpecs := directory.GetIndexSpec(iIndex)
	countrySpecs := directory.GetCountrySpec(indSpecs)
	for k,v := range countrySpecs {
		fmt.Println(k,v)
	}
}

