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
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strings"
)

// hashCCCmd represents the hashCC command
var (
	modulo int
	ccode     string
	hashCCcmd= &cobra.Command{
		Use:   "suffix",
		Short: "Return the MOSES bucket suffix for a given country code (the first 2 character of a document id)",
		Long: `Ex : suffix  ES/12345`,
		Run: func(cmd *cobra.Command, args []string) {
			hashCmd(cmd)
		},
	}

)

func init() {
	rootCmd.AddCommand(hashCCcmd)
	hashCCcmd.Flags().StringVarP(&prefix,"prefix","p","","return the suffix of bucket for a given MOSES  (prefix)")
	hashCCcmd.Flags().IntVarP(&modulo, "modulo", "m", 5,"modulo")
}

func hashCmd(cmd *cobra.Command){
	if modulo >0 {
		pref := strings.Split(prefix,"/")[0]
		if len(pref) == 2 {
			fmt.Printf("The suffix for %s is: %02d \n", prefix, utils.HashKey(pref, modulo))
		}
	} else {
		fmt.Printf("invalid country code")
	}
}



