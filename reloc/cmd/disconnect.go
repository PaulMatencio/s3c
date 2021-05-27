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
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// leaveCmd represents the leave command
var (
	fromIP, toIP , fn, ft , root string
	FT []string
	Ulong =  ` 
    find the string (ip addr)  --fromIP <ip addr> 
    or find the string(ip addr) --fromIP <ip addr> and replace by the tring --toIP <ip addr> in
    every file from a root directory tree ( --root directory) that matches the 
    --fileName and --fileType argument

    --fileName and --fileType can be generic
    Default --fileName is "*""
    Default --fileTxpe is "conf"
            
    Examples 

      (1) Find  IP addresses in every file of type conf,xml or yaml from the 
      /etc directory tree
      reloc disconnect -r /etc -t 10.12.202.10 -f 10.14.204.10  -T "conf|xml|yaml" 
                                                                                         
      (2) Find  and Replace IP addresses in every filename scality-* and file type equals 
      to conf,xml or yaml from the /etc directory  tree
      reloc disc -R -r /etc -t 10.12.202.10 -f 10.14.204.10 -N scality-* -T "conf|xml|yaml"

      (3) The default configuration file is $HOME/.reloc.yaml )
	  
      Arguments in the configuration file are overridden by its corresponding input arguments	
      
      $HOME/.reloc.yaml
      move:
        fromIP: 10.12.202.10
        toIP: 10.15.203.10
      files:
        root: "/etc"
        fn: "*"
        ft: "conf|yaml|yml|xml|py"
      `
	replace , check bool
	discCmd = &cobra.Command {
		Use:   "disc",
		Short: "Disconnect a storage node from Scality Ring",
		Long: Ulong,
		Run: func(cmd *cobra.Command, args []string) {
			Reloc(cmd)
	},}
	discCmd1 = &cobra.Command {
		Use:   "disconnect",
		Short: "Disconnect a storage node from Scality Ring",
		Long:  Ulong,
		Run: func(cmd *cobra.Command, args []string) {
			Reloc(cmd)
	},}
)

func init() {
	rootCmd.AddCommand(discCmd)
	initReloc(discCmd)
	rootCmd.AddCommand(discCmd1)
	initReloc(discCmd1)
}

func initReloc(cmd *cobra.Command){
	cmd.Flags().StringVarP(&fromIP, "fromIP", "f", "","from ip address")
	cmd.Flags().StringVarP(&toIP, "toIP", "t", "","from ip address")
	cmd.Flags().StringVarP(&root, "root", "r", "","root directory")
	cmd.Flags().StringVarP(&fn, "fileName", "N", "","file name")
	cmd.Flags().StringVarP(&ft, "fileType", "T", "","file types separed by | ")
	cmd.Flags().BoolVarP(&replace, "replace", "R", false,"Replace strings( default Find)")
	cmd.Flags().BoolVarP(&check, "check", "C", true,"check valid IP address")
}

func Reloc(cmd *cobra.Command) {

	if len(fromIP) == 0 {
		if fromIP = viper.GetString("move.fromIP");len(fromIP) == 0 {
			gLog.Info.Println("From IP address is missing")
			os.Exit(100)
		}
	}
	if check && net.ParseIP(fromIP).To4() == nil  {
		gLog.Warning.Printf("%s is not a valid IPv4 address",fromIP)
	}

    if replace {
		if len(toIP) == 0 {
			if toIP = viper.GetString("move.toIP"); len(toIP) == 0 {
				gLog.Info.Println("To IP address is missing for a replacement")
				os.Exit(100)
			}
		}
		if check && net.ParseIP(toIP).To4() == nil {
			gLog.Error.Printf("%q is not a valid IPv4 address", toIP)
			os.Exit(100)
		}
	}

	if len(root) == 0 {
		if root = viper.GetString("files.root");len(root) == 0 {
			root= "."
			gLog.Warning.Printf("Current directory, %q will be used as root\n",root)

		}
	}

	if len(fn) == 0 {
		if fn = viper.GetString("files.fn");len(fn) == 0 {
			fn="*"
			gLog.Warning.Printf("File name is missing, %q  will be used as file name\n",fn)
		}
	}

	if len(ft) == 0 {
		if ft = viper.GetString("files.ft"); len(ft) == 0 {
			ft = "conf|yaml|xml"
			gLog.Warning.Printf("File types are missing, %q  will be used as file types\n", ft)

		}
	}
	FT = strings.Split(ft, "|")
	if err := filepath.Walk(root, findReplace); err != nil {
		gLog.Error.Printf("error %v replacing file\n",err)
	} else {
		gLog.Info.Println("End")
	}

}


func findReplace(path string, fi os.FileInfo, err error) error {

	var (
		matched bool
		re = regexp.MustCompile(fromIP)
		Pattern []string
	)
	if err != nil {
		gLog.Error.Println(err)
		return nil
	}
	if !!fi.IsDir() {
		return nil //
	}
	pattern:= fn
	for _,t :=range FT {
		if len(t) >0 {
			pattern += "."+t
			Pattern = append(Pattern,t)
		}
	}

	// gLog.Trace.Println(Pattern)
	now := time.Now()

    matched = false
    Matched := make([]bool,len(Pattern))
	for i,p:= range Pattern {
		p = fn + "." + p
		if Matched[i], err = filepath.Match(p, fi.Name()); err != nil {
			return err
		} else {
			matched = Matched[i]
			if matched {
				break
			}
		}
	}

	if matched {
		backup := path + "-" + fmt.Sprintf("%4d-%02d-%02d:%02d:%02d:%02d",now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),now.Second())
		if read, err := ioutil.ReadFile(path); err == nil {
			// Find string fromIP
			if r:= re.Find([]byte(read));len(r) > 0 {
			 	if replace {
			 		// Replace string fromIP by string toIP
			 		//  backup old file before replace
					 if err = ioutil.WriteFile(backup, []byte(read), 0644); err == nil {
						 gLog.Info.Printf("Replacing string %s by string %s in file %s\n", fromIP, toIP, path)
						 newContents := strings.Replace(string(read), fromIP, toIP, -1)
						 return ioutil.WriteFile(path, []byte(newContents), 0)
					 } else {
						 return err
					 }
				 } else {
					 gLog.Trace.Printf("Searching string %s in file %s ", fromIP, path)
					 gLog.Info.Printf("Find %q in %s\n", r, path)
				 }
			}
		} else {
			gLog.Error.Printf("Error %v while reading path %s", err,path)
			return nil
		}
	}
	return nil
}

