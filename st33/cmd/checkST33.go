// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
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
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	_ "path"
	"path/filepath"
	"strings"
)

//  check ST33 data consistency using the ST33 control file
var (
	checkST33Cmd = &cobra.Command {
		Use:   "chkST33",
		Short: "Command to check ST33 data file consistency vs control file",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
				checkST33(cmd,args)
		},
	}
	cfile string
	Errors,Warnings = 0,0
)


func initCdFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&idir,"idir","d","","input directory containing  st33  files to be uploaded")
	// cmd.Flags().StringVarP(&partition,"partition", "p","", "subdirectory of data/control file prefix ex: p00001")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","input fullname data file, list of fullname data files separated by a commma or a range of data file suffix ex: 0020...0025")
	cmd.Flags().StringVarP(&datval,"data-prefix", "","", "data file prefix  ex: datval.lot")
	cmd.Flags().StringVarP(&conval,"ctrl-prefix", "","", "control file prefix ex: conval.lot")
}

func init() {
	RootCmd.AddCommand(checkST33Cmd)
	initCdFlags(checkST33Cmd)
}

func checkST33(cmd *cobra.Command, args []string) {

	var (
		files []string
		f    int
		err   error
	)
	if len(idir) == 0 {
		idir = viper.GetString("st33.input_data_directory")
		if len(idir) == 0 {
			gLog.Info.Printf("%s","Input directory missing, please check your config file or specif  -d or --idir ")
			return
		}
	}

	if len(partition) == 0 {
		partition = viper.GetString("st33.input_data_partition")
		if len(partition) == 0 {
			gLog.Info.Printf("%s","Input directory partition is missing, please check your config file or specif  -p or --partition ")
			return
		}
	}

	// if no datval argument . try to get in from the config file
	if len(datval) == 0 {
		datval = viper.GetString("st33.data_file_prefix")
		if len(datval) == 0 {
			gLog.Info.Printf("Data file name prefix is  missing, please check your config file or specify --data-prefix")
			return
		}
	}

	// if no conval argument, try to get it from the config file
	if len(conval) == 0 {
		conval = viper.GetString("st33.control_file_prefix")
		if len(conval) == 0 {
			gLog.Info.Printf("Control file name prefix is  missing, please check your config file or sepecify --ctrl-prefix")
			return
		}
	}

	// build an array of input files based on above arguments

	if files, err = buildInputFiles(ifile); err != nil || len(files) == 0 {
		gLog.Error.Printf("Problem to parse input files %s. Check --ifile argument and its syntax rules ex:  -i 056...060 or -i 058,070,085 ",ifile)
		return
	}

	for _,file := range files {

		var (

			ifile = filepath.Join(filepath.Join(idir,partition),file)
        	cfile =  strings.Replace(ifile,datval,conval,1)
        	ind int
        	errors, warnings = 0,0
        	v  st33.Conval
		)
		gLog.Info.Printf("Checking ST33 input file %s",ifile)
		gLog.Warning.Printf("Checking ST33 input file %s",ifile)

		r, err := st33.NewSt33Reader(ifile)

		if err != nil {
			gLog.Fatal.Printf("%v", err)
			os.Exit(100)
		}
		if c, err := st33.BuildConvalArray(cfile); err == nil {

			for ind, v = range *c {

				lp := len(v.PxiId)
				typ := v.PxiId[lp-2 : lp-1]

				if typ == "B" { // BLOB record
					r.ReadST33BLOB(v)
				} else if typ == "P" {
						w,e,c := r.ReadST33Tiff(v,ind)
						warnings += w
						errors += e
						if c {
							gLog.Error.Printf("Skipping file %s due to critical error",file)
							break
						}

				} else {
					gLog.Warning.Printf("%s 's document code is %s", v.PxiId, typ)
				}
			}
			gLog.Warning.Printf("File: %s - Number of documents processed: %d  - Number of warnings: %d  - Number of errors: %d",file, ind+1,warnings,errors )
			Errors += errors
			Warnings += warnings
		} else {
			gLog.Error.Println(err)
		}
		f++
	}
	gLog.Warning.Printf("Partition: %s  - Total number of files: %d - Total number of warnings: %d - Total number of errors: %d ",partition,f,Warnings,Errors)
}


