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
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// st33ToS3Cmd represents the st33ToS3 command

var (
	async, parallel int
	file,sBucket,partition  string
	files,ranges []string
	reload  bool   // force reload when true
	check   bool   // check mode when true
	toS3V2Cmd = &cobra.Command {
		Use:   "toS3",
		Short: "Command to extract ST33 file then upload to S3",
		Long: `Command extract files containing ST33 Tiff Images and Blob then upload them to S3`,
		Run: func(cmd *cobra.Command, args []string) {
			toS3Func(cmd,args)
		},
	}

)


func initT3Flags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&idir,"idir","d","","input directory containing  st33  files to be uploaded")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","input fullname data file, list of fullname data files separated by a commma or a range of data file suffix ex: 020...025")
	cmd.Flags().StringVarP(&datval,"data-prefix", "","", "data file prefix  ex: datval.lot")
	cmd.Flags().StringVarP(&conval,"ctrl-prefix", "","", "control file prefix ex: conval.lot")
	cmd.Flags().StringVarP(&partition,"partition", "p","", "subdirectory of data/control file prefix ex: p00001")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","name of the  target bucket")
	cmd.Flags().StringVarP(&sBucket,"state-bucket","s","","name of the migration state bucket")
	cmd.Flags().BoolVarP(&reload,"reload","r",false,"reload the bucket")
	cmd.Flags().BoolVarP(&check,"test-mode","t",false,"test mode")
	cmd.Flags().IntVarP(&async,"async","a",0,"number of document to be uploaded concurrently")
	cmd.Flags().IntVarP(&parallel,"parallel","",0,"number of pages of a document to be concurrently uploaded")
}

func init() {
	// RootCmd.AddCommand(toS3Cmd)
	RootCmd.AddCommand(toS3V2Cmd)
	// initT3Flags(toS3Cmd)
	initT3Flags(toS3V2Cmd)

}


//  Upload  St33 files to S3
//  idir : input directory
//  datval : data file prefix
//  conval : control file prefix
//  bucket : target bucket
//  ifile:  An input file , a list of files and range of suffix  Ex  003    004,007   004...020


func toS3Func(cmd *cobra.Command, args []string) {

	var (
		files []string
		err   error
	)

	if len(idir) == 0 {
		idir = viper.GetString("st33.input_data_directory")
		if len(idir) == 0 {
			gLog.Info.Printf("%s","Input directory is missing, please check your config file or specif  -d or --idir ")
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

	if len(bucket) == 0 {
		gLog.Info.Printf("%s",missingBucket)
		return
	}

	if len(sBucket) == 0 {
		sBucket = viper.GetString("logging.bucket")
		if len(sBucket) == 0 {
			gLog.Warning.Printf("Missing bucket to log the status of this migration",)
		}
	}

	gLog.Info.Printf("Input Directory %s  - Data file name prefix:%s - Control file name prefix:%s - output state bucket name:  %s",idir,datval,conval,sBucket)
	if check {
		gLog.Info.Print("Running in test mode")
	}

	for _,file := range files {

		var (
			// file = filepath.Join(idir,file)
			file = filepath.Join(filepath.Join(idir,partition),file)
			numdocs,numrecs,numpages int = 0,0,0
			size  int         = 0
			Err   []st33.S3Error
			start0 = time.Now()
			toS3 = st33.ToS3Request {
				File: file,   //
				Bucket: bucket,
				Number: bucketNumber,
				LogBucket: sBucket,
				DatafilePrefix: datval,
				CrlfilePrefix: conval,
				Profiling: profiling,
				Reload: reload,
				Async: async,
				Check: check,
			}
		)

		// For helping to identify  the current input file ( lot number)   in all the logs

		if lot == "" {
			gLog.Info.Printf("Processing input file ..........................  %s %s", idir, file)
			/* gLog.Warning.Printf("Processing input file  ..........................  %s %s", idir, file)
			gLog.Error.Printf("Processing input file  ..........................  %s %s", idir, file)
			*/
		}



		if async <= 1  {
			// async 0 or 1
			// upload 1 document at a time
			// however parallel number of pages  are uploaded concurrently
			if parallel == 0 {
				numpages, numrecs,numdocs, size, Err = st33.ToS3V1(&toS3)
			} else {
				toS3.Async = parallel
				numpages, numrecs, numdocs, size, Err = st33.ToS3V1Parallel(&toS3)
			}
			// gLog.Info.Printf("%d documents/ %d pages were processed - error ", numdocs, numpages, err)
		} else {
			numpages, numrecs,numdocs, size, Err = st33.ToS3V1Async(&toS3)
		}

		//  free oS memory just in case

		gLog.Info.Printf("Input file: %s - Number of uploaded documents: %d - Number of read pages/records: %d/%d - Number of errors: %d  Upload Size: %.2f GB - Total elapsed time: %s\n", file, numdocs, numpages, numrecs, len(Err),float64(size)/float64(1024*1024*1024), time.Since(start0))
		if len(Err) > 0 {
			gLog.Error.Printf("Uploading %s - List of errors:", file)
			for _, v := range Err	{
				gLog.Error.Printf("Key:%s  Error:%v\n", v.Key, v.Err)
			}
		}
		// debug.FreeOSMemory()
	}
}


// return an array of ST33  files to be processed
// input an input file, a list of input file or a range of input files suffix
//    Ex  003    004,007   004...020
// output an array of input files to be processed by toS3  <datval>xxx

func buildInputFiles(ifile string) ([]string,error) {

	var (
		files []string
		err error
	)

	if len(ifile) == 0 {  // all  files  that match datval filter will be processed
		gLog.Warning.Printf("All data files of the directory %s will be uploaded", idir)
		if files, err = utils.ReadDirectory(idir, datval); err == nil {
			return files, err
		}
	}

	if ranges = strings.Split(ifile, "..."); len(ranges) > 1 { // input files range ex: 001...020

		s, err1 := strconv.Atoi(ranges[0])
		e, err2 := strconv.Atoi(ranges[1])
		if s > e {
			gLog.Fatal.Println("End suffix must be higher or equal to start prefix")
		}
		if err1 != nil || err2 != nil {
			gLog.Fatal.Println("Suffix must be numeric")
		}
		df := "%0" + strconv.Itoa(len(ranges[0])) + "d"

		for i := s; i <= e; i++ {
			k := fmt.Sprintf(df, i)
			files = append(files, datval+k)
		}

	} else {
		//  input files separated by a comma
		for _,v := range  strings.Split(ifile, ",") {
			files= append(files,datval+v)
		}
	}
	return files,err
}

