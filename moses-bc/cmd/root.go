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
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"time"
)
const (

	RETRY = 5
	MinPartSize                = 5 * 1024 * 1024     // 5 MB
	MaxFileSize                = MinPartSize * 409.6 // 2 GB
	DefaultDownloadConcurrency = 5
	Dummy                      = "/dev/null"
	WaitTime	= time.Duration(200)          /*  default waitime between retry in sec */
	RetryNumber = 5             /*   default retry number*/
	MaxPartSize					= MinPartSize *  20
)

var (
	config string
	bucket,levelDBUrl,prefix	 string
	verbose, Debug,autoCompletion 	 bool
	loglevel,profiling, bucketNumber, retryNumber  int
	waitTime time.Duration
	srcBucket, tgtBucket              string
	srcEnv, tgtEnv 					 string
	check        bool
	listpn     *bufio.Scanner
	tgtS3, srcS3 *s3.S3
	// conTimeout,readTimeout,writeTimeout, copyTimeout,timeout time.Duration
	invalidBucket = "Invalid bucket name, it should not contain a suffix"
	missingInputFile = "Missing input file     --input-file"

	missingSrcBucketAndIfile = "Either -ssource-bucket or --input-file is required"

	missingSourceUrls = "Missing source sproxyd urls, add sproxyd source urls in the config file"
	missingTargetUrls = "Missing target sproxyd urls, add sproxyd target urls in the confif file"
	missingSrcS3Uri = "Missing source S3 uri"
	missingSrcS3AccessKey = "Missing source S3 access key"
	missingSourceS3SecretKey = "Missing source S3 access key"
	missingTargetS3Uri = "Missing source S3 uri"
	missingTargetS3AccessKey = "Missing source S3 access key"
	missingTargetS3SecretKey = "Missing source S3 access key"
	missingoDir      = "Missing output directory --output-directory"
	missingiFile     = "Missing input file --input-file"
	missingSrcBucket = "Missing source S3 bucket --source-bucket"
	missingTgtBucket = "Missing target S3 bucket --target-bucket"

)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "moses-bc",
	Short: "Command to backup/clone moses",
	Long: ``,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}


func init() {

	rootCmd.PersistentFlags().IntVarP(&loglevel, "loglevel", "l", 0,"Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)")
	rootCmd.PersistentFlags().StringVarP(&config,"config", "c","", "sc config file; default $HOME/.clone/config.yaml")
	rootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "C",false, "generate bash auto completion")
	rootCmd.PersistentFlags().IntVarP(&profiling,"profiling", "P",0, "display memory usage every P seconds")
	// rootCmd.PersistentFlags().StringVarP(&bMedia,"bMedia","","S3","backup media [S3|File]")

	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",rootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",rootCmd.PersistentFlags().Lookup("profiling"))
	viper.BindPFlag("bMedia",rootCmd.PersistentFlags().Lookup("bMedia"))
	cobra.OnInitialize(initConfig)
}

// initConfig reads in config file and ENV variables if set.

func initConfig() {
	// utils.InitConfig(config,*viper.GetViper(),*RootCmd)
	var configPath string
	if config != "" {
		// Use config file from the application flag.
		viper.SetConfigFile(config)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatalln(err)
		}

		configPath = filepath.Join("/etc/clone") // call multiple times to add many search paths
		viper.AddConfigPath(configPath)            // another path to look for the config file

		configPath = filepath.Join(home,".clone")
		viper.AddConfigPath(configPath)            // path to look for the config file

		viper.AddConfigPath(".")               // optionally look for config in the working directory
		viper.SetConfigName("config")          // name of the config file without the extension
		viper.SetConfigType("yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}  else {
		log.Printf("Error %v  reading config file %s",err,viper.ConfigFileUsed())
		log.Printf("AWS sdk shared config will be used if present ")
	}
	logOutput:= utils.GetLogOutput(*viper.GetViper())
	loglevel = utils.SetLogLevel(*viper.GetViper(),loglevel)
	//bucketNumber = utils.GetNumberOfBucket(*viper.GetViper())
	levelDBUrl = utils.GetLevelDBUrl(*viper.GetViper())
	if logOutput != "terminal" {
		logOutput += string(os.PathSeparator) + bucket
	}

	if waitTime = viper.GetDuration("transport.retry.waittime"); waitTime == 0 {
		waitTime = WaitTime
	} else {
		waitTime = time.Duration(waitTime*time.Millisecond)
	}

	if  retryNumber = viper.GetInt("transport.retry.number"); retryNumber == 0 {
		retryNumber = RetryNumber
	}

	sproxyd.DoRetry= retryNumber

	if conTimeout := viper.GetDuration("transport.connectionTimeout"); conTimeout >  0 {
		sproxyd.ConnectionTimeout= time.Duration(conTimeout*time.Millisecond)
	}


	if copyTimeout := viper.GetDuration("transport.copyTimeout"); copyTimeout > 0 {
		sproxyd.CopyTimeout = time.Duration(copyTimeout*time.Millisecond)
	}

	if readTimeout := viper.GetDuration("transport.readTimeout"); readTimeout >0 {
		sproxyd.ReadTimeout = time.Duration(readTimeout*time.Millisecond)
	}

	if writeTimeout := viper.GetDuration("transport.writeTimeout"); writeTimeout > 0 {
		sproxyd.WriteTimeout = time.Duration(writeTimeout*time.Millisecond)
	}

	/*
	Set  new source and target  sproxyd
		sproxyd.HP
	    sproxyd.TargetHP


	sproxyd.SetNewProxydHost()
	sproxyd.SetNewTargetProxydHost()
	 */

	gLog.InitLog(rootCmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		utils.GenAutoCompletionScript(rootCmd,configPath)
	}

}