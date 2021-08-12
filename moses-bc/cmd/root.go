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
	missingInputFile = "Missing input file  --input-file"

	missingSrcBucketAndIfile = "Either --source-bucket or --input-file is required"

	missingSourceUrls = "Missing  --source-sproxyd-url argument or add xx.sproxyd.source.urls in the config.yaml file - xx = [backup|clone|restore|check]"
	missingSourceEnv = "Missing  --source-sproxyd-env argument or add xx.sproxyd.source.env [prod|osa] in the config.yaml file - xx = [backup|clone|restore|check]"
	missingSourceDriver = "Missing  --source-sproxyd-driver argument  or add xx.sproxyd.source.driver [bpchord|bparc] in the config.yaml file - xx = [backup|clone|restore|check]"
	MissingTargetUrls = "Missing  --target-sproxyd-url argument or add xx.sproxyd.target.urls in the config.yaml file - xx = [backup|clone|restore|check]"
	missingTargetEnv = "Missing  --target-sproxyd-env argument or add xx.sproxyd.target.env [prod|osa] in the config.yaml file - xx = [backup|clone|restore|check]"
	missingTargetDriver = "Missing  --target-sproxyd-driver argument or add xx.sproxyd.target.drive [bpchord|bparc] in the config.yaml file - xx = [backup|clone|restore|check]"
	missingSrcS3Uri = "Missing source S3 uri, add xx.s3.source.url in the config.yaml file - xx = [backup|clone|restore|check]"
	missingSrcS3AccessKey = "Missing source S3 access key, add xx.s3.source.credential.access_key_id in the config.yaml file - xx = [backup|clone|restore|check]"
	missingSourceS3SecretKey = "Missing source S3 secret key, add xx.s3.source.credential.secret_access_key in the config.yaml file - xx = [backup|clone|restore|check]"
	missingTargetS3Uri = "Missing source S3 uri, add xx.s3.target.url in the config.yaml file - xx = [backup|clone|restore|check]"
	missingTargetS3AccessKey = "Missing target S3 access key, add xx.s3.target.credential.access_key_id in the config.yaml file - xx = [backup|clone|restore|check]"
	missingTargetS3SecretKey = "Missing  target S3 secret key, add xx.s3.target.credential.secret_access_key in the config.yaml file - xx = [backup|clone|restore|check]"
	missingoDir      = "Missing output directory --output-directory argument. Use  --help or -h for help"
	missingiFile     = "Missing input file --input-file argument. Use  --help or -h for help "
	missingSrcBucket = "Missing source S3 bucket --source-bucket argument. Use  --help or -h for help"
	missingTgtBucket = "Missing target S3 bucket --target-bucket argument. Use  --help or -h for help"
	missingIndBucket = "Missing index S3 bucket --index-bucket argument. Use  --help or -h for help"
	missingBucket = "Missing S3 bucket--bucket argument. Use  --help or -h for help"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "moses-bc",
	Short: "Command to backup/clone/restore/migrate moses assets",
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
	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",rootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",rootCmd.PersistentFlags().Lookup("profiling"))
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