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
	"errors"
	"fmt"
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
)
var (
	config string
	bucket,levelDBUrl	 string
	verbose, Debug,autoCompletion 	 bool
	loglevel,profiling, bucketNumber, retryNumber  int
	waitTime time.Duration
	source,target string
	bMedia,bS3Url,bS3AccessKey,bS3SecretKey,metaUrl,metaAccessKey,metaSecretKey string
	// conTimeout,readTimeout,writeTimeout, copyTimeout,timeout time.Duration
	missingBucket = "Missing bucket - please provide the bucket name"
	missingPrefix = "Missing prefix  - please provide the prefix of the keys"
	missingSource = "Missing source sproxyd urls, add sproxyd source urls in the config file"
	missingTarget = "Missing target sproxyd urls, add sproxzd target urls in the confif file"
	missingBS3url = "Missing Backup S3 url"
	missingBS3ak = "Missing Backup S3 access key"
	missingBS3sk = "Missing Backup S3 secret key"
	missingMetaurl = "Missing source meta-moses S3 rest endpoint url"
	missingMetaak = "Missing source meta-moses aws access key"
	missingMetask = "Missing source meta-moses aws secret key"


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
	rootCmd.PersistentFlags().StringVarP(&bMedia,"backupMedia","","S3","backup media [S3|File]")

	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",rootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",rootCmd.PersistentFlags().Lookup("profiling"))
	viper.BindPFlag("backupMedia",rootCmd.PersistentFlags().Lookup("backupMedia"))
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

	waitTime = utils.GetWaitTime(*viper.GetViper())

	if retryNumber =utils.GetRetryNumber(*viper.GetViper()); retryNumber > 0 {
		sproxyd.DoRetry= retryNumber
	}

	if source = viper.GetString("sproxyd.source.urls"); len(source) == 0 {
		err := errors.New(missingSource)
		log.Fatalln(err)
	} else {
		sproxyd.Url = source
	}

	if target = viper.GetString("sproxyd.target.urls"); len(target) == 0 {
		err := errors.New(missingTarget)
		log.Fatalln(err)
	} else {
		sproxyd.TargetUrl = target
	}

	if conTimeout := viper.GetDuration("sproxyd.target.connectionTimeout"); conTimeout >  0 {
		sproxyd.ConnectionTimeout= conTimeout
	}


	if copyTimeout := viper.GetDuration("sproxyd.target.connectionTimeout"); copyTimeout == 0 {
		sproxyd.CopyTimeout = copyTimeout
	}

	if readTimeout := viper.GetDuration("sproxyd.target.readTimeout"); readTimeout == 0 {
		sproxyd.ReadTimeout = readTimeout
	}

	if writeTimeout := viper.GetDuration("sproxyd.target.readTimeout"); writeTimeout == 0 {
		sproxyd.ReadTimeout = writeTimeout
	}

	if driver := viper.GetString("sproxyd.source.driver"); len(driver) == 0 {
		sproxyd.Driver = driver
	}
	if driver := viper.GetString("sproxyd.target.driver"); len(driver) == 0 {
		sproxyd.TargetDriver= driver
	}
	sproxyd.SetNewProxydHost()
	sproxyd.SetNewTargetProxydHost()

	gLog.InitLog(rootCmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		utils.GenAutoCompletionScript(rootCmd,configPath)
	}

}