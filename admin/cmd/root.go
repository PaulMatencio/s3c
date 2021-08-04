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
	"github.com/paulmatencio/s3c/utils"
	"log"
	"os"
	"path/filepath"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	CONTIMEOUT = 2000  // connection timeout in ms
	KEEPALIVE = 15000  // keep alive  in ms
	HTTP = "http://"
)
var (
	config,bucket,cc,url	 string
	verbose, Debug,autoCompletion 	 bool
	loglevel, retryNumber, bucketdPort, s3Port  int
	adminPort []int
	waitTime time.Duration
	missingBucket = "Missing bucket - please provide the bucket name"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "admin",
	Short: "S3 admin commands",
	Long: ``,

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
	rootCmd.PersistentFlags().StringVarP(&config,"config", "c","", "sc config file; default $HOME/.admin/config.yaml")
	rootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "C",false, "generate bash auto completion")
	rootCmd.PersistentFlags().StringVarP(&cc,"cc", "a","", "string to be appended to the path directory of the log")


	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",rootCmd.PersistentFlags().Lookup("autoCompletion"))
	// viper.BindPFlag("profiling",rootCmd.PersistentFlags().Lookup("profiling"))

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

		configPath = filepath.Join("/etc/sc") // call multiple times to add many search paths
		viper.AddConfigPath(configPath)            // another path to look for the config file

		configPath = filepath.Join(home,".sc")
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
	if logOutput != "terminal" {
		logOutput += string(os.PathSeparator) + cc
	}
	waitTime = utils.GetWaitTime(*viper.GetViper())
	if retryNumber =utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
		retryNumber = 1
	}

	gLog.InitLog(rootCmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		utils.GenAutoCompletionScript(rootCmd,configPath)
	}

}