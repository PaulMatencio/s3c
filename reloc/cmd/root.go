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
	"os"
	"log"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	config, logOutput string
	loglevel int
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "reloc",
	Short: "Find and replace strings as for instance IP addresses in files of a directory tree",
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
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&config, "config", "c", "","config file (default is $HOME/.reloc.yaml)")
	rootCmd.PersistentFlags().IntVarP(&loglevel, "loglevel", "l", 3,"Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)")
	rootCmd.PersistentFlags().StringVarP(&logOutput, "logOutput", "", "terminal","output location log")
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if config != "" {
		// Use config file from the flag.
		viper.SetConfigFile(config)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".reloc" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".reloc")
		viper.SetConfigType("yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}  else {
		log.Printf("Error %v  while reading config file %s",err,viper.ConfigFileUsed())
	}
	gLog.InitLog(rootCmd.Name(),loglevel,logOutput)

}
