
package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"time"
)

// rootCmd represents the base command when called without any subcommands
var (
	config,bucket,key,metaEx 	 string
	verbose, Debug,autoCompletion 	 bool
	// log          = lumber.NewConsoleLogger(lumber.INFO)

	odir,pdir  string
	waitTime time.Duration
	loglevel,profiling,bucketNumber,retryNumber int

	missingBucket = "Missing bucket - please provide the bucket name"
	missingToBucket = "Missing target bucket - please provide the target bucket name"
	missingFromBucket = "Missing source bucket - please provide the source bucket name"
	missingKey = "Missing key - please provide the key of the object"
	missingInputFile ="Missing date input file - please provide the input file path (absolute or relative to current directory"
	missingMetaFile ="Missing meta input file - please provide the meta file path (absolute or relative to current directory"
	missingOutputFolder ="Missing output directory - please provide the output directory path( absolute or relative to current directory)"
	missingInputFolder ="Missing input directory - please provide the input directory path( absolute or relative to current directory)"

	RootCmd = &cobra.Command {
		Use:   "sc",
		Short: "Scality S3 frontend commands",
		Long: ``,
		TraverseChildren: true,
})


// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func init() {

	// persistent flags

	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose","v", false, "verbose output")
	RootCmd.PersistentFlags().IntVarP(&loglevel, "loglevel", "l", 0,"Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)")
	RootCmd.Flags().StringVarP(&config,"config", "c","", "Full path of the config file; default $HOME/.sc/config.yaml")
	RootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "C",false, "generate bash auto completion")
	RootCmd.PersistentFlags().IntVarP(&profiling,"profiling", "P",0, "display memory usage every P seconds")

	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("verbose",RootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("loglevel",RootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",RootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",RootCmd.PersistentFlags().Lookup("profiling"))
	// read and init the config with  viper
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
	bucketNumber = utils.GetNumberOfBucket(*viper.GetViper())
	gLog.InitLog(RootCmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		utils.GenAutoCompletionScript(RootCmd,configPath)
	}
	if metaEx = viper.GetString("meta.extension"); metaEx == "" {
		metaEx = "md"
	}

	 waitTime = utils.GetWaitTime(*viper.GetViper())
	if retryNumber =utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
		retryNumber =1
	}

}


/*
func setLogLevel() (int) {

	if loglevel == 0 {
		loglevel = viper.GetInt("logging.log_level")
	}

	if verbose {
		loglevel= 4
	}

	return loglevel

}



func getLogOutput() (string) {
	return viper.GetString("logging.output" )

}
*/