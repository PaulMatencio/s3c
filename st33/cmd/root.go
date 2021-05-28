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
)

// rootCmd represents the base command when called without any subcommands
var (
	cfgFile,bucket,metaEx 	 string
	verbose, Debug,autoCompletion,test 	 bool
	// log          = lumber.NewConsoleLogger(lumber.INFO)
	conval,datval string
	odir,pdir,bdir,ifile, lot string
	loglevel,profiling,suffix,bucketNumber int

	missingBucket = "Missing bucket - please provide the bucket name"
	missingKey = "Missing pxid - please provide a pxi id"
	missingInputFile ="Missing input data file - please provide the input data file "
	missingCtrlFile ="Missing input control file - please provide the input control file"
	missingOutputFolder ="Missing output directory - please provide the output directory path"
	missingInputFolder ="Missing input directory - please provide the input directory path"
	missingSourceBucket = "Missing source bucket - please provide a source  bucket name"
	missingTargetBucket = "Missing target bucket - please provide a target  bucket name"
	missingIds = "missing both a pxi id and an input file containing list of pxi ids"

	RootCmd = &cobra.Command {
		Use:   "st33",
		Short: "st33 to S3 migration tools",
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
	RootCmd.Flags().StringVarP(&cfgFile,"config", "c","", "sc config file; default $HOME/.st33/config.yaml")
	RootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "C",false, "generate bash auto completion")
	RootCmd.PersistentFlags().IntVarP(&profiling,"profiling", "P",0, "display memory usage every P seconds")
	RootCmd.PersistentFlags().StringVarP(&partition,"partition", "p","", "subdirectory of data/control file prefix ex: p00001")
	RootCmd.PersistentFlags().StringVarP(&lot,"lot", "L","", "string to be appended to the path directory of the log")

	// RootCmd.Flags().StringVarP(&datval,"data-file-directrory", "","", "data file prefix  ex: datval.lot")
	// RootCmd.Flags().StringVarP(&datval,"data-file-prefix", "","", "data file prefix  ex: datval.lot")
	// RootCmd.Flags().StringVarP(&conval,"control-file-prefix", "","", "control file prefix ex: conval.lot")
	// RootCmd.PersistentFlags().BoolVarP(&test,"test","t",false,"test mode")

	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("verbose",RootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("loglevel",RootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",RootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",RootCmd.PersistentFlags().Lookup("profiling"))
	viper.BindPFlag("partition",RootCmd.PersistentFlags().Lookup("partition"))
	viper.BindPFlag("lot",RootCmd.PersistentFlags().Lookup("lot"))

	// read and init the config with  viper
	cobra.OnInitialize(initConfig)
}


// initConfig reads in config file and ENV variables if set.

func initConfig() {
	var (
		configPath string
	)
	if cfgFile != "" {
		// Use config file from the application flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatalln(err)
		}

		configPath = filepath.Join("/etc/st33") // call multiple times to add many search paths
		viper.AddConfigPath(configPath)            // another path to look for the config file

		configPath = filepath.Join(home,".st33")
		viper.AddConfigPath(configPath)            // path to look for the config file

		viper.AddConfigPath(".")               // optionally look for config in the working directory
		viper.SetConfigName("config")          // name of the config file without the extension
		viper.SetConfigType("yaml")
	}


	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it

	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}  else {
		log.Printf("Error %v reading config file %s",err,viper.ConfigFileUsed())
		log.Printf("AWS sdk shared config will be used if present ")
	}

	logOutput:= utils.GetLogOutput(*viper.GetViper())
	logCombine:= utils.GetLogCombine(*viper.GetViper())
	bucketNumber = utils.GetNumberOfBucket(*viper.GetViper())

	loglevel = utils.SetLogLevel(*viper.GetViper(),loglevel)
	if logOutput != "terminal" {
		logOutput += string(os.PathSeparator) + partition
		if lot != ""  {
			logOutput += string(os.PathSeparator) + lot
		}
	}

	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)
	if ! logCombine {
		gLog.InitLog(RootCmd.Name(), loglevel, logOutput)
	} else {
		gLog.InitLog1(RootCmd.Name(), loglevel, logOutput)
	}
	// Generate auto completion command
	if  autoCompletion {
		utils.GenAutoCompletionScript(RootCmd,configPath)
	}

	if metaEx = viper.GetString("meta.extension"); metaEx == "" {
		metaEx = "md"
	}



}


