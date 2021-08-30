package utils

import (
	"github.com/mitchellh/go-homedir"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"path/filepath"
	"time"
)

func GenAutoCompletionScript(rootcmd *cobra.Command,pathname string) {

	autoCompScript := filepath.Join(pathname,rootcmd.Name()+"_bash_completion")
	rootcmd.GenBashCompletionFile(autoCompScript)
	gLog.Info.Printf("Generate bash completion script %s to",autoCompScript)
}


func SetLogLevel(viper viper.Viper,loglevel int) (int) {

	if loglevel == 0 {
		loglevel = viper.GetInt("logging.log_level")
	}
	verbose := viper.GetBool("verbose")
	if verbose {
		loglevel= 4
	}
	return loglevel

}

func GetLogOutput(viper viper.Viper) (string) {
	return viper.GetString("logging.output" )
}

func GetLogCombine(viper viper.Viper) (bool) {
	return viper.GetBool("logging.combine" )
}

func GetBucket(viper viper.Viper) (int) {
	return viper.GetInt("s3.bucket")
}

func GetNumberOfBucket(viper viper.Viper) (int) {
  return viper.GetInt("buckets.number")
}

func GetLevelDBUrl(viper viper.Viper) (string) {
	return viper.GetString("levelDB.url")
}

func GetBucketdUrl(viper viper.Viper) (string) {
	return viper.GetString("bucketd.url")
}
func GetWaitTime(viper viper.Viper) (time.Duration) {
	return viper.GetDuration("retry.waitime")
}

func GetRetryNumber(viper viper.Viper) (int) {
	return viper.GetInt("retry.number")
}

func GetTopology(viper viper.Viper) (string) {
	return viper.GetString("admin.topology")
}


func InitConfig(config string,viper viper.Viper,rootcmd cobra.Command) {

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

	logOutput:= GetLogOutput(viper)
	loglevel := viper.GetInt("log_level")
	autoCompletion := viper.GetBool("autocompletion")

	loglevel = SetLogLevel(viper,loglevel)

	gLog.InitLog(rootcmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		GenAutoCompletionScript(&rootcmd,configPath)
	}
	if metaEx := viper.GetString("meta.extension"); metaEx == "" {
		metaEx = "md"
	}


}