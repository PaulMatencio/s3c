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
	CONTIMEOUT = 2000  // connection timeout in ms
	KEEPALIVE = 15000  // keep alive  in ms
	HTTP = "http://"
)

var (
	Config string
	bucket,prefix	 string
	verbose, Debug,autoCompletion 	 bool
	loglevel,profiling, bucketNumber, retryNumber  int
	waitTime time.Duration
	srcBucket, tgtBucket              string
	srcEnv, tgtEnv 					 string
	check        bool
	listpn     *bufio.Scanner
	tgtS3, srcS3 , logS3 *s3.S3
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
	missingBucket = "Missing S3 bucket --bucket argument. Use  --help or -h for help"
	/* */
	uSrcBucket = "full (with suffix) or partial name (without suffix) of the source s3 bucket. See documentation for details"
	uTgtBucket = "full (with suffix) or partial name (without suffix) of the target s3 bucket.  See documentation for details"
	uIndBucket = "full (with suffix) or partial name (without suffix) of the directory s3 bucket. See documentation for details"
	uPrefix = "prefix limits the list of results to only those keys that begin with the specified prefix. See documentation for details"
	uMaxkey = "maximum number of documents to be concurrently processed -  maximum concurrent operations = (--max-key X --max-page)"
	uMaxPage = "maximum number of pages per document to be concurrently processed - maximum concurrent operations = (--max-key X --max-page)"
	uMaxLoop ="maximum number of loop, 0 means no upper limit which means the full list will be processed"
	uMaker ="Marker is the key of the source or input bucket where you want S3 to start listing from"
	uMaxPartSize="maximum multipart' partition size (MB) for multipart processing"
	uFromDate ="process only objects which are modified after <yyyy-mm-ddThh:mm:ss>"
	uToDate ="process only objects which are modified before <yyyy-mm-ddThh:mm:ss>"
	uMaxVerion ="maximum number of versions if bucket versioning is enabled"
	uInputFile ="input file used for incremental process - record format: [PUT|GET|DELETE] CC/PN/KC"
	uInputBucket ="input bucket used for incremental process - key format: YYYYMMDD/CC/PN/KC or YYYY/MM/DD/CC/PN/KC"
	uSrcDriver = "source Sproxyd driver [bpchord|bparc]"
	uSrcUrl = "source Sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy"
	uSrcEnv = "source Sproxyd environnement  [prod|osa|intg]"
	uTgtDriver = "target Sproxyd driver [bpchord|bparc]"
	uTgtUrl = "target Sroxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy"
	uTgtEnv = "target Sproxyd environnement  [prod|osa|intg]"
	uReplace ="replace existing Sproxyd objects if they exist"
	uLogit = "enable backup history log"
	uReIndex = "re-index and update the directory after this process"
	uVersionId="the version id of the S3 object to be processed - default the last version will be processed"
	uCtimeout = "set the cancel timeout (sec)  for the background context"
	uDryRun ="Dry run for testing"
	uNameSpace = "badger namespace"
	uDatabase = "badger database name"
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
	rootCmd.PersistentFlags().StringVarP(&Config,"config", "c","", "config file; default $HOME/.clone/config-bc.yaml")
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
	if len(Config) > 0  {
		// Use config file from the application flag.
		log.Printf("Setting Config file to %s",Config)
		viper.SetConfigFile(Config)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatalln(err)
		}
		configPath = filepath.Join("/etc/clone") // call multiple times to add many search paths
		viper.AddConfigPath(configPath)            // another path to look for the config file
		configPath = filepath.Join(home,".clone")
		viper.AddConfigPath(configPath)
		viper.AddConfigPath(".")               // optionally look for config in the working directory
		viper.SetConfigName("config-bc")          // name of the config file without the extension
		viper.SetConfigType("yaml")
		//viper.SetConfigFile( filepath.Join(configPath,"config.yaml"))
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


	gLog.InitLog(rootCmd.Name(),loglevel,logOutput)
	log.Printf("Logging level: %d   Output: %s",loglevel,logOutput)

	if  autoCompletion {
		utils.GenAutoCompletionScript(rootCmd,configPath)
	}

}