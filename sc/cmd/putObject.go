

package cmd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"time"
)

// putObjectCmd represents the putObject command
var (
	pfshort = "Command to upload a given file to a bucket"
	poshort = "Command to upload a byte buffer to a bucket"
	datafile,metafile string
	absolute bool
	fPutObjectCmd = &cobra.Command{
		Use:   "fPutObject",
		Short: pfshort,
		Long: ``,
		Hidden: true,
		Run: fPutObject,
	}

	putObjectCmd = &cobra.Command{
		Use:   "putObj",
		Short: poshort,
		Long: ``,
		Run: putObject,
	}

	fPoCmd = &cobra.Command{
		Use:   "fputObj",
		Short: pfshort,
		Long: ``,
		Run: fPutObject,
	}

	poCmd = &cobra.Command{
		Use:   "po",
		Short: poshort,
		Long: ``,
		Hidden: true,
		Run: putObject,
	}
)

func initPfFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of  the bucket")
	cmd.Flags().StringVarP(&datafile,"datafile","i","","the data file you 'd like to  upload")
	cmd.Flags().BoolVarP(&absolute,"absolute","a",false,"key = absolute path name")
	// cmd.Flags().StringVarP(&metafile,"metafile","m","","the meta file to upload")
}

func init() {

	RootCmd.AddCommand(fPutObjectCmd)
	RootCmd.AddCommand(fPoCmd)
	//rootCmd.AddCommand(putObjectCmd)
	initPfFlags(fPutObjectCmd)
	initPfFlags(fPoCmd)
}


func fPutObject(cmd *cobra.Command, args []string) {

	var (
	start = utils.LumberPrefix(cmd)
	svc  *s3.S3
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		gLog.Warning.Printf("%s",missingInputFile)
		utils.Return(start)
		return
	}

	cwd,_:= os.Getwd()
	datafile = filepath.Join(cwd,datafile)
	svc      = s3.New(api.CreateSession())

	if result,err := fPutObj(svc ,datafile); err == nil {
		gLog.Info.Printf("Successfully upload file %s to  Bucket %s  - Etag : %s", datafile,bucket,*result.ETag)
	} else {
		gLog.Error.Printf("fail to upload %s - error: %v",datafile,err)
	}

	utils.Return(start)

}

// called by fputObj command
func fPutObj(svc *s3.S3 , datafile string) (*s3.PutObjectOutput,error) {

	var (

		metafile = datafile+"."+ metaEx
		_,key = filepath.Split(datafile)
		usermd = map[string]string{}
		err error

	)
	if absolute {
		key =  datafile
	}
	gLog.Info.Print(key)
	if  usermd,err  = utils.ReadUsermd(metafile); err != nil  {
		gLog.Error.Printf("Error %v reading meta data file %s",err,metafile)
	}

	req:= datatype.FputObjRequest{
		// Service : s3.New(api.CreateSession()),
		Service: svc,
		Bucket: bucket,
		Key: key,
		Inputfile: datafile,
		Usermd : usermd,

	}
	return api.FputObject2(req)
}


func putObject(cmd *cobra.Command, args []string) {

	var (
		//buffer *bytes.Buffer
		start = utils.LumberPrefix(cmd)
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		gLog.Warning.Printf("%s",missingInputFile)
		utils.Return(start)
		return
	}

	cwd,_:= os.Getwd()
	datafile = filepath.Join(cwd,datafile)
	if result,err := putObj(datafile); err == nil {
		gLog.Info.Printf("Successfuly upload file %s to  Bucket %s  - Etag : %s", datafile,bucket,*result.ETag)
	} else {
		gLog.Error.Printf("fail to upload %s - error: %v",datafile,err)
	}

	utils.Return(start)

}


func putObj(datafile string) (*s3.PutObjectOutput,error) {

	var (

		metafile = datafile+"."+ metaEx
		_,key = filepath.Split(datafile)
		meta []byte
		data []byte
		err error
		start = time.Now()
	)

	// read Meta file into meta []byte
	if  meta,err  = utils.ReadFile(metafile); err != nil || len(meta) == 0 {
		gLog.Info.Printf("no user metadata  %s",metafile)
	}

	// Read data file into data []byte

	if  data,err  := utils.ReadFile(datafile); err != nil || len(data) == 0 {
		gLog.Error.Printf("Error %v reading %s",err,datafile)
		utils.Return(start)
	}

	req:= datatype.PutObjRequest{

		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key: key,
		Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Meta : meta,

	}

	return api.PutObject(req)
}
