package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
)

// getObjectCmd represents the getObject command
var (
	goshort = "Command to fetch an object from a given bucket"
	// odir   string
	getObjectCmd = &cobra.Command {
		Use:   "getObj",
		Short: goshort,
		Long: ``,
		Run: getObject,
	}

	getObjCmd = &cobra.Command {
		Use:   "go",
		Short: goshort,
		Long: ``,
		Hidden: true,
		Run: getObject,
	}

	fgetObjCmd = &cobra.Command {
		Use:   "fgetObj",
		Short: "Command to download an objet from a given bucket to a file",
		Long: ``,
		Run: fGetObject,
	}
)

func initGoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}

func initFgoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","tha name of the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
	cmd.Flags().StringVarP(&odir,"odir","O","","the ouput directory relative to the working (or Home ir omitted)  directory you'like to save")

}

func init() {

	RootCmd.AddCommand(getObjectCmd)
	RootCmd.AddCommand(getObjCmd)
	RootCmd.AddCommand(fgetObjCmd)
	RootCmd.MarkFlagRequired("bucket")
	RootCmd.MarkFlagRequired("key")
	initGoFlags(getObjectCmd)
	initGoFlags(getObjCmd)
	initFgoFlags(fgetObjCmd)


}


//  getObject utilizes the api to get object

func getObject(cmd *cobra.Command,args []string) {

	// handle any missing args
	start := utils.LumberPrefix(cmd)

	switch {
	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return
	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return
	}

	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key : key,
	}
	result, err := api.GetObject(req)

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("error [%v]",aerr.Error())
			}
		} else {
			gLog.Error.Printf("[%v]",err.Error())
		}
	} else {

		utils.PrintUsermd(req.Key,result.Metadata)
		b, err := utils.ReadObject(result.Body)
		if err == nil {
			gLog.Info.Printf("Key: %s  - ETag: %s  - Content length: %d - Object lenght: %d",key,*result.ETag,*result.ContentLength,b.Len())
		}
		utils.Return(start)
	}
}

func fGetObject(cmd *cobra.Command,args []string) {

	var (
		err  error
		result *s3.GetObjectOutput
		start = utils.LumberPrefix(cmd)
	)
	// handle any missing args

	switch {
	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return
	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return

	}

	if len(odir) == 0 {
		gLog.Warning.Printf("%s", missingOutputFolder)
		odir,_ = os.UserHomeDir()
	} else {
		if sep := strings.Split(odir, string(os.PathSeparator)); len(sep) == 1 {
			cwd, _ := os.Getwd()
			odir = filepath.Join(cwd, odir)
		}
		if !utils.Exist(odir){
			utils.MakeDir(odir)
		}
	}

	pathname := filepath.Join(odir,strings.Replace(key,string(os.PathSeparator),"_",-1))
    gLog.Trace.Printf("Output pathname: %s",pathname)
	//  build a request
	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key : key,
	}
	// get the object
	result, err = api.GetObject(req);
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("error [%v]",aerr.Error())
			}
		} else {
			gLog.Error.Printf("[%v]",err.Error())
		}
	} else {
		if err = utils.SaveObject(result,pathname); err == nil {
			gLog.Info.Printf("Object %s is downloaded to %s",key,pathname)
		} else {
			gLog.Error.Printf("Saving %s Error %v ",key,err)
		}
		// utils.PrintUsermd(req.Key,result.Metadata)
		utils.WriteUsermd(result.Metadata,pathname+".md")
	}
	utils.Return(start)
}







