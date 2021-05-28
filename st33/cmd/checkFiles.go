
package cmd

import (
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strconv"
)

// readConvalCmd represents the readConval command
var (
	idir string
checkFilesCmd = &cobra.Command{
	Use:   "chkFiles",
	Short: "Command to check if all content of a given st33 file has been written to a folder",
	Long: `Command to check if all the  Tiff images and Blobs of a given st33 data file have been written to a folder`,
	Run: func(cmd *cobra.Command, args []string) {
		checkFiles(cmd,args)
	},
}
)

func initCfFlags(cmd *cobra.Command) {


	cmd.Flags().StringVarP(&ifile,"ifile","i","","the corresponding control file that was used to migrate the data file")
	cmd.Flags().StringVarP(&idir,"idir","I","","the name of the folder you would like to check against a given data file")
	// cmd.Flags().StringVarP(&odir,"ofile","o","","output file")
}


func init() {

	RootCmd.AddCommand(checkFilesCmd)
	initCfFlags(checkFilesCmd)


}

func checkFiles(cmd *cobra.Command, args []string) {

	var (
		pages,w  int=0,0
		p int

	)
	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(idir) == 0 {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {

		for k, v := range *c {

			key := utils.Reverse(v.PxiId)
			metafile := filepath.Join(idir,key+".1.md")
			if _, err  := os.Stat(metafile); err == nil {
				if  usermd,err  := utils.ReadUsermd(metafile); err != nil  {
					gLog.Error.Printf("Index %d  - Error %v reading meta data file %s  for key: %s&/%s",k,err,metafile,v.PxiId, key)
				} else {
					if pg,ok := usermd["Pages"];ok {
						p,_ = strconv.Atoi(pg)
						vp := int(v.Pages)
						if vp != 0 && p != vp {
							gLog.Warning.Printf("Index %d - Conval Key %s -  S3 Key %s  - Conval pages %d !=  folder pages %d",k, v.PxiId,key ,v.Pages,p)
							w ++
						} else {
							gLog.Trace.Printf("checking  pages number of key %s %d %d ",key,p,v.Pages)
						}
					} else {
						gLog.Trace.Println(usermd)
					}
				}

				pages += p

			} else {
				gLog.Error.Printf("Index: %d - Error %v while checking the metadata for key: %s/%s",k, err,v.PxiId,key)
				w++
			}
		}

		if w ==  0 {
			gLog.Info.Printf("%d documents, %d  pages have been migrated without error",len(*c),pages)
		}
	} else {
		gLog.Error.Println(err)
	}
}
