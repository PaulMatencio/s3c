

package cmd

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/db"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"path/filepath"
	"time"
)

// st33ToFilesCmd represents the st33ToFiles command
var (
	blob,DB string
	database *badger.DB
	toFiles2Cmd = &cobra.Command{
		Use:   "toFiles",
		Short: "Command to extract an ST33 file then write to local files",
		Long: `Command to extract an ST33 file containing Tiff images and Blobs then write to  files`,
		Run: func(cmd *cobra.Command, args []string) {
			toFilesFunc(cmd,args)
		},
	}

	)

func initTfFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 input file containing st33 formated data")
	cmd.Flags().StringVarP(&odir,"odir","O","","output directory of the extraction")
	cmd.Flags().StringVarP(&blob,"blob","B","","Blob output folder relative to the output directory")
	cmd.Flags().StringVarP(&DB,"DB","D","","name of the badger database")
	cmd.Flags().BoolVarP(&check,"test-mode","t",false,"test mode")
}

func init() {
	RootCmd.AddCommand(toFiles2Cmd)
	initTfFlags(toFiles2Cmd)


}


func toFilesFunc(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}
	if !check {
		if len(odir) > 0 {
			pdir = filepath.Join(utils.GetHomeDir(), odir)
			utils.MakeDir(pdir)
			gLog.Info.Printf("Files are written to : %s", pdir)
		} else {
			gLog.Info.Printf("%s", missingOutputFolder)
			return
		}

		bdir = pdir
		if len(blob) > 0 {
			bdir = filepath.Join(pdir, blob)
			utils.MakeDir(bdir)
		}
	} else {
		gLog.Info.Print("Running is test mode")
	}

	//  open badger database
	if len(DB) > 0 {
		database,_ := db.OpenBadgerDB(DB)
		kv := map[string]string{
			"st33" : ifile,
			"start" : fmt.Sprintf("%v",time.Now()),
		}
		db.UpdateBadgerDB(database,kv)
	}

	gLog.Info.Printf("Processing input file %s",ifile)
	if numpages,numrecs,numdocs,numerrors,err:=  st33.ToFilesV1(ifile,odir,bdir, check); err != nil {
		gLog.Error.Printf("%v",err)
	} else {
		gLog.Info.Printf("%d documents - %d pages - %d records were processed. Number errors %d", numdocs, numpages, numrecs, numerrors)
	}

}
