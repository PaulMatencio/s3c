
package cmd

import (
	"github.com/paulmatencio/s3c/gLog"
	st33 "github.com/paulmatencio/s3c/st33/utils"
	"github.com/spf13/cobra"
	"io"
	"path/filepath"
)

//  same as checkST33.go
var (

	readST33Cmd = &cobra.Command{
		Use:   "readST33",
		Short: "Command to read an ST33 input file",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			readST33(cmd,args)
		},
	}
)

func initRdFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&ifile,"data-file","i","","the St33 data file ")
	cmd.Flags().StringVarP(&idir,"input-directory","d","","the name of the directory")
	// cmd.Flags().StringVarP(&odir,"ofile","o","","output file")
}

func init() {

	RootCmd.AddCommand(readST33Cmd)
	initRdFlags(readST33Cmd)
}

//  Read ST33 files
func readST33(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(idir) == 0 {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}

	if r,err  := st33.NewSt33Reader(filepath.Join(idir,ifile)); err == nil {
		for {
			 b,err := r.Read()
			 gLog.Info.Printf(" Prev address :X'%x' -  Cur address : X'%x' - Record length: %d\n",r.Previous,r.Current,len(b))
			 if err == io.EOF {
			 	r.File.Close()
			 	break
			 }
		}
	} else {
		gLog.Error.Printf("%v",err)
	}
}
