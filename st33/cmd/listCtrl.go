
package cmd

import (
	"bufio"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/st33/utils"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// readConvalCmd represents the readConval command
var (
	ofile, pfile string
	lsCtrlCmd = &cobra.Command{
	Use:   "lsCtrl",
	Short: "Command to list a control file",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		listConval(cmd,args)
	},
})


func initLvFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&ifile,"ifile","i","","The full pathname of an input ST33 control file")
	cmd.Flags().StringVarP(&ofile,"ofile","o","","The output pathname relative to the home directory")
	cmd.Flags().StringVarP(&pfile,"pfile","q","","The output pathname relative to the home directory containing only pxi ids")

}


func init() {

	RootCmd.AddCommand(lsCtrlCmd)
	initLvFlags(lsCtrlCmd)


}

func listConval(cmd *cobra.Command, args []string) {

	var (

		home,pathname,file  string
		of,pf  *os.File
		err error
		w,p  *bufio.Writer
		records int
	)


	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(ofile) != 0 {

        home = utils.GetHomeDir()
        pathname = filepath.Join(home,ofile)
        if _,err = os.Stat(pathname); os.IsNotExist(err) {
        	os.Create(pathname)
		}
		if of, err = os.OpenFile(pathname, os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			gLog.Warning.Printf("Error %v opening file %s",pathname,err)
			os.Exit(1)
		} else {
			w = bufio.NewWriter(of)
			defer of.Close()
		}
	}

	if len(pfile) != 0 {

		home = utils.GetHomeDir()
		pathname = filepath.Join(home,pfile)
		if _,err = os.Stat(pathname); os.IsNotExist(err) {
			os.Create(pathname)
		}
		if pf, err = os.OpenFile(pathname, os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			gLog.Warning.Printf("Error %v opening file %s",pathname,err)
			os.Exit(1)
		} else {
			p = bufio.NewWriter(pf)
			defer of.Close()
		}
	}

	_,file = filepath.Split(ifile)

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {
		for k, v := range *c {
			imageType := "TIFF"
			lp := len(v.PxiId)

			if v.PxiId[lp-2:lp-1] == "B" {
				imageType = "BLOB"
			}
			records += v.Records
			if len(ofile) == 0  &&  len(pfile) == 0 {
				gLog.Trace.Printf("%d %s %s  Pages: %d   records: %d ... %s",k, v.PxiId, utils.Reverse(v.PxiId), v.Pages,v.Records,imageType)
			}

			if len(ofile) > 0 {
				fmt.Fprintln(w, file, k, v.PxiId, utils.Reverse(v.PxiId), v.Pages,v.Records,imageType)
			}

			if len(pfile) > 0 {
				fmt.Fprintf(p,v.PxiId+" ")
			}

		}
		 if w != nil {
			 w.Flush()
		 }


	} else {
		gLog.Error.Println(err)
	}
    gLog.Info.Printf("Total number of records: %d",records)

}
