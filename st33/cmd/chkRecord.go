package cmd

import (
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"io"
	"path/filepath"
	"strconv"
)

// readConvalCmd represents the readConval command
var (
	ST33 bool
	rl   int
	chKRecordCmd = &cobra.Command{
		Use:   "chkRecord",
		Short: "Command to check valid record of VB file",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			readVB(cmd,args)
		},
	}
)

func initVbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&ifile,"input-file","i","","the VB data file ")
	cmd.Flags().StringVarP(&idir,"input-directory","d","","the name of the directory")
	cmd.Flags().BoolVarP(&ST33,"st33","s",true,"Input format is ST33")
}

func init() {

	RootCmd.AddCommand(chKRecordCmd)
	initVbFlags(chKRecordCmd)
}

//  Read VB file
func readVB(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(idir) == 0 {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}

	n:= 0
	bad:= 0
	if vb,err := utils.NewVBRecord(filepath.Join(idir,ifile)); err == nil {
		for {
			b,err:= vb.Read()
			if ST33 {
				rl = 0
				if len(b) > 5 {
					RL := utils.Ebc2asci(b[0:5])
					rl, _ = strconv.Atoi(string(RL[0:5]))
					if len(b) != rl {
						gLog.Error.Printf("Wrong ST33 record - Record lengths differ rdw=%d  not equal size of the record : %d", len(b), rl)
						bad++
					}
				}
			}
			gLog.Trace.Printf("Previous address : X'%x'  Current address : X'%x' - Record length: %d %d\n",vb.Previous,vb.Current,len(b),rl)
			if err == io.EOF {
				vb.File.Close()
				gLog.Info.Printf("Total number of records: %d %d",n,bad)
				break
			} else {
				n++
				/* e:= []byte("\n")
				vb.OutFile.Write(append(b,e[0]))
				*/
			}
		}
	} else {
		gLog.Error.Printf("%v %v ",err)
	}
}
