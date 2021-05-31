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
	"fmt"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	clone "github.com/paulmatencio/s3c/clone/lib"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"time"
)

// restoreMosesCmd represents the restoreMoses command
var (
	pn,inDir string
	restoreCmd = &cobra.Command{
	Use:   "restoreMoses",
	Short: "Command to restore",
	Long: ``,
	Run: restore,
}
)
func initResFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	// cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	// cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 100, "maximum number of keys to be processed concurrently")
	//cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	// cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().StringVarP(&inDir, "inDir", "I", "", "input directory")
	cmd.Flags().StringVarP(&outDir, "outDir", "O", "", "outputdirectory")
	cmd.Flags().StringVarP(&pn, "pn", "k", "", "publication number")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	// cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages ")
	// cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
}


func init() {
	rootCmd.AddCommand(restoreCmd)
	initResFlags(restoreCmd)

}

func restore(cmd *cobra.Command, args []string) {
	var (
		document *documentpb.Document
		err error
	)
	start := time.Now()
	if document,err  = clone.ReadDocument(pn,inDir); err == nil {
		pages := document.GetPage()
		gLog.Info.Printf("Document id %s - Page Numnber %d ",document.DocId,document.PageNumber)
		if usermd, err := base64.Decode64(document.GetMetadata()); err == nil {
			gLog.Info.Printf("Document metadata %s",string(usermd))
		} else {
			gLog.Error.Println(err)
		}

		if len(pages) != int(document.NumberOfPages) {
			gLog.Error.Printf("Backup of document is inconsistent %s  %d - %d ", pn,len(pages),document.NumberOfPages)
			os.Exit(100)
		}
		for _, page := range pages {
			//object := page.GetObject()
			pfn := pn + "_" + fmt.Sprintf("%04d", page.GetPageNumber())
			if fi, err := os.OpenFile(filepath.Join(outDir, pfn), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
				defer fi.Close()
				bytes := page.GetObject()
				if _, err := fi.Write(bytes); err != nil {
					fmt.Printf("Error %v writing page %d", err, pfn)
				}
			}
		}
		fmt.Println(document.NumberOfPages)
		fmt.Println(document.Metadata)
		fmt.Println(document.LastUpdated)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

