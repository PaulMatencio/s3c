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
	"github.com/paulmatencio/s3c/gLog"
	db "github.com/paulmatencio/s3c/moses-bc/db"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// databaseCmd represents the database command
var databaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Test badger local key value storage",
	Long: `Test badger local key value storage.`,
	Run: DataBase,
}

func init() {
	rootCmd.AddCommand(databaseCmd)
}

func DataBase(cmd *cobra.Command, args []string) {

	var (
		dir, op, h string
		err        error
		key, value string
		mydb       db.DB
		nameSpace  = []byte("backup")
		marker =  "/next-marker"
	)
	// open a data base
	if len(args) < 1 {
		fmt.Println("Missing op argument [get|put|list] ")
		return
	} else {
		op = args[0]
		switch op {
		case "put":
			if len(args) < 3 {
				fmt.Printf("Missing key/value argument for op %s", op)
				return
			} else {
				key = args[1]+marker
				value = args[2]
			}
		case "get":
			if len(args) < 2 {
				fmt.Printf("Missing key  argument for op %s", op)
				return
			} else {
				key = args[1]+marker
			}
		case "list":
			if len(args) >= 2 {
				prefix = args[1]
			} else {
				prefix = ""
			}
		}
	}

	if h, err = os.UserHomeDir(); err == nil {
		dir = filepath.Join(h, "mydb")
	}

	if mydb, err = db.NewBadgerDB(dir, nil); err != nil {
		gLog.Error.Printf("Error %v", err)
	}
	defer mydb.Close()

	switch op {
	case "put":
		if err = mydb.Set(nameSpace, []byte(key), []byte(value)); err != nil {
			fmt.Println(err)
		} else {
			gLog.Trace.Printf("Key:%s Value:%s", string(key), string(value))
		}
	case "get":
		if value, err := mydb.Get(nameSpace, []byte(key)); err == nil {
			fmt.Println(string(value))
		} else {
			fmt.Println(err)
		}
	case "list":
		if len(prefix) == 0 {
			prefix = ""
		}
		if err = mydb.List(nameSpace, []byte(prefix)); err != nil {
			fmt.Printf("%v", err)
		}
	default:
		prefix = ""
		if err = mydb.List(nameSpace, []byte(prefix)); err != nil {
			fmt.Printf("%v", err)
		}
	}
}
