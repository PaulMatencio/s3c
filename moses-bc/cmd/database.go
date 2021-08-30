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
var (
	databaseCmd = &cobra.Command{
		Use:   "database",
		Short: "Test badger local key value storage",
		Long:  `Test badger local key value storage.`,
		Run:   DataBase,
	}
	nameSpace string
	dbName    string
	key       string
	value     string
)

func initDBFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().StringVarP(&method, "method", "m", "get", "database operations [get|put|list] ")
	cmd.Flags().StringVarP(&key, "key", "k", "", "key of the new item ")
	cmd.Flags().StringVarP(&value, "value", "v", "", "value of the new item  ")
	cmd.Flags().StringVarP(&dbName, "db-name", "d", "", "database names")
	cmd.Flags().StringVarP(&nameSpace, "name-space", "n", "backup", "key name space ")

}

func init() {
	rootCmd.AddCommand(databaseCmd)
	initDBFlags(databaseCmd)
}

func DataBase(cmd *cobra.Command, args []string) {

	var (
		dir, h    string
		err       error
		mydb      db.DB
		namespace = []byte(nameSpace)
	)

	marker = ""
	if method == "get" || method == "put" || method == "delete" {
		if len(key) == 0 {
			gLog.Error.Printf("missing key")
			return
		} else {
			key += marker
		}
	}

	if method == "put" && len(value) == 0 {
		gLog.Error.Printf("missing value for method %s", method)
		return
	}

	// open a data base
	if len(dbName) == 0 {
		gLog.Error.Printf("Data  base name is missing")
		return
	}

	if h, err = os.UserHomeDir(); err == nil {
		dir = filepath.Join(h, dbName)
	}
	if mydb, err = db.NewBadgerDB(dir, nil); err != nil {
		gLog.Error.Printf("Error %v", err)
	}
	defer mydb.Close()

	switch method {
	case "put":
		if err = mydb.Set(namespace, []byte(key), []byte(value)); err != nil {
			fmt.Println(err)
		} else {
			gLog.Trace.Printf("Key:%s Value:%s", string(key), string(value))
		}
	case "get":
		if value, err := mydb.Get(namespace, []byte(key)); err == nil {
			fmt.Println(string(value))
		} else {
			fmt.Println(err)
		}
	case "delete":
		if  err := mydb.Delete(namespace, []byte(key)); err == nil {
			fmt.Printf("key %s is deleted\n", string(key))
		} else {
			fmt.Println(err)
		}
	case "list":
		if len(prefix) == 0 {
			prefix = ""
		}
		if err = mydb.List(namespace, []byte(prefix)); err != nil {
			fmt.Printf("%v", err)
		}
	default:
		prefix = ""
		if err = mydb.List(namespace, []byte(prefix)); err != nil {
			fmt.Printf("%v", err)
		}
	}
}
