package utils

import (
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

// Return home directory
func GetHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}


// create a directory if it does not exist
func MakeDir(dir string) {
	if _,err:= os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir,0755)
	}
}

//  return all  data files of a directory
func ReadDataDir(dir string) ([]os.FileInfo,error) {
	var df []os.FileInfo
	fis,err := ioutil.ReadDir(dir)
	if err == nil {
		for _,fi := range fis {
			if !fi.IsDir() && !strings.Contains(fi.Name(),viper.GetString("meta.extension")) {
				df = append(df,fi)
			}
		}
	}
	return df,err
}