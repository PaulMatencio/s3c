// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lib

import (
	"errors"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"runtime"
	"strings"
	"time"
)


func ParseLog(line string) (error, string, string) {

	gLog.Trace.Println(line)
	if len(line) > 0 {
		L := strings.Split(line, " ")
		gLog.Trace.Println(L)
		if len(L) == 2 {
			gLog.Trace.Printf("Method %s Key %s", L[0], L[1])
			return nil, L[0], L[1]
		}
	}
	return errors.New(fmt.Sprintf("Invalid input parameter  <method> <key> in %s", line)), "", ""
}

func ParseInputKey(key string) (error, string) {
	var (
		err error
	)

	if len(key) <= 8 {
		err = errors.New(fmt.Sprintf("Invalid input key %s", key))
		return err,key
	}
	if _, err = time.Parse("2006-01-02", key[0:4] + "-" + key[4:6] + "-" + key[6:8]); err != nil {
		err = errors.New(fmt.Sprintf("Invalid input key %s - key does not start with yyyymmdd/", key))
		return err,key
	}
	return nil,  key[9:]
}

func ParseLoggerKey(key string) (error, string) {
	var (
		err error
	)

	if len(key) <= 11 {
		err = errors.New(fmt.Sprintf("Invalid logger key %s", key))
		return err,key
	}
	// replace "/" by "-"
	key1 := strings.Replace(key,"/","-",0)
	if _, err = time.Parse("2006-01-02", key1); err != nil {
		err = errors.New(fmt.Sprintf("Invalid input key %s - key does not start with yyyy/mm/dd/", key))
		return err,key
	}
	return nil,  key1[11:]
}



func Profiling(profiling int) {
	if profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				gLog.Info.Printf("PROFILING: System memory %d MB", float64(m.Sys)/1024/1024)
				gLog.Info.Printf("PROFILING: Heap allocation %d MB", float64(m.HeapAlloc)/1024/1024)
				gLog.Info.Printf("PROFILING: Total allocation %d MB", float64(m.TotalAlloc)/1024/1024)
				time.Sleep(time.Duration(profiling) * time.Second)
			}
		}()
	}
}


