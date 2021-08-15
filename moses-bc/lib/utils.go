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

func SetBucketName(prefix string, bucket string) string {
	var s string
	if len(prefix) > 0 {
		pref := strings.Split(prefix, "/")[0]
		if pref != "XP" {
			s = fmt.Sprintf("%02d", HashKey(pref, 5))
		} else {
			s = "05"
		}
	}
	gLog.Trace.Printf("Set bucket name %s ", bucket+"-"+s)
	return bucket + "-" + s
}

func HasSuffix(bucket string) bool {
	if bucket[len(bucket)-2:len(bucket)] >= "00" && bucket[len(bucket)-2:len(bucket)] <= "05" {
		return true
	}
	return false
}

func CheckBucketName(srcBucket string, tgtBucket string) error {
	var err error
	if srcBucket == tgtBucket {
		return errors.New(fmt.Sprintf("Target bucket %s and source bucket %s are the same", tgtBucket, srcBucket))

	} else {
		if len(srcBucket) > 2 && len(tgtBucket) > 2 {
			sl2 := srcBucket[len(srcBucket)-2 : len(srcBucket)]
			tl2 := tgtBucket[len(tgtBucket)-2 : len(tgtBucket)]
			if sl2 != tl2 {
				return errors.New(fmt.Sprintf("The suffix <%s> of source bucket %s  and the suffix <%s> target bucket %s are different", sl2, srcBucket, tl2, tgtBucket))

			}
		}
	}
	return err
}

func GetBucketSuffix(bucket string, prefix string) (error, string) {
	var (
		err error
		s   string
	)
	pref := strings.Split(prefix, "/")[0]
	if pref != "XP" {
		s = fmt.Sprintf("%02d", HashKey(pref, 5))
	} else {
		s = "05"
	}
	suf := bucket[len(bucket)-2 : len(bucket)]
	if suf >= "00" && suf <= "05" {
		if suf != s {
			return errors.New(fmt.Sprintf("For the given document id prefix %s, the suffix of the bucket %s should be %s\n\t\t\tYou can use the suffix commmand to detemine the suffix for a given prefix \n\t\t\tor just omit it,the correct suffix will be appended to the bucket name", prefix, bucket, s)), s
		} else {
			return err, ""
		}
	}

	return err, s
}

func HashKey(key string, modulo int) int {
	v := 0
	for k := 0; k < len(key); k++ {
		v += int(key[k])
	}
	return v % modulo
}

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
