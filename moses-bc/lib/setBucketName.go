package lib

import (
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"strings"
	"errors"
)

func SetBucketName(prefix string, bucket string) (name string) {
	var (
		s string
		pref string
	)
	if len(prefix) > 0 {
		pref = strings.Split(prefix, "/")[0]
		if pref != "XP" {
			s = fmt.Sprintf("%02d", HashKey(pref, 5))
		} else {
			s = "05"
		}
	}
	gLog.Trace.Printf("Hashed pref %s - Set bucket name %s ",pref, bucket+"-"+s)
	name = bucket + "-" + s
	return
}

func SetBucketName1(condition bool, pn string, bucket string ) (buck1 string) {

		if  condition {
			buck1 = SetBucketName(pn, bucket)
		} else {
			buck1 = bucket
		}
	return
}

func CheckBucketName(srcBucket string, tgtBucket string) (err error) {

	if srcBucket == tgtBucket {
		err = errors.New(fmt.Sprintf("Target bucket %s and source bucket %s are the same", tgtBucket, srcBucket))
		return err

	} else {
		if len(srcBucket) > 2 && len(tgtBucket) > 2 {
			sl2 := srcBucket[len(srcBucket)-2 : len(srcBucket)]
			tl2 := tgtBucket[len(tgtBucket)-2 : len(tgtBucket)]
			if sl2 != tl2 {
				err = errors.New(fmt.Sprintf("The suffix <%s> of source bucket %s  and the suffix <%s> target bucket %s are different", sl2, srcBucket, tl2, tgtBucket))
				return err

			}
		}
	}
	return err
}