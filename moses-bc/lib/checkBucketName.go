package lib

import (
	"errors"
	"fmt"
)

func CheckBucketName(srcBucket string, tgtBucket string) (error) {
	var err error
	if srcBucket == tgtBucket {
		return errors.New(fmt.Sprintf("Target bucket %s is the same as the source bucket %s", tgtBucket, srcBucket))

	} else {
		if len(srcBucket) > 2 && len(tgtBucket)>  2 {
			sl2 := srcBucket[len(srcBucket)-2]
			tl2 := tgtBucket[len(tgtBucket)-2]
			if sl2 != tl2 {
				return errors.New(fmt.Sprintf("The suffix (the last 2 characters) of source bucket: %s and target bucket: %s are different ", sl2, tl2))

			}
		}
	}
	return err
}