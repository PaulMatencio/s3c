package lib

import (
	"errors"
	"fmt"
	"strings"
)



func GetBucketSuffix(bucket string, prefix string) (err error, s string) {

	pref := strings.Split(prefix, "/")[0]
	if pref != "XP" {
		s = fmt.Sprintf("%02d", HashKey(pref, 5))
	} else {
		s = "05"
	}
	suf := bucket[len(bucket)-2 : len(bucket)]
	if suf >= "00" && suf <= "05" {
		if suf != s {
			err = errors.New(fmt.Sprintf("For the given document id prefix %s, the suffix of the bucket %s should be %s\n\t\t\tYou can use the suffix commmand to detemine the suffix for a given prefix \n\t\t\tor just omit it,the correct suffix will be appended to the bucket name", prefix, bucket, s))
			return err, s
		} else {
			return err, ""
		}
	}

	return err, s
}

func HasSuffix(bucket string) bool {
	if bucket[len(bucket)-2:len(bucket)] >= "00" && bucket[len(bucket)-2:len(bucket)] <= "05" {
		return true
	}
	return false
}

func HashKey(key string, modulo int) (v int) {
	v = 0
	for k := 0; k < len(key); k++ {
		v += int(key[k])
	}
	return v % modulo
}
