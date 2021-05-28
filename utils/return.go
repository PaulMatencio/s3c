package utils

import (
	"github.com/paulmatencio/s3c/gLog"
	"time"
)

func Return(start time.Time) {
	gLog.Info.Printf("Elapsed time %s",time.Since(start))
	// LumberPrefix(nil)
}

