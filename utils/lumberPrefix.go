package utils

import (
	"github.com/spf13/cobra"
	"time"
)

func LumberPrefix(cmd *cobra.Command) (time.Time){
    /*
	prefix := "[sc]"
	if cmd != nil {
		prefix = cmd.Name()
	}
	lumber.Prefix("[" + prefix + "]")
    */
	return time.Now()
}
