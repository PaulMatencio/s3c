package lib

var (
	MaxPage int  		//  Number of max pages to be concurrentlz processed
	MaxPageSize int64 	//  maximum page size for multipart upload or dowbload
	MaxPartSize int64
	MaxCon  int  		// Maximum number of concurrent upload /download
	PartNumber int64  	 //number of  parts in a multi par upload /download
	Replace bool
)

const(
	MinPartSize = 1024*1014*5    // 5 MB
)
