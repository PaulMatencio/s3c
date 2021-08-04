package lib

import (
	"encoding/base64"
	"fmt"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"strconv"
)
func InspectBlobs(document *documentpb.Document,  maxPage int) {
	if document.NumberOfPages <= int32(maxPage) {
		inspect_regular_blob(document)
	} else {
		inspect_large_blob(document,maxPage)
	}
}

func inspect_regular_blob(document *documentpb.Document) {

	pages := document.GetPage()
	for _, pg := range pages {
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s -Page Size %d\n",pageid,pg.Size)
		pagemeta,_ :=  base64.StdEncoding.DecodeString(pg.Metadata)
		fmt.Printf("\t\tPage metadata %s\n",pagemeta)
		fmt.Println(pg.PageId,pg.PageNumber,pg.Metadata,pg.Size)
	}

}


func inspect_large_blob(document *documentpb.Document,maxPage int) {
	var (
		np = int (document.NumberOfPages)
		q     int = np  / maxPage
		r     int = np  % maxPage
		start int = 1
		end   int = start + maxPage-1
	)



	for s := 1; s <= q; s++ {
		inspect_part_large_blob(document,start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		inspect_part_large_blob(document,q*maxPage+1 , np)
	}

}


func inspect_part_large_blob(document *documentpb.Document,start int,end int)  {

	var (
		pages = document.GetPage()
	)
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s -Page Size %d\n",pageid,pg.Size)
		pagemeta,_ :=  base64.StdEncoding.DecodeString(pg.Metadata)
		fmt.Printf("\t\tPage metadata %s\n",pagemeta)
	}
}
