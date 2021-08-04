package lib

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	"strconv"
)
func InspectBlobs(document *documentpb.Document,  maxPage int, verbose bool) {
	if document.NumberOfPages <= int32(maxPage) {
		inspect_regular_blob(document,verbose)
	} else {
		inspect_large_blob(document,maxPage,verbose)
	}
}

func inspect_regular_blob(document *documentpb.Document,verbose bool) {

	pages := document.GetPage()
	for _, pg := range pages {
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s - Page Size %d\n",pageid,pg.Size)
		pagemeta,_ :=  base64.StdEncoding.DecodeString(pg.Metadata)
		if verbose {
			fmt.Printf("\t\tPage metadata %s\n", pagemeta)
		} else {
			pagmeta := meta.Pagemeta{}
			json.Unmarshal([]byte(pagemeta), &pagmeta)
			fmt.Printf("\t\tPage Number %d - Length %d - Png %v - Tiff %v - Pdf %v\n",pagmeta.PageNumber,pagmeta.PageLength,pagmeta.PubId,pagmeta.MultiMedia.Png,pagmeta.MultiMedia.Tiff,pagmeta.MultiMedia.Pdf)
		}

	}

}


func inspect_large_blob(document *documentpb.Document,maxPage int,verbose bool) {
	var (
		np = int (document.NumberOfPages)
		q     int = np  / maxPage
		r     int = np  % maxPage
		start int = 1
		end   int = start + maxPage-1
	)



	for s := 1; s <= q; s++ {
		inspect_part_large_blob(document,start, end,verbose)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		inspect_part_large_blob(document,q*maxPage+1 , np,verbose)
	}

}


func inspect_part_large_blob(document *documentpb.Document,start int,end int,verbose bool)  {

	var (
		pages = document.GetPage()
	)
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s -Page Size %d\n",pageid,pg.Size)
		pagemeta,_ :=  base64.StdEncoding.DecodeString(pg.Metadata)
		if verbose {
			fmt.Printf("\t\tPage metadata %s\n", pagemeta)
		}  else {

		}
	}
}
