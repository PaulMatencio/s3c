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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	"net/http"
	"strconv"
)

const (
	LEHeader = "II\x2A\x00" // Header for little-endian files.
	BEHeader = "MM\x00\x2A" // Header for big-endian files.  4D 4D 00  2A
	ifdLen = 10 // Length of an IFD entry in bytes.
)
func InspectBlobs(document *documentpb.Document, maxPage int, verbose bool) {
	if document.NumberOfPages <= int32(maxPage) {
		inspectBlob(document, verbose)
	} else {
		inspectLargeBlob(document, maxPage, verbose)
	}
}

func inspectBlob(document *documentpb.Document, verbose bool) {

	var (
		pages       = document.GetPage()
		tiff, png   string
		tiffl, pngl int
	)

	for _, pg := range pages {
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s - Page Size %d - Object Size %d\n", pageid, pg.Size, len(pg.Object))
		pagemeta, _ := base64.StdEncoding.DecodeString(pg.Metadata)
		if verbose {
			fmt.Printf("\t\tPage metadata %s\n", pagemeta)
		} else {
			pagmeta := meta.Pagemeta{}
			json.Unmarshal([]byte(pagemeta), &pagmeta)
			if pagmeta.MultiMedia.Tiff {
				if bytes.Compare(pg.Object[0:4],[]byte(LEHeader)) == 0 {
					tiff = "image/tiff"
				} else {
					tiff = http.DetectContentType(pg.Object[pagmeta.TiffOffset.Start:pagmeta.TiffOffset.End])
					fmt.Printf("%s %s\n",pg.Object[pagmeta.TiffOffset.Start:pagmeta.TiffOffset.Start+3],LEHeader)
				}
				tiffl = pagmeta.TiffOffset.End - pagmeta.TiffOffset.Start + 1
				/* 4D 4D 00 4A   */

			}
			if pagmeta.MultiMedia.Png {
				png = http.DetectContentType(pg.Object[pagmeta.PngOffset.Start:pagmeta.PngOffset.End])
				pngl = pagmeta.PngOffset.End - pagmeta.PngOffset.Start + 1
			}
			fmt.Printf("\t\tPage Number: %d - Page Length: %d - Png %v:%s:%d - Tiff %v:%s:%d\n", pagmeta.PageNumber, pagmeta.PageLength, pagmeta.MultiMedia.Png, png, pngl, pagmeta.MultiMedia.Tiff, tiff, tiffl)
		}
	}
}

func inspectLargeBlob(document *documentpb.Document, maxPage int, verbose bool) {

	var (
		np        = int(document.NumberOfPages)
		q     int = np / maxPage
		r     int = np % maxPage
		start int = 1
		end   int = start + maxPage - 1
	)

	for s := 1; s <= q; s++ {
		inspectLargeBlobPart(document, start, end, verbose)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		inspectLargeBlobPart(document, q*maxPage+1, np, verbose)
	}

}

func inspectLargeBlobPart(document *documentpb.Document, start int, end int, verbose bool) {

	var (
		pages       = document.GetPage()
		tiff, png   string
		tiffl, pngl int
	)
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		pgn := int(pg.PageNumber)
		pageid := pg.PageId + "/p" + strconv.Itoa(pgn)
		fmt.Printf("\tPage id %s - Page Size %d - Object Size %d\n", pageid, pg.Size, len(pg.Object))
		pagemeta, _ := base64.StdEncoding.DecodeString(pg.Metadata)
		pagmeta := meta.Pagemeta{}
		json.Unmarshal([]byte(pagemeta), &pagmeta)
		if verbose {
			fmt.Printf("\t\tPage metadata %s\n", pagemeta)
		} else {
			pagmeta := meta.Pagemeta{}
			json.Unmarshal([]byte(pagemeta), &pagmeta)
			if pagmeta.MultiMedia.Tiff {
				if bytes.Compare(pg.Object[pagmeta.TiffOffset.Start:pagmeta.TiffOffset.Start+3],[]byte(LEHeader)) == 0 {
					tiff = "image/tiff"
				} else {
					fmt.Printf("%s %S\n",pg.Object[pagmeta.TiffOffset.Start:pagmeta.TiffOffset.Start+3],LEHeader)
					tiff = http.DetectContentType(pg.Object[pagmeta.TiffOffset.Start:pagmeta.TiffOffset.End])
				}
				tiffl = pagmeta.TiffOffset.End - pagmeta.TiffOffset.Start + 1
			}
			if pagmeta.MultiMedia.Png {
				png = http.DetectContentType(pg.Object[pagmeta.PngOffset.Start:pagmeta.PngOffset.End])
				pngl = pagmeta.PngOffset.End - pagmeta.PngOffset.Start + 1
			}
			fmt.Printf("\t\tPage Number %d - Length %d - Png %v:%s:%d - Tiff %v:%s:%d\n", pagmeta.PageNumber, pagmeta.PageLength, pagmeta.MultiMedia.Png, png, pngl, pagmeta.MultiMedia.Tiff, tiff, tiffl)
		}
	}
}
