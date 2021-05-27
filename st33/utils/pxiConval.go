package st33

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmatencio/s3c/utils"
	"strconv"
	"strings"
)

type Conval struct {

	PxiId 		string   `json:"pxiId"`
	PubDate 	string	 `json:"pubDate"`
	ScanDate	string   `json:"scanDate"`
	Pages  		uint32   `json:"numPages"`
	Records  	int      `json:"numRecords,omitempty"`
	DocSize  	uint32   `json:"docSize"`
	Status      string   `json:"status"`
	Revisory    string   `json:"revisory"`
	Copyright 	string   `json:"copyright"`
	Quality  	string   `json:"quality,omitempty"`

}

func (con *Conval) New (buf []byte,l int) int {

	var (
		pages 		uint32
		records		string
		Big  		binary.ByteOrder = binary.BigEndian
	)

	add 	:= 	utils.Ebc2asci(buf[l : l+36])

	buf1 	:= 	bytes.NewReader(buf[l+36: l+40])
	_ 		= 	binary.Read(buf1, Big, &pages)

	records = string(utils.Ebc2asci(buf[l+41 : l+47]))

	// con.Size = string(ebc2asc.Ebc2asci(buf[l+47 : l+54]))

	pxiid := string(add[3:25])

	long := len(pxiid)
	if pxiid[long-1:] == " "  {
		pxiid = pxiid[0:long-1]
	}

	con.PxiId  		=   strings.Replace(pxiid," ","_",-1) // remove all spaces

	con.PubDate   	=   string(add[26:34])
	con.Copyright	= 	string(add[34:35])
	con.Quality 	= 	string(add[35:36])
	con.Pages 		= 	pages
	con.ScanDate 	= 	string(utils.Ebc2asci(buf[l+54 : l+62]))
	con.Status		= 	string(utils.Ebc2asci(buf[l+62 : l+63]))
	con.Revisory 	= 	string(utils.Ebc2asci(buf[l+63 : l+64]))
	/*
	if i,err   := strconv.Atoi(con.Size); err == nil {
		con.DocSize = uint32(i) * 1024
	} else {
		log.Println(err)
		con.DocSize = 0
	}

	*/

	/* check the error */
	con.Records,_= strconv.Atoi(records)

	return l + 143
}

func BuildConvalArray(inputFile string) (*[]Conval, error ){
	vals := [] Conval{}
	abuf, err := utils.ReadBuffer(inputFile)
	if err == nil {
		defer abuf.Reset()
		buf  := abuf.Bytes()
		bufl := len(buf)
		l,dir := 0, Conval{}

		for l < bufl {
			l = dir.New(buf,l)
			vals = append(vals,dir)
		}
	} else {
		return nil,err
	}
	return &vals,err
}