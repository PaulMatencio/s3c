
package st33

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmatencio/s3c/utils"
	"strings"
)

type Directory struct {
	PxiId 	string
	Pages  	uint32
	Records uint32
	Size   	uint32

}

func (dir *Directory) New (buf []byte,l int) int {
	var (
		pages,records, size uint32
		Big binary.ByteOrder = binary.BigEndian
	)

	add 	   := 	utils.Ebc2asci(buf[l : l+21])
	dir.PxiId   =   strings.Replace(string(add[0:21])," ","",-1) // remove all spaces in string

	buf1 	:= 	bytes.NewReader(buf[l+40 : l+44])
	_ 		= 	binary.Read(buf1, Big, &pages)

	buf1 	= 	bytes.NewReader(buf[l+44 : l+48])
	_ 		= 	binary.Read(buf1, Big, &records)

	buf1 	= 	bytes.NewReader(buf[l+48: l+52])
	_ 		= 	binary.Read(buf1, Big, &size)
	size    =  size*1024
	dir.Pages = pages
	dir.Records = records
	dir.Size = size

	return l + 144
}

func BuildDirectoryArray(inputFile string) (*[]Directory, error ){
	dirs := [] Directory{}
	abuf, err := utils.ReadBuffer(inputFile)
	if err == nil {
		defer abuf.Reset()
		buf  := abuf.Bytes()
		bufl := len(buf)
		l,dir := 0, Directory{}

		for l < bufl {
			l = dir.New(buf,l)

			dirs = append(dirs,dir)
		}
	} else {
		return nil,err
	}
	return &dirs,err
}