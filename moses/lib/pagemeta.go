package bns

import (
	"encoding/json"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	goLog "github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"os"
	"strconv"
)

// Structure of the user page metadata
type Pagemeta struct {
	PubId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber`
		KindCode    string `json:"kindCode"`
	} `json:"pubId"`
	BnsId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber`
		KindCode    string `json:"kindCode"`
		BnsId22     string  `json:"bnsId22,omitempty"`
	} `json:"bnsId"`
	PublicationOffice string `json:"publicationOffice"`
	PageNumber        int    `json:"pageNumber"`
	RotationCode      struct {
		Pdf  int `json:"pdf"`
		Png  int `json:"png"`
		Tiff int `json:"tiff"`
	} `json:"rotationCode"`
	Pubdate    string `json:"pubDate`
	Copyright  string `json:"copyright`
	MultiMedia struct {
		Pdf   bool `json:"pdf"`
		Png   bool `json:"png"`
		Tiff  bool `json:"tiff"`
		Video bool `json:"video"`
	} `json:"multiMedia"`
	PageIndicator []string `json:"pageIndicator"`
	PageLength    int      `json:"pageLength"`
	TiffOffset    struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"tiffOffset,omitempty"`
	PngOffset struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"pngOffset,omitempty"`
	PdfOffset struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"pdfOffset,omitempty"`
}

// Convert a structure  into Json format  and write it to a file
func (pagemeta *Pagemeta) Encode(filename string) error {
	file, err := os.Create(filename)
	if err == nil {
		defer file.Close()
		encoder := json.NewEncoder(file)
		return encoder.Encode(&pagemeta)
	} else {
		return err
	}
}

// Read a json format into a structure
func (pagemeta *Pagemeta) Decode(filename string) error {
	file, err := os.Open(filename)
	if err == nil {
		defer file.Close()
		decoder := json.NewDecoder(file)
		return decoder.Decode(&pagemeta)
	} else {
		return err
	}
}

// return document id  in the form CC/PN/KC/pn
func (pagemeta *Pagemeta) GetPathName() string {
	//return "/" + pagemeta.BnsId.CountryCode + "/" + pagemeta.BnsId.PubNumber + "/" + pagemeta.BnsId.KindCode + "/p" + strconv.Itoa(pagemeta.PageNumber)
	return (fmt.Sprintf("%s/%s/%s/p%s", pagemeta.BnsId.CountryCode, pagemeta.BnsId.PubNumber, pagemeta.BnsId.KindCode, strconv.Itoa(pagemeta.PageNumber)))
}

func (pagemeta *Pagemeta) UsermdToStruct(meta string) error {

	if jsonByte, err := base64.Decode64(meta); err == nil {
		return json.Unmarshal(jsonByte, &pagemeta)
	} else {
		return err
	}
}

// Read from a file a page in json format ans store it in a page structure
// same as Decode
func (pagemeta *Pagemeta) SetPagemd(filename string) error {
	//* USE Encode
	var (
		buf []byte
		err error
	)
	if buf, err = ioutil.ReadFile(filename); err != nil {
		goLog.Warning.Println(err, "Reading", filename)
		return err
	} else if err = json.Unmarshal(buf, &pagemeta); err != nil {
		goLog.Warning.Println(err, "Unmarshalling", filename)
		return err
	}
	return err
}

type PAGE struct {
	Metadata Pagemeta `json:"Metadata"`
	Tiff     struct {
		Size  int    `json:"size"`
		Image []byte `json:"image"`
	} `json:"tiff,omitempty"`
	Png struct {
		Size  int    `json:"size"`
		Image []byte `json:"image"`
	} `json:"Png,omitempty"`
}

func (page *PAGE) Encode(filename string) error {
	file, err := os.Create(filename)
	if err == nil {
		defer file.Close()
		encoder := json.NewEncoder(file)
		return encoder.Encode(&page)
	} else {
		return err
	}
}
