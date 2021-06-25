package bns

import (
	"encoding/json"
	"errors"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	gLog "github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"os"
	"strconv"
)

type DocumentMetadata struct {
	PubId struct {
		CountryCode string `json: "countryCode`
		PubNumber   string `json: "pubNumber"`
		KindCode    string `json: "kindCode"`
	} `json: "PubId,omitempty"`

	BnsId struct {
		CountryCode string `json: "countryCode`
		PubNumber   string `json: "pubNumber"`
		KindCode    string `json: "kindCode"`
	} `json: "bnsId,omitempty"`

	DocId             interface{} `json:"docId` // could be integer  or string
	PublicationOffice string      `json:"publicationOffice`
	FamilyId          interface{} `json:"familyId"` // could be integer  or string
	TotalPage         int         `json:totalPage"`
	DocType           string      `json:docType"`
	PubDate           string      `json:pubDate"`
	LoadDate          string      `json:loadDate"`
	Copyright         string      `json:"copyright,omitempty"`

	LinkPubId []struct {
		CountryCode string `json: "countryCode`
		PubNumber   string `json: "pubNumber"`
		KindCode    string `json: "kindCode"`
	} `json: "linkPubId,omitemty`

	MultiMedia struct {
		Tiff  bool `json:"tiff"`
		Png   bool `json:"png"`
		Pdf   bool `json:"pdf"`
		Video bool `json:"video"`
	} `json:"multiMedia"`

	FpClipping struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
		KindCode    string `json:"kindCode"`
	} `json:"fpClipping,omitempty"`

	AbsRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"absRangePageNumber,omitempty"`

	AmdRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"amdRangePageNumber,omitempty"`

	BibliRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"bibliRangePageNumber,omitempty"`

	ClaimsRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"claimsRangePageNumber,omitempty"`

	DescRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"descRangePageNumber,omitempty"`

	DrawRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"drawRangePageNumber,omitempty"`

	SearchRepRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"searchRepRangePageNumber,omitempty"`

	DnaSequenceRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"dnaSequenceRangePageNumber,omoitempty"`

	ApplicantCitationsRangePageNumber []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"applicantCitationsRangePageNumber,omitempty"`

	Classification []string `json:"classification,omitempty"`
}

/*
type Range struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

*/

// Document ( TOC) methods
// read from a file a document (TOC)  in json format and store it in a document struture
func (docmeta *DocumentMetadata) SetDocmd(filename string) error {
	var (
		buf []byte
		err error
	)
	if buf, err = ioutil.ReadFile(filename); err != nil {
		gLog.Warning.Println(err, "Reading", filename)
		return err
	} else if err = json.Unmarshal(buf, &docmeta); err != nil {
		gLog.Warning.Println(err, "Unmarshalling", filename)
		return err
	}
	return err
}

// Write a document structure ( TOC) to a file
func (docmeta *DocumentMetadata) Encode(filename string) error {
	file, err := os.Create(filename)
	if err == nil {
		defer file.Close()
		encoder := json.NewEncoder(file)
		return encoder.Encode(&docmeta)
	} else {
		return err
	}
}

/* Read the content of a file and convert it into a Document metadata structure   */
func (docmeta *DocumentMetadata) Decode(filename string) error {
	file, err := os.Open(filename)
	if err == nil {
		defer file.Close()
		decoder := json.NewDecoder(file)
		return decoder.Decode(&docmeta)
	} else {
		return err
	}
}

// Get total number of pages of a document
func (docmeta *DocumentMetadata) GetPageNumber() (int, error) {
	if page := docmeta.TotalPage; page > 0 {
		return docmeta.TotalPage, nil
	} else {
		return 0, errors.New("Page number invalid")
	}
}

// Get  the publication date of a document
func (docmeta *DocumentMetadata) GetPubDate() (Date, error) {
	date := Date{}
	err := error(nil)
	if docmeta.PubDate != "" {
		date, err = ParseDate(docmeta.PubDate)
	} else {
		err = errors.New("no Publication date")
	}
	return date, err
}

func (docmeta *DocumentMetadata) GetNumberOfPages() (int) {
	return docmeta.TotalPage
}


// get pages ranges and sub pages
func (docmeta *DocumentMetadata) GetPagesRanges(section string) string {
	var pagesranges string
	switch section {
	case "Abstract":
		for _, ranges := range docmeta.AbsRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "Amendement":
		for _, ranges := range docmeta.AmdRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}

	case "Biblio":
		for _, ranges := range docmeta.BibliRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "Claims":
		for _, ranges := range docmeta.ClaimsRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "Description":
		for _, ranges := range docmeta.DescRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "Drawings":
		for _, ranges := range docmeta.DrawRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "SearchRep":
		for _, ranges := range docmeta.SearchRepRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "DnaSequence":
		for _, ranges := range docmeta.DnaSequenceRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	case "Citations":
		for _, ranges := range docmeta.ApplicantCitationsRangePageNumber {
			pagesranges += fmt.Sprintf("%s:%s,", strconv.Itoa(ranges.Start), strconv.Itoa(ranges.End))
		}
	default:
	}
	if len(pagesranges) > 0 {
		return pagesranges[0 : len(pagesranges)-1]
	} else {
		return pagesranges
	}
}

func (docmeta *DocumentMetadata) UsermdToStruct(meta string) error {

	if jsonByte, err := base64.Decode64(meta); err == nil {
		return json.Unmarshal(jsonByte, &docmeta)
	} else {
		return err
	}
}

// return the document Id  in the format CC/PN/KC
func (docmeta *DocumentMetadata) GetPathName() string {
	return (fmt.Sprintf("%s/%s/%s", docmeta.PubId.CountryCode, docmeta.PubId.PubNumber, docmeta.PubId.KindCode))
	// return  docmeta.BnsId.CountryCode + "/" + docmeta.BnsId.PubNumber + "/" + docmeta.BnsId.KindCode
}

// Get document metadata
func (docmeta *DocumentMetadata) GetMetadata(bnsRequest *HttpRequest, pathname string) error {
	var (
		err        error
		docmd      []byte
		statusCode int
	)
	if docmd, err, statusCode = GetDocMetadata(bnsRequest, pathname); err == nil {
		gLog.Trace.Println("Document Metadata=>", string(docmd))
		if len(docmd) != 0 {
			if err = json.Unmarshal(docmd, &docmeta); err != nil {
				gLog.Error.Println(docmd, docmeta, err)

			}
		} else if statusCode == 404 {
			gLog.Warning.Printf("Document %s is not found", pathname)
			err = errors.New("Document not found")
		} else {
			gLog.Warning.Printf("Document's %s metadata is missing", pathname)
			err = errors.New("Document metadata is missing")
		}
	} else {
		gLog.Error.Println(err)

	}
	return err

}


func (docmeta *DocumentMetadata) HasPDF(usermd string) (error,bool) {
		if err := docmeta.UsermdToStruct(usermd); err == nil {
				if docmeta.MultiMedia.Pdf {
					return nil,true
				} else {
					return nil,false
				}
			} else {
				return err,false
			}
}