package datatype

/*
	used by inspectBlobs
 */

type DocumentMetadata struct {
	PubId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
		KindCode    string `json:"kindCode"`
	} `json: "PubId,omitempty"`

	BnsId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
		KindCode    string `json:"kindCode"`
	} `json: "bnsId,omitempty"`

	DocId             interface{} `json:"docId"` // could be integer  or string
	PublicationOffice string      `json:"publicationOffice"`
	FamilyId          interface{} `json:"familyId"` // could be integer  or string
	TotalPage         int         `json:"totalPage"`
	DocType           string      `json:"docType"`
	PubDate           string      `json:"pubDate"`
	LoadDate          string      `json:"loadDate"`
	Copyright         string      `json:"copyright,omitempty"`

	LinkPubId []struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
		KindCode    string `json:"kindCode"`
	} `json: "linkPubId,omitemty"`

	MultiMedia struct {
		Tiff  bool `json:"tiff"`
		Png   bool `json:"png"`
		Pdf   bool `json:"pdf"`
		Video bool `json:"video"`
	} `json:"multiMedia"`

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


