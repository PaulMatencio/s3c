package datatype

/*
	used by inspectBlobs and backup
 */

type Pagemeta struct {
	PubId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
		KindCode    string `json:"kindCode"`
	} `json:"pubId"`
	BnsId struct {
		CountryCode string `json:"countryCode"`
		PubNumber   string `json:"pubNumber"`
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
	Pubdate    string `json:"pubDate"`
	Copyright  string `json:"copyright"`
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



