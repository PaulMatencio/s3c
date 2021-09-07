package datatype
/*
	moses s3 index metadata
 */
type UserMd struct {
	FpClipping string `json:"fpClipping,omitempty"`
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}