package bns

import (
	"net/http"

	hostpool "github.com/bitly/go-hostpool"
)

type Configuration struct {
	Storage_nodes []string
}

type BnsResponse struct {
	HttpStatusCode int    `json: "httpstatuscode,omitempty"`
	Pagemd         []byte `json: "pagemeta,omitempty"` // decoded user meta data
	Usermd         string `json: "usermd,omitempty"`   // encoded user meta data
	ContentType    string `json: "content-type,omitempty"`
	Image          []byte `json: "images"`
	BnsId          string `json: "bnsId,omitempty"`
	PageNumber     string `json: "pageNunber,omitempty"`
	Page           int    `json: "page,omitempty`
	Err            error  `json: "errorCode"`
}

type BnsResponseLi struct {
	Page        int     `json: "page,omitempty`
	Pagemd      []byte  `json: "pagemeta,omitempty"` // decoded user meta data
	ContentType string  `json: "content-type,omitempty"`
	Image       *[]byte `json: "images"` // address of the image
	BnsId       string  `json: "bnsId,omitempty"`
}

type Date struct {
	Year       int16
	Month, Day byte
}

// bns Http request structure
type HttpRequest struct {
	Hspool hostpool.HostPool
	Urls   []string
	// Path   string
	Client *http.Client
	Media  string
}

type OpResponse struct {
	Err    error
	SrcUrl string
	Num    int
	Num200 int
}

type MetaResponse struct {
	Err     error
	SrcUrl  string
	Encoded string
	Decoded []byte
}
