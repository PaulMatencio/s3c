// bns project bns.go
package bns

import (
	"strings"
)

func GetPageMetadata(bnsRequest *HttpRequest, url string) ([]byte, error, int) {
	return GetMetadata(bnsRequest, url)
}


func ChkPageMetadata(bnsRequest *HttpRequest, url string) ([]byte, error, int) {
	return ChkMetadata(bnsRequest, url)
}

func GetDocMetadata(bnsRequest *HttpRequest, url string) ([]byte, error, int) {
	return GetMetadata(bnsRequest, url)
}

func BuildSubtable(content string, index string) []int {
	page_tab := make([]int, 0, Max_page)
	dpage := strings.Split(content, ",")
	for k, v := range dpage {
		if strings.Contains(v, index) {
			page_tab = append(page_tab, k+1)
		}
	}
	return page_tab
}
