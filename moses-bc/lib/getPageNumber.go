package lib

import (
	"errors"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/gLog"
	moses "github.com/paulmatencio/s3c/moses/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
)

func GetPageNumber(key string) (int, error, int) {
	var (
		pgn            int
		err            error
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.Env + "/" + key ,
		}
		resp *http.Response
	)

	metadata := moses.DocumentMetadata{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if usermd, err := base64.Decode64(encoded_usermd[0]); err == nil {
				gLog.Info.Println(string(usermd))
				if err = metadata.UsermdToStruct(string(usermd)); err == nil {

					if pgn, err = metadata.GetPageNumber(); err == nil {
						gLog.Info.Printf("Key %s  - Number of pages %d",key,pgn)
						return pgn, err, resp.StatusCode
					} else {
						gLog.Error.Printf("Error getting page number %d",pgn)
					}
				}else {
					gLog.Error.Println(err)
				}
			}
		default:
			err = errors.New("Check the status Code for the reason")
		}
	}
	return pgn, err, resp.StatusCode
}
