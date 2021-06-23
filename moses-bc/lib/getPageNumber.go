package lib

import (
	"errors"
	base64 "github.com/paulmatencio/ring/user/base64j"
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
				if err = metadata.UsermdToStruct(string(usermd)); err == nil {
					if pgn, err = metadata.GetPageNumber(); err == nil {
						return pgn, err, resp.StatusCode
					}
				}
			}
		default:
			err = errors.New("Check the status Code for the reason")
		}
	}
	return 0, err, resp.StatusCode
}
