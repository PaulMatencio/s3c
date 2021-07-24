package lib

import (
	"errors"
	"fmt"
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
			Path: sproxyd.Env + "/" + key,
		}
		resp *http.Response
	)

	metadata := moses.DocumentMetadata{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if err = metadata.UsermdToStruct(string(encoded_usermd[0])); err == nil {
				if pgn, err = metadata.GetPageNumber(); err == nil {
					gLog.Info.Printf("Key %s  - Number of pages %d", key, pgn)
					return pgn, err, resp.StatusCode
				} else {
					gLog.Error.Printf("Error getting page number %d", pgn)
				}
			} else {
				gLog.Error.Println(err)
			}
		default:
			err = errors.New("Check the status Code for the reason")
		}
		return pgn, err, resp.StatusCode
	}
	return pgn, err, -1
}

func GetPathName(pathId string) (error, string, int) {
	var (
		pathName       string
		err            error
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: pathId,
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		resp *http.Response
	)

	metadata := moses.Pagemeta{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if err = metadata.UsermdToStruct(string(encoded_usermd[0])); err == nil {
				if pathName = metadata.GetPathName(); err == nil {
					// gLog.Info.Printf("PathId %s - Pathname %s",pathId,pathName)
					return err, pathName, resp.StatusCode
				} else {
					err = errors.New(fmt.Sprintf("Error getting path name for path id %s", pathId))
				}
			}
		default:
			err = errors.New(fmt.Sprintf("Error getting path name for path id %s - Status code: %s", pathId, resp.StatusCode))
		}
		return err, pathName, resp.StatusCode
	}
	return err, pathName, -1
}