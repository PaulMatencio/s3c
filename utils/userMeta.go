package utils

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"os"
)


func GetUserMeta(metad map[string]*string) (string,error) {

	var (
		err error
		u []byte
	)

	if v,ok := metad["Usermd"];ok {
		u,err =  base64.StdEncoding.DecodeString(*v)
		return string(u),err
	} else {
		return "",err
	}
}

func GetVersionId(metad map[string]*string) (string,error) {

	var (
		err error
	)

	if v,ok := metad["VersionId"];ok {
		return *v,nil
	} else {
		err = errors.New("VersionId does not exist")
		return "",err
	}
}


func GetPxiMeta(metad map[string]*string) (string,error) {

	var (
		err error
	)

	if v,ok := metad["Pages"];ok {
		return *v,err
	} else {
		return "",err
	}

}



func BuildUserMeta(meta []byte) (map[string]*string) {

	metad := make(map[string]*string)
	if len(meta) > 0 {
		m := base64.StdEncoding.EncodeToString(meta)
		metad["Usermd"] = &m
		//md := string(meta)
		//metad["Usermd"] = &md
	}
	return metad
}


func AddMoreUserMeta(metad map[string]string,source string) (map[string]string) {
	metad["Source"] = source
	return metad
}



func WriteUserMeta(metad map[string]*string,pathname string) {

	var (
		usermd string
		err    error
	)
	/* convert map to json */


	if usermd,err  = GetUserMeta(metad); err == nil && len(usermd) > 0 {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			gLog.Error.Printf("Error %v writing %s ",err,pathname)
		}

	}

}



func PrintUserMeta(key string, metad  map[string]*string) {

	if v,ok:= metad["Usermd"];ok {
		usermd,_ :=  base64.StdEncoding.DecodeString(*v)
		gLog.Info.Printf("key:%s - User metadata: %s", key, usermd)
	}

}

func PrintPxiMeta(key string, metad  map[string]*string) {

	if v,ok := metad["Pages"];ok {
		gLog.Info.Printf("Key %s - Pxi metadata %s=%s", key,"Pages", *v)
	}

}

func BuildUsermd(usermd map[string]string) (map[string]*string) {

	metad := make(map[string]*string)
	for k,v := range usermd {
		V:= v     /*
		             Circumvent  a Go pointer  problem  => &v points to same address for every k
		          */
		metad[k] = &V
	}
	return metad
}




func PrintUsermd(key string, metad map[string]*string) {

	gLog.Info.Printf("Key: %s",key)
	for k,v := range metad {
		switch k {
		case "Usermd" :
			val,_ :=  base64.StdEncoding.DecodeString(*v)
			gLog.Info.Printf("%s: %s",k,string(val))
		case "S3meta" :
			val,_ :=  base64.StdEncoding.DecodeString(*v)
			gLog.Info.Printf("%s: %s",k,string(val))
		default:
			gLog.Info.Printf("%s: %s", k, *v)
		}
		/*
		if k == "Usermd" {
			usermd,_ :=  base64.StdEncoding.DecodeString(*v)
			gLog.Info.Printf("%s: %s",k,string(usermd))
		} else {
			if k == "S3Meta" {
				s3meta,_ :=  base64.StdEncoding.DecodeString(*v)
				gLog.Info.Printf("%s: %s",k,string(s3meta)
			} else {
				gLog.Info.Printf("%s: %s", k, *v)
			}
		}
		 */
	}
}

func PrintMetadata(metad map[string]*string) {
	for k,v := range metad {
		gLog.Trace.Printf("%s: %s", k,*v)
	}
}


func WriteUsermd(metad map[string]*string ,pathname string ) {

	/* convert map into json */
	if usermd,err := json.MarshalIndent(metad,""," "); err ==  nil {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			gLog.Error.Printf("Error %v writing %s ",err,pathname)
		}

	}
}

func ReadUsermd(pathname string) (map[string]string,error) {

	var (
		meta = map[string]string{}
		err   error
		usermd []byte
	)
	if  _,err:= os.Stat(pathname) ; err == nil {

		if usermd, err = ioutil.ReadFile(pathname); err == nil {
			if err = json.Unmarshal(usermd, &meta); err == nil {
				return meta, err
			}
		}
	}
	if os.IsNotExist(err) {
		return meta,nil
	}
	return meta,err

}