package lib

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/spf13/viper"
)

func SetSproxydHost(op string,srcUrl string,targetUrl string,driver string,targetDriver string ) {

	s_url := op + ".sproxyd.source.urls"
	t_url := op + ".sproxyd.target.urls"
	s_driver := op + ".sproxyd.source.driver"
	t_driver := op + ".sproxyd.target.driver"

	if len(srcUrl) > 0 {
		sproxyd.Url = srcUrl
	} else {
		if  urls := viper.GetString(s_url);len (urls) > 0 {
			sproxyd.Url = urls
			srcUrl = urls
		} else {
			gLog.Error.Println("Source sproxyd urls are missing , add check.sproxyd.source.urls to the config file")
			return
		}
	}

	if len(driver) > 0 {
		sproxyd.Driver = driver
	} else {
		if  drv := viper.GetString(s_driver);len (drv) > 0 {
			sproxyd.Driver = drv
			driver = drv
		} else {
			gLog.Error.Println("Source sproxyd driver is missing , add check.sproxyd.source.driver to the config file")
			return
		}
	}

	if len(targetUrl) > 0 {
		sproxyd.TargetUrl = targetUrl
	} else {
		if  urls := viper.GetString(t_url);len (urls) > 0 {
			sproxyd.TargetUrl = urls
			targetUrl= urls
		} else {
			gLog.Error.Println("target sproxyd urls are missing , add check.sproxyd.target.urls to the config file")
			return
		}
	}
	if len(targetDriver) > 0 {
		sproxyd.TargetDriver = targetDriver
	} else {
		if  drv := viper.GetString(t_driver);len (drv) > 0 {
			sproxyd.TargetDriver= drv
			targetDriver = drv
		} else {
			gLog.Error.Println("Source sproxyd driver is missing , add check.sproxyd.target.driver to the config file")
			return
		}
	}

	sproxyd.SetNewProxydHost1(srcUrl, driver)
	sproxyd.SetNewTargetProxydHost1(targetUrl, targetDriver)
	gLog.Trace.Printf("Connection timeout %v - read timeout %v - write timeoiut %v",sproxyd.ConnectionTimeout,sproxyd.ReadTimeout,sproxyd.WriteTimeout)
	gLog.Trace.Printf("Source Host Pool: %v - Source Env: %s - Source Driver: %s", sproxyd.HP.Hosts(), sproxyd.Env, sproxyd.Driver)
	gLog.Trace.Printf("Target Host Pool: %v -  Source Env: %s - Source Driver: %s", sproxyd.TargetHP.Hosts(), sproxyd.TargetEnv, sproxyd.TargetDriver)

}

func SetSourceSproxyd(op string,srcUrl string,driver string ) {

	s_url := op + ".sproxyd.source.urls"
	s_driver := op + ".sproxyd.source.driver"

	if len(srcUrl) > 0 {
		sproxyd.Url = srcUrl
	} else {
		if  urls := viper.GetString(s_url);len (urls) > 0 {
			sproxyd.Url = urls
			srcUrl = urls
		} else {
			gLog.Error.Println("Source sproxyd urls are missing , add check.sproxyd.source.urls to the config file")
			return
		}
	}

	if len(driver) > 0 {
		sproxyd.Driver = driver
	} else {
		if  drv := viper.GetString(s_driver);len (drv) > 0 {
			sproxyd.Driver = drv
			driver = drv
		} else {
			gLog.Error.Println("Source sproxyd driver is missing , add check.sproxyd.source.driver to the config file")
			return
		}
	}

	sproxyd.SetNewProxydHost1(srcUrl, driver)
	gLog.Trace.Printf("Connection timeout %v - read timeout %v - write timeoiut %v",sproxyd.ConnectionTimeout,sproxyd.ReadTimeout,sproxyd.WriteTimeout)
	gLog.Trace.Printf("Source Host Pool: %v - Source Env: %s - Source Driver: %s", sproxyd.HP.Hosts(), sproxyd.Env, sproxyd.Driver)

}

func SetTargetSproxyd(op string,targetUrl string,targetDriver string ) {

	t_url := op + ".sproxyd.target.urls"
	t_driver := op + ".sproxyd.target.driver"


	if len(targetUrl) > 0 {
		sproxyd.TargetUrl = targetUrl
	} else {
		if  urls := viper.GetString(t_url);len (urls) > 0 {
			sproxyd.TargetUrl = urls
			targetUrl= urls
		} else {
			gLog.Error.Println("target sproxyd urls are missing , add check.sproxyd.target.urls to the config file")
			return
		}
	}
	if len(targetDriver) > 0 {
		sproxyd.TargetDriver = targetDriver
	} else {
		if  drv := viper.GetString(t_driver);len (drv) > 0 {
			sproxyd.TargetDriver= drv
			targetDriver = drv
		} else {
			gLog.Error.Println("Source sproxyd driver is missing , add check.sproxyd.target.driver to the config file")
			return
		}
	}

	sproxyd.SetNewTargetProxydHost1(targetUrl, targetDriver)
	gLog.Trace.Printf("Connection timeout %v - read timeout %v - write timeoiut %v",sproxyd.ConnectionTimeout,sproxyd.ReadTimeout,sproxyd.WriteTimeout)
	gLog.Trace.Printf("Target Host Pool: %v -  Source Env: %s - Source Driver: %s", sproxyd.TargetHP.Hosts(), sproxyd.TargetEnv, sproxyd.TargetDriver)

}

func CreateS3Session(op string,location string) (*s3.S3) {
	var (
		url string
		accessKey  string
		secretKey string
		session  datatype.CreateSession
	)

	c := op+".s3."+location+".url"
	if  url = viper.GetString(c); len(url) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file",c)))
		return nil
	}
	c= op+".s3."+location+".credential.access_key_id"
	if accessKey = viper.GetString(c); len(accessKey) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file",c)))
		return nil
	}
	c= op+".s3."+location+".credential.secret_access_key"
	if secretKey = viper.GetString(c); len(secretKey) == 0 {
		gLog.Error.Println(errors.New(fmt.Sprintf("missing %s in the config file",c)))
		return nil
	}

	// gLog.Info.Println(metaUrl,metaAccessKey,metaSecretKey)
	session = datatype.CreateSession{
		Region:    viper.GetString("check.s3.source.region"),
		EndPoint:  url,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	return s3.New(api.CreateSession2(session))
}