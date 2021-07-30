package lib

import (
	"errors"
	"fmt"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/spf13/viper"
	"net/url"
)

func SetSourceSproxyd(op string, srcUrl string, driver string, env string) error {

	var (
		s_url               = op + ".sproxyd.source.urls"
		s_driver            = op + ".sproxyd.source.driver"
		s_env               = op + ".sproxyd.source.env"
		err                 error
		missingSourceUrls   = "Missing  --source-sproxyd-url or add xx.sproxyd.source.urls in the config.yaml file - xx = [backup|clone|restore|check]"
		missingSourceEnv    = "Missing  --source-sproxyd-env or add xx.sproxyd.source.env [prod|osa] in the config.yaml file - xx = [backup|clone|restore|check]"
		missingSourceDriver = "Missing  --source-sproxyd-driver or add xx.sproxyd.source.driver [bpchord|bparc] in the config.yaml file - xx = [backup|clone|restore|check]"

	)
	if len(srcUrl) > 0 {
		if _, err = url.ParseRequestURI(srcUrl); err != nil {
			return err
		}
		sproxyd.Url = srcUrl

	} else {
		if urls := viper.GetString(s_url); len(urls) > 0 {
			sproxyd.Url = urls
			srcUrl = urls
		} else {
			// gLog.Error.Println("--source-url sproxyd urls are missing , add check.sproxyd.source.urls to the config file")
			// gLog.Error.Printf("%s", missingSourceUrls)
			err = errors.New(fmt.Sprintf("%s", missingSourceUrls))
			gLog.Error.Printf("%v",err)
			return err
		}
	}

	if len(driver) > 0 {
		sproxyd.Driver = driver
	} else {
		if drv := viper.GetString(s_driver); len(drv) > 0 {
			sproxyd.Driver = drv
			driver = drv
		} else {
			err = errors.New(fmt.Sprintf("%s", missingSourceDriver))
			gLog.Error.Printf("%v",err)
			//gLog.Error.Printf("%s", missingSourceDriver)
			return err
		}
	}

	if len(env) > 0 {
		sproxyd.Env = env
	} else {
		if env := viper.GetString(s_env); len(env) > 0 {
			sproxyd.Env = env
		} else {
			// gLog.Error.Printf("%s", missingSourceEnv)
			err = errors.New(fmt.Sprintf("%s", missingSourceEnv))
			gLog.Error.Printf("%v",err)
			return err
		}
	}

	sproxyd.SetNewProxydHost1(srcUrl, driver)

	gLog.Trace.Printf("Connection timeout %v - read timeout %v - write timeoiut %v", sproxyd.ConnectionTimeout, sproxyd.ReadTimeout, sproxyd.WriteTimeout)
	gLog.Trace.Printf("Source Host Pool: %v - Source Env: %s - Source Driver: %s", sproxyd.HP.Hosts(), sproxyd.Env, sproxyd.Driver)
	return nil
}

func SetTargetSproxyd(op string, targetUrl string, targetDriver string, targetEnv string) error {

	var (
		t_url    = op + ".sproxyd.target.urls"
		t_driver = op + ".sproxyd.target.driver"
		t_env    = op + ".sproxyd.target.env"
		err      error
		missingTargetUrls   = "Missing  --target-sproxyd-url or add xx.sproxyd.target.urls in the config.yaml file - xx = [backup|clone|restore|check]"
		missingTargetEnv    = "Missing  --target-sproxyd-env or add xx.sproxyd.target.env [prod|osa] in the config.yaml file - xx = [backup|clone|restore|check]"
		missingTargetDriver = "Missing  --target-sproxyd-driver or add xx.sproxyd.target.driver [bpchord|bparc] in the config.yaml file - xx = [backup|clone|restore|check]"
	)

	if len(targetUrl) > 0 {
		if _, err = url.ParseRequestURI(targetUrl); err != nil {
			return err
		}
		sproxyd.TargetUrl = targetUrl
	} else {
		if urls := viper.GetString(t_url); len(urls) > 0 {
			sproxyd.TargetUrl = urls
			targetUrl = urls
		} else {
			err = errors.New(fmt.Sprintf("%s", missingTargetUrls))
			gLog.Error.Printf("%v",err)
			return err
		}
	}

	if len(targetDriver) > 0 {
		sproxyd.TargetDriver = targetDriver
	} else {
		if drv := viper.GetString(t_driver); len(drv) > 0 {
			sproxyd.TargetDriver = drv
			targetDriver = drv
		} else {
			// gLog.Error.Printf("%s", missingTargetDriver)
			err = errors.New(fmt.Sprintf("%s", missingTargetDriver))
			gLog.Error.Printf("%v",err)
			return err
		}
	}

	if len(targetEnv) > 0 {
		sproxyd.TargetEnv = targetEnv
	} else {
		if env := viper.GetString(t_env); len(env) > 0 {
			sproxyd.TargetEnv = env
		} else {
			// gLog.Error.Printf("%s", missingTargetEnv)
			err = errors.New(fmt.Sprintf("%s", missingTargetEnv))
			gLog.Error.Printf("%v",err)
			return err
		}
	}

	sproxyd.SetNewTargetProxydHost1(targetUrl, targetDriver)
	gLog.Trace.Printf("Connection timeout %v - read timeout %v - write timeoiut %v", sproxyd.ConnectionTimeout, sproxyd.ReadTimeout, sproxyd.WriteTimeout)
	gLog.Trace.Printf("Target Host Pool: %v -  Target Env: %s - Target Driver: %s", sproxyd.TargetHP.Hosts(), sproxyd.TargetEnv, sproxyd.TargetDriver)
	return nil
}


