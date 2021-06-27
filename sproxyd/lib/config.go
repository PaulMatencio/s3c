// sproxyd project sproxyd.go
package sproxyd

import (
	hostpool "github.com/bitly/go-hostpool"
	"strings"
)

// var Host = []string{"http://luo001t.internal.epo.org:81/proxy/chord/", "http://luo002t.internal.epo.org:81/proxy/chord/", "http://luo003t.internal.epo.org:81/proxy/chord/"}

func SetNewProxydHost() {
	sproxyd := strings.Split(Url,",")
	HP = hostpool.NewEpsilonGreedy(sproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})
	Driver = Driver
	Env = Env
	Host = Host[:0] // reset
	Host = sproxyd
}

func SetNewTargetProxydHost() {
	targetSproxyd := strings.Split(TargetUrl,",")
	TargetHP = hostpool.NewEpsilonGreedy(targetSproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})
	TargetDriver = TargetDriver
	TargetEnv = TargetEnv
	TargetHost = TargetHost[:0] // reset
	TargetHost = targetSproxyd
}

func SetNewProxydHost1(urls string,driver string) {
	sproxyd := strings.Split(urls,",")
	for  i,h := range sproxyd {
		if len(h) > 0 {
			sproxyd[i] = h + "/" + driver
		}
	}
	HP = hostpool.NewEpsilonGreedy(sproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})
	Driver = Driver
	Env = Env
	Host = Host[:0] // reset
	Host = sproxyd
}

func SetNewTargetProxydHost1(urls string,driver string) {
	targetSproxyd := strings.Split(urls,",")
	for  i,h := range targetSproxyd {
		if len(h) > 0 {
			targetSproxyd[i] = h + "/" + driver
		}
	}
	TargetHP = hostpool.NewEpsilonGreedy(targetSproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})

	TargetDriver = TargetDriver
	TargetEnv = TargetEnv
	TargetHost = TargetHost[:0] // reset
	TargetHost = targetSproxyd
}

