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

func SetNewProxydHost1(urls string) {
	sproxyd := strings.Split(urls,",")
	HP = hostpool.NewEpsilonGreedy(sproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})
	Driver = Driver
	Env = Env
	Host = Host[:0] // reset
	Host = sproxyd
}

func SetNewTargetProxydHost1(urls string) {
	targetSproxyd := strings.Split(urls,",")
	TargetHP = hostpool.NewEpsilonGreedy(targetSproxyd, 0, &hostpool.LinearEpsilonValueCalculator{})
	TargetDriver = TargetDriver
	TargetEnv = TargetEnv
	TargetHost = TargetHost[:0] // reset
	TargetHost = targetSproxyd
}

