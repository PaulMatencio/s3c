package sproxyd

import (
	"net"
	"net/http"
	"time"

	hostpool "github.com/bitly/go-hostpool"
)

const (
	Proxy           = "proxy"
	TIME_OUT        = 30   // second
	COPY_TIMEOUT    = 20   // second
	WRITE_TIMEOUT   = 20   // second
	READ_TIMEOUT    = 20   //second
	CONNECT_TIMEOUT = 1000 // ms
	KEEP_ALIVE      = 20   //sec
	RETRY           = 5
	WAITTIME         = 100
)

var (
	Url                 = "http://10.12.202.10:81/,http://10.12.202.12:81/,http://10.12.202.21:81/,http://10.12.202.22:81/,http://10.12.202.25:81/,http://10.12.202.26:81/,http://10.12.202.15:81/,http://10.12.202.16:81/"
	TargetUrl           = "http://10.12.201.170:81/,http://10.12.201.171:81/,http://10.12.201.172:81/,http://10.12.201.174:81/,http://10.11.201.175:81/,http://10.11.201.176:81/"
	Debug               bool                                                /* debug mode */
	Test                bool                                                /*  test mode  */
	HP                  hostpool.HostPool                                   /* source hosts pool */
	TargetHP            hostpool.HostPool                                   /* destination hostpool */
	Driver              = "bpchord"                                         /* default  source sproxyd driver */
	TargetDriver        = "bpchord"                                         /* destination sproxy driver */
	DummyHost           = "http://0.0.0.0:81/"                              /* Used by doRequest.go  to build the url with hostpool */
	DummyHost2          = "http://0.0.0.0:82/"                              /* Used by doRequest.go  to build the url with hostpool */
	Timeout             = time.Duration(TIME_OUT)                           /* GET/PUT timeout */
	CopyTimeout         = time.Duration(COPY_TIMEOUT * time.Second)         /* Copy PNsend 5sec*/
	WriteTimeout        = time.Duration(WRITE_TIMEOUT * time.Second)        /* send time out */
	ReadTimeout         = time.Duration(READ_TIMEOUT * time.Second)         /* receive timeout */
	ConnectionTimeout   = time.Duration(CONNECT_TIMEOUT * time.Millisecond) /* connection timeout */
	ConnectionKeepAlive = time.Duration(KEEP_ALIVE * time.Second)
	Waittime             = time.Duration(WAITTIME* time.Millisecond)        /* wait between 2 retries */

	DoRetry                = RETRY /* number of low level sproxyd Retry if errors */
	Host                   = []string{}
	Env                    = "prod"
	TargetHost             = []string{}
	TargetEnv              = "osa"
	Num200, Num404, Num412 = 0, 0, 0

	//Host = []string{"http://10.12.201.11:81/proxy/bparc/", "http://10.12.201.12:81/proxy/bparc/"}
	// hlist := strings.Split(sproxyd.Url, ",")
	// sproxyd.HP = hostpool.NewEpsilonGreedy(hlist, 0, &hostpool.LinearEpsilonValueCalculator{})

	Transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   ConnectionTimeout,
			KeepAlive: ConnectionKeepAlive,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
)

// sproxyd htp request structure
type HttpRequest struct {
	Hspool    hostpool.HostPool
	Client    *http.Client
	Path      string
	ReqHeader map[string]string
	// Buffer    []byte
}

// sproxyd http response structure
type HttpResponse struct {
	Url      string
	Response *http.Response
	Body     *[]byte
	Err      error
}
