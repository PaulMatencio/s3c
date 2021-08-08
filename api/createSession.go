package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/viper"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)
const (
	CONTIMEOUT = 2000  // connection timeout in ms
	KEEPALIVE = 15000  // keep alive  in ms
)
func CreateSession() *session.Session {

	var (
		sess     *session.Session
		loglevel = *aws.LogLevel(1)
		maxRetries            = 5
		conTimeout, keepAlive int
	)

	if viper.GetInt("loglevel") == 5 {
		loglevel = aws.LogDebug
	}

	if retry := viper.GetInt("transport.retry.number"); retry > 0 {
		maxRetries = retry
	} else {
		if retry = viper.GetInt("transport.retry.number"); retry > 0 {
			maxRetries = retry
		}
	}

	if conTimeout := viper.GetInt("transport.connectionTimeout"); conTimeout == 0 {
		conTimeout = CONTIMEOUT
	}

	if keepAlive := viper.GetInt("transport.connectionTimeout"); keepAlive == 0 {
		keepAlive = KEEPALIVE
	}
	if viper.ConfigFileUsed() == "" {
		myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.S3ServiceID {
				return endpoints.ResolvedEndpoint{
					URL: "http://127.0.0.1:9000",
					//	URL:           "http://10.12.201.11",
					SigningRegion: "us-east-1",
				}, nil
			}
			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}

		sess = session.Must(session.NewSession(&aws.Config{
			Credentials:      credentials.NewSharedCredentials("", "minio-account"),
			EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
			S3ForcePathStyle: aws.Bool(true),
			LogLevel:         aws.LogLevel(loglevel),
		}))

	} else {

		/*
			Hard coded credential taken from application configuration file )
		*/
		/*
			client := http.Client{}

			Transport    := &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   500 * time.Millisecond, // connection timeout
					KeepAlive: 2 * time.Second,

				}).Dial,
				TLSHandshakeTimeout: 10 * time.Second,
			}
			client.Transport = Transport
		*/

		/*
			cache DNS works fine  but not necessary for the moment
		*/
		/*
			client := http.Client{}
			r := &dnscache.Resolver{}
			t := &http.Transport{
				DialContext: func(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
					host, port, err := net.SplitHostPort(addr)
					if err != nil {
						return nil, err
					}
					gLog.Info.Printf("host:%v port:%v",host,port )
					ips, err := r.LookupHost(ctx, host)
					if err != nil {
						return nil, err
					}

					for _, ip := range ips {
						var dialer net.Dialer
						conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
						if err == nil {
							break
						}
					}
					return
				},
			}
			client.Transport = t
		*/

		client := http.Client{}
		Transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(conTimeout) * time.Millisecond, // connection timeout
				KeepAlive: time.Duration(keepAlive) * time.Millisecond,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		if proxy := viper.GetString("transport.proxy.http"); len(proxy) == 0 {
			//Transport.Proxy = http.ProxyFromEnvironment
			gLog.Info.Println("no http proxy - if needed, please add trnsport.proxy.http: http://proxyUri to the config.yaml file to congigure your proxy")
			Transport.Proxy = nil
		} else {
			if proxyHttp, err := url.Parse(proxy); err == nil {
				gLog.Info.Printf("Setting http proxy %s",proxyHttp)
				Transport.Proxy = http.ProxyURL(proxyHttp)
			} else {
				gLog.Warning.Printf("Setting http proxy %s",os.Getenv("http_proxy"))
				Transport.Proxy = http.ProxyFromEnvironment
			}
		}
		client.Transport = Transport
		sess, _ = session.NewSession(&aws.Config{
			Region:                         aws.String(viper.GetString("s3.region")),
			Endpoint:                       aws.String(viper.GetString("s3.url")),
			Credentials:                    credentials.NewStaticCredentials(viper.GetString("credential.access_key_id"), viper.GetString("credential.secret_access_key"), ""),
			DisableRestProtocolURICleaning: aws.Bool(true),
			S3ForcePathStyle:               aws.Bool(true),
			LogLevel:                       aws.LogLevel(loglevel),
			MaxRetries:                     aws.Int(maxRetries),
			HTTPClient:                     &client,
		})
	}
	return sess
}

func CreateSession2(req datatype.CreateSession) *session.Session {

	var (
		sess                  *session.Session
		loglevel              = *aws.LogLevel(1)
		maxRetries            = 3
		conTimeout, keepAlive int
	)

	if viper.GetInt("loglevel") == 5 {
		loglevel = aws.LogDebug
	}

	if retry := viper.GetInt("transport.retry.number"); retry > 0 {
		maxRetries = retry
	} else {
		if retry = viper.GetInt("transport.retry.number"); retry > 0 {
			maxRetries = retry
		}
	}

	if conTimeout := viper.GetInt("transport.connectionTimeout"); conTimeout == 0 {
		conTimeout = CONTIMEOUT
	}

	if keepAlive := viper.GetInt("transport.connectionTimeout"); keepAlive == 0 {
		keepAlive = KEEPALIVE
	}

	if viper.ConfigFileUsed() == "" {
		myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.S3ServiceID {
				return endpoints.ResolvedEndpoint{
					URL:           "http://127.0.0.1:9000",
					SigningRegion: "us-east-1",
				}, nil
			}
			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}
		sess = session.Must(session.NewSession(&aws.Config{
			Credentials:      credentials.NewSharedCredentials("", "minio-account"),
			EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
			S3ForcePathStyle: aws.Bool(true),
			LogLevel:         aws.LogLevel(loglevel),
		}))

	} else {
		if req.MaxRetries > 0 {
			maxRetries = req.MaxRetries
		}

		client := http.Client{}
		// ctx := context.Background()
		Transport := &http.Transport{
			DialContext: ( &net.Dialer{
				Timeout:   time.Duration(conTimeout) * time.Millisecond, // connection timeout
				KeepAlive: time.Duration(keepAlive) * time.Millisecond,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxConnsPerHost: 100,
			// MaxIdleConnsPerHost: 100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		if proxy := viper.GetString("transport.proxy.http"); len(proxy) == 0 {
			//Transport.Proxy = http.ProxyFromEnvironment
			gLog.Info.Println("no http proxy - if needed, please add trnsport.proxy.http: http://proxyUri to the config.yaml file to congigure your proxy")
			Transport.Proxy = nil
		} else {
			if proxyHttp, err := url.Parse(proxy); err == nil {
				gLog.Info.Printf("Setting http proxy %s",proxyHttp)
				Transport.Proxy = http.ProxyURL(proxyHttp)
			} else {
				gLog.Warning.Printf("Setting http proxy %s",os.Getenv("http_proxy"))
				Transport.Proxy = http.ProxyFromEnvironment
			}
		}
		client.Transport = Transport
		sess, _ = session.NewSession(&aws.Config{
			Region:                         aws.String(req.Region),
			Endpoint:                       aws.String(req.EndPoint),
			Credentials:                    credentials.NewStaticCredentials(req.AccessKey, req.SecretKey, ""),
			DisableRestProtocolURICleaning: aws.Bool(true),
			S3ForcePathStyle:               aws.Bool(true),
			LogLevel:                       aws.LogLevel(loglevel),
			MaxRetries:                     aws.Int(maxRetries),
			HTTPClient:                     &client,
		})

	}

	return sess

}
