package id

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	// "github.com/aws/aws-sdk-go/aws/endpoints"
	// "github.com/aws/aws-sdk-go/service/iam"
	"github.com/spf13/viper"
)

func CreateSession() (*session.Session, error) {

	var (
		sess     *session.Session
		loglevel  = *aws.LogLevel(1)
		err error
	)

	if viper.GetInt("loglevel") == 5 {
		loglevel = aws.LogDebug
	}

		/*
			Hard coded credential taken from application configuration file )
		*/

		sess, err = session.NewSession(&aws.Config{

			Region:           aws.String(viper.GetString("iam.region")),
			Endpoint:         aws.String(viper.GetString("iam.url")),
			Credentials:      credentials.NewStaticCredentials(viper.GetString("credential.access_key_id"), viper.GetString("credential.secret_access_key"), ""),
			S3ForcePathStyle: aws.Bool(true),
			LogLevel:         aws.LogLevel(loglevel),

		})

	return sess,err

}