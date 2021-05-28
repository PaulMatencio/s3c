package api

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func AddBucketAcl(req datatype.PutBucketAclRequest) (*s3.PutBucketAclOutput,error){

	var (
		result *s3.PutBucketAclOutput
	)

	getAcl := datatype.GetBucketAclRequest {

		Service:req.Service,
		Bucket: req.Bucket,
	}

	if r,err := GetBucketAcl(getAcl); err == nil {

		ACLPolicy := s3.AccessControlPolicy {
			Owner: r.Owner,
		}


		// new Grant to be added

		if req.ACL.Email == ""  {
			err = errors.New("the email address is missing")
			return result,err
		}

		grantee := s3.Grantee {
			DisplayName: &req.ACL.Email,
			ID : &req.ACL.ID,
			Type: &req.ACL.Type,
		}

		permission := req.ACL.Permission

		if !(permission == "FULL_CONTROL" || permission == "WRITE" || permission == "WRITE_ACP" || permission == "READ" || permission == "READ_ACP") {
			err = errors.New("Illegal permission value. It must be one of: FULL_CONTROL, WRITE, WRITE_ACP, READ, or READ_ACP" )
			return result,err

		}

		newGrant := s3.Grant {
			Grantee: &grantee,
			Permission: &permission,
		}

		// Append new Grant to ACL policy grants
		ACLPolicy.Grants= append(r.Grants,&newGrant)

		fmt.Println("====",ACLPolicy)

		input := &s3.PutBucketAclInput{
			Bucket: aws.String(req.Bucket),
			AccessControlPolicy: &ACLPolicy,
		}

		return req.Service.PutBucketAcl(input)

	} else {
		return result,err
	}
}



func PutBucketAcl(req datatype.PutBucketAclRequest) (*s3.PutBucketAclOutput,error){

	var (
		result *s3.PutBucketAclOutput
	)

	getAcl := datatype.GetBucketAclRequest {
		Bucket: req.Bucket,
	}

	if r,err := GetBucketAcl(getAcl); err != nil {

		ACLPolicy := s3.AccessControlPolicy {
			Owner: r.Owner,
		}

		if req.ACL.Email == ""  {
			err = errors.New("the email address is missing")
			return result,err
		}

		grantee := s3.Grantee {
			EmailAddress: &req.ACL.Email,
			Type: &req.ACL.Type,
		}


		permission := req.ACL.Permission
		if !(permission == "FULL_CONTROL" || permission == "WRITE" || permission == "WRITE_ACP" || permission == "READ" || permission == "READ_ACP") {
			err = errors.New("Illegal permission value. It must be one of: FULL_CONTROL, WRITE, WRITE_ACP, READ, or READ_ACP" )
			return result,err

		}

		newGrant := s3.Grant {
			Grantee: &grantee,
			Permission: &permission,
		}
		// Append new Grant to ACL policy grants
		ACLPolicy.Grants= append(r.Grants,&newGrant)

		input := &s3.PutBucketAclInput{
			Bucket: aws.String(req.Bucket),
			AccessControlPolicy: &ACLPolicy,
		}

		return req.Service.PutBucketAcl(input)

	} else {
		return result,err
	}
}

/*
func SetBucketOwner(req datatype.PutBucketAclRequest) (*s3.PutBucketAclOutput,error){

	var (
		result *s3.PutBucketAclOutput
		owner  s3.Owner
	)

	getAcl := datatype.GetBucketAclRequest {
		Bucket: req.Bucket,
	}

	if r,err := GetBucketAcl(getAcl); err != nil {

		if req.ACL.Email == ""  {
			err = errors.New("the email address is missing")
			return result,err
		}

		grantee:= s3.Grantee {
			EmailAddress: &req.ACL.Email,
			Type: &req.ACL.UserType,
		}


		ACLPolicy := s3.AccessControlPolicy {
			Owner:    &owner,
			Grants:   r.Grants,
		}

		// ACLPolicy.SetOwner(&req.ACL.Owner)
		// ACLPolicy.Grants= r.Grants

		input := &s3.PutBucketAclInput{
			Bucket: aws.String(req.Bucket),
			AccessControlPolicy: &ACLPolicy,
		}

		return req.Service.PutBucketAcl(input)

	} else {
		return result,err
	}
}
*/
