package utils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
)

func PrintBucketAcl( r *s3.GetBucketAclOutput) {
	// fmt.Println(r)
	fmt.Printf("\nBucket owner:\n Display name:%s\n ID:%s\n",*r.Owner.DisplayName,*r.Owner.ID)
	PrintGrantee(r.Grants)


}

func PrintObjectAcl( r *s3.GetObjectAclOutput) {
	fmt.Printf("\nBucket owner:\n Display name:%s\n ID:%s\n",*r.Owner.DisplayName,*r.Owner.ID)
	PrintGrantee(r.Grants)

}

func PrintGrantee(grant []*s3.Grant ) {
	for _,v := range grant {
		fmt.Printf("Grantee:\n %v\n", *v.Grantee)
		fmt.Printf(" Permission %s\n",*v.Permission)
	}
}
