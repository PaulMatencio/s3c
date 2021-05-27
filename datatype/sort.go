package datatype

import  "github.com/aws/aws-sdk-go/service/s3"


// sorting an   CompletePart
type ByPart []*s3.CompletedPart
func (ar ByPart) Len() int           { return len(ar) }
func (ar ByPart) Less(i, j int) bool { return *(ar[i].PartNumber) < *(ar[j].PartNumber) }
func (ar ByPart) Swap(i, j int)      { ar[i], ar[j] = ar[j], ar[i] }

