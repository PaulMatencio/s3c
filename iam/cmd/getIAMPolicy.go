

package cmd

import (
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/iam/id"
	"github.com/spf13/cobra"
)

// getIAMPolicyCmd represents the getIAMPolicy command
var getIAMPolicyCmd = &cobra.Command{
	Use:   "getIAMPolicy",
	Short: "Command to get scality IAM policy",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		getPolicy(cmd,args)
	},
}

func init() {
	RootCmd.AddCommand(getIAMPolicyCmd)
}


func getPolicy(cmd *cobra.Command, args []string) {

	 if session,err  := id.CreateSession(); err == nil {
	 	// create an IAM Service Client
		 svc := iam.New(session)
		 arn := "arn:aws:iam::aws:policy/FullAccesPxiTest"
		 result, err := svc.GetPolicy(&iam.GetPolicyInput{
			 PolicyArn: &arn,
		 })
		 if err == nil {
			 gLog.Info.Printf("%s - %s\n", arn, *result.Policy.Description)
		 } else {
			 gLog.Error.Printf("%v",err)
		 }
	 } else {
	 	gLog.Info.Printf("Create session failed %v",err)
	 }
}
