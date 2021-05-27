package datatype

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/gLog"
	"os"
)

type Clusters struct {
	Topology []Cluster `json:"topology"`
}

func (c Clusters) GetCluster() []Cluster {
	return c.Topology
}

func  (c Clusters) New(file string) (error,*Clusters) {
	gLog.Trace.Printf("Input json file :%s",file)
	cl:= Clusters{}
	if cFile, err := os.Open(file); err == nil {
		defer cFile.Close()
		return json.NewDecoder(cFile).Decode(&cl),&cl
	} else {
		return err,&cl
	}
}

func (c Clusters) GetHost() (error, *map[string]bool) {

	var (
		s3Host = make(map[string]bool)
		err error
	)
	for _,r := range c.GetCluster() {
		for _,v := range r.Repds {
			s3Host[v.Host]= true
		}
		for _,v := range r.GetWsbs() {
			s3Host[v.Host]= true
		}
	}
	return err,&s3Host
}

type Repds struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}

type Wsbs struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}

type Cluster struct {
	Num   int     `json:"num"`
	Repds []Repds `json:"repds"`
	Wsbs  []Wsbs  `json:"wsbs"`
}

func (c Cluster) GetNum() (int) {
	return c.Num
}
func (c Cluster) GetRepds() []Repds {
	return c.Repds
}
func (c Cluster) GetWsbs() []Wsbs{
	return c.Wsbs
}

type RaftSessions []struct {
	ID                int           `json:"id"`
	RaftMembers       []RaftMembers `json:"raftMembers"`
	ConnectedToLeader bool          `json:"connectedToLeader"`
}

type RaftSession struct {
	ID                int           `json:"id"`
	RaftMembers       []RaftMembers `json:"raftMembers"`
	ConnectedToLeader bool          `json:"connectedToLeader"`

}

type RaftSessionInfo struct {
	Leader       RaftMembers        `json:"leader"`
	Connected    []RaftMembers   `json:"connected"`
	Disconnected []RaftMembers `json:"disconnected"`
}

type RaftMembers struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}

type RaftLeader struct {
	IP string `json:"ip,omitempty"`
	Port int `json:"port,omitempty"`
}

type RaftBucket struct {
	RaftSessionID int    `json:"raftSessionId"`
	Leader        RaftLeader `json:"leader"`
	Creating      bool   `json:"creating"`
	Deleting      bool   `json:"deleting"`
	Version       int    `json:"version"`
}

type RaftState struct {
	Term       int `json:"term"`
	Voted      int `json:"voted"`
	Appended   int `json:"appended"`
	Backups    int `json:"backups"`
	Committing int `json:"committing"`
	Committed  int `json:"committed"`
	Pruned     int `json:"pruned"`
}

func  (c RaftSessions) GetRaftSessions(file string) (error,*RaftSessions) {
	gLog.Trace.Printf("Input json file :%s",file)
	if cFile, err := os.Open(file); err == nil {
		defer cFile.Close()
		return json.NewDecoder(cFile).Decode(&c),&c
	} else {
		return err,&c
	}
}

type HealthCheck struct {
	CnNorth1     HealthCode     `json:"cn-north-1,omitempty"`
	UsWest1      HealthCode     `json:"us-west-1,omitempty"`
	UsEast1      HealthCode     `json:"us-east-1,omitempty"`
	UsEast2      HealthCode     `json:"us-east-2,omitempty"`
	ApNortheast1 HealthCode     `json:"ap-northeast-1,omitempty"`
	SaEast1      HealthCode     `json:"sa-east-1,omitempty"`
	CaCentral1   HealthCode     `json:"ca-central-1,omitempty"`
	ApSoutheast2 HealthCode     `json:"ap-southeast-2,omitempty"`
	UsWest2      HealthCode     `json:"us-west-2,omitempty"`
	Dc1          HealthCode     `json:"dc-1,omitempty"`
	EuCentral1   HealthCode     `json:"eu-central-1,omitempty"`
	EU           HealthCode     `json:"EU,omitempty"`
	ApSoutheast1 HealthCode     `json:"ap-southeast-1,omitempty"`
	ApNortheast2 HealthCode     `json:"ap-northeast-2,omitempty"`
	EuWest2      HealthCode     `json:"eu-west-2,omitempty"`
	Sproxyd      HealthCode    `json:"sproxyd"`
	ApSouth1     HealthCode     `json:"ap-south-1,omitempty"`
	EuWest1      HealthCode     `json:"eu-west-1,omitempty"`
	Bucketclient Bucketclient   `json:"bucketclient"`
	Vault        Vault          `json:"vault"`
}

type HealthCode struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}


type Bucketclient struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Body    interface{}   `json:"body"`
}
type Vault struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Body    interface{}   `json:"body"`
}

func (h HealthCheck) GetVaultCode() Vault {
	return h.Vault
}
func (h HealthCheck) GetBucketCode() Bucketclient {
	return h.Bucketclient
}
func (h HealthCheck) GetEUCode()  HealthCode {
	return h.EU
}

func (h HealthCheck) GetSproxyd()  HealthCode {
	return h.Sproxyd
}

func (h HealthCheck) Print(name string )  {

}








