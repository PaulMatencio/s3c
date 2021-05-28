// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	hostpool "github.com/bitly/go-hostpool"
	directory "github.com/paulmatencio/ring/directory/lib"
	sindexd "github.com/paulmatencio/ring/sindexd/lib"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/url"
	"os"
	"sort"
	"strings"
)

var (
	toSindexUrl string
	force, check bool
	toSindexdCmd = &cobra.Command{
		Use:   "toSindexd",
		Short: "Replication of Sindexd tables to remote Sindexd tables",
		Long:  `Replication of Sindexd tables to remote Sindexd tables: 

     See Usage for the description of the other -- flags. See Examples below for full and incremental replications below 
     --index [PN|PD|NP|OM|XX-PN|XX-PD|XX-BN]
     Explanation of the --index flag 
        PN or PD => Full replication of Publication number and Publication data tables for a given country
        BN => Full replication of the Legacy BNS id table for a given country	
        NP => Full replication of Cite NPL table
        OM => Full replication of publication number and publication date tables for other countries
        OB => Full replication of legacy BNS tables for other countries
        XX-PN => Incremental replication of publication number tables ( every or specific country, XP and Cite NPL inclusive )
        XX-PD => Incremental replication of publication number tables ( every or specific country, XP and Cite NPL inclusive)
        XX-BN => Incremental replication of legacy BNS tables ( every country ,XP inclusive)

        Note : XP table contains the indexes of the Non Patent Literature documents
               Cite NPL  contains the indexes of the Cite NPL documents 
               
        Examples

        - Full replication
                    
           - There are 3 indexes tables per large country ( check the documentation for such countries)
				
                sindexd toSindexd  -i PN  -p US -m 500 ( Publication number and Publication date indexes for the US )
                sindexd toSindexd  -i BN  -p US -m 500 ( legacy BNS indexes for the US )
                sindexd toSindexd  -i PN  -p US -m 500 -k <Key1> ( From key1 to replicate from a specific key)
					
           - Small countries indexes are grouped in one table named "OTHER"

                sindexd toSindexd  -i OM -m 500 ( Publication number and Publication Date for the small countries)
                sindexd toSindexd  -i OB -m 500 ( Legacy BNS index the small countries)
					
           - Cite NPL table 
                sindexd toSindexd -i NP -m 1000
     	
        - Incremental replication		
                
                sindexd toSindexd -i XX-PN  -p 20200403  -m 500 ( publication number for every country of April 3,2020 )
                sindexd toSindexd -i XX-PN  -p 20200403/US  ( publication number for for US  of April 3,2020)
                sindexd toSindexd -i XX-PD  -p 20200403  -m 500 ( publication date for every country of April 3,2020 )
                sindexd toSindexd -i XX-BN  -p 20200403/US  ( legacy BNS id for US of April 3,020 )
                sindexd toSindexd -i XX-BN  -p 20200403/US  ( legacy BNS id for US of April 3,2020 )

         Note : XX-PN, includes Cite NPL publication number
                XX-PD  includes Cite NPL publication date
                XX-BN  There is no Cite NPL publication number or publication date
 				`,
		Run: func(cmd *cobra.Command, args []string) {
			toSindexd(cmd,args)
		},
	}

)

type Loaded struct {
	LoadDate string `json:"loadDate""`
	PubDate  string `json:pubdate""`
}

func init() {
	rootCmd.AddCommand(toSindexdCmd)
	initCopyFlags(toSindexdCmd)
}

func initCopyFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl,"sindexd","s","","sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&toSindexUrl,"toSindexd","t","","target sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","the prefix of the key")
	cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table <PN>/<PD>/<BN>/<NP>/<OM>/<OB>")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey,"maxKey","m",100,"maxmimum number of keys to be processed concurrently")
	cmd.Flags().BoolVarP(&force,"force","f",false,"Force to allow overwriting -- Be careful --")
	cmd.Flags().BoolVarP(&check,"check","v",false,"Check mode")

}

func toSindexd(cmd *cobra.Command,args []string) {

	if len(sindexUrl) == 0 {
		if sindexUrl = viper.GetString("sindexd.url"); len(sindexUrl) ==0 {
			gLog.Info.Println("%s", "missing source  sindexed URL");
			os.Exit(2)
		}
	}
	if len(toSindexUrl) == 0 {
		if toSindexUrl = viper.GetString("toSindexd.url"); len(toSindexUrl) == 0 {
			gLog.Info.Println("%s", "missing target sindexed URL");
			os.Exit( 2)
		}
		/* check  from and to Url to avoid overwriting */
		url, _ := url.Parse(sindexUrl)
		toUrl, _ := url.Parse(toSindexUrl)
		toHostname:= toUrl.Hostname()
		hostname := url.Hostname()
		to := strings.Split(toHostname,".")
		from :=  strings.Split(hostname,".")
		if to[0] == from[0] && to[1] == from[1] && to[2]== from[2] {
			gLog.Warning.Printf("From Sindexd cluster %v = to sindexd cluster %v cluster ... use -f to allow overwriting",hostname,toHostname)
			if !force {
				os.Exit(2)
			}
		}
		gLog.Info.Printf("Copying sindexd tables from host %s to host %s ",hostname,toHostname)
	}

	if len(iIndex) == 0 {
		iIndex = "PN"
	}
	// indSpecs := directory.GetIndexSpec(iIndex)
	if len(prefix) == 0 {
		if iIndex == "PN" || iIndex == "PD" || iIndex == "BN" || iIndex == "XX"{
			gLog.Info.Printf("%s", missingPrefix);
			os.Exit(2)
		}
	}

	sindexd.Delimiter = delimiter
	// sindexd.Host = append(sindexd.Host, sindexUrl)
	sindexd.Host = strings.Split(sindexUrl,",")
	sindexd.TargetHost = strings.Split(toSindexUrl,",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})
	sindexd.TargetHP = hostpool.NewEpsilonGreedy(sindexd.TargetHost, 0, &hostpool.LinearEpsilonValueCalculator{})

	switch (iIndex) {
		case "PN","PD": bkupSindexd("",check)  // Used prefix for index
		case "BN": bkupBnsId("",check)  // Use prefix for BNS
		case "OM":   /* Other MOSES and Epoque  indexes*/
			bkupSindexd(iIndex,check)
		case "OB":   /* other legacy BNS indexes */
			bkupBnsId(iIndex,check)
		case "NP":   /* Cite NPL */
			bkupSindexd(iIndex,check)
		case "XX-PN":
			incSindexd("XX","PN",check)
		case "XX-PD":
			incSindexd("XX","PD",check)
		case "XX-BN":
			incSindexd("XX","BN",check)
		default:
		gLog.Info.Printf("%s", "invalid index table : <PN>/<PD>/<BN>/<OM>/<OB>");
		os.Exit(2)
	}
}

func bkupSindexd (index string, check bool)  {

	/*
		List prefix sindexd table ( "PD")
	    for each key,value  {
	       map[key] = value
	       key1 = key  -  YYYY/MM/DD  = Publication number
	       map[key1] =value
	    }
	    add map[key] to target PD  tables
	    add map[key1] to target PN tables

	 */

	indSpecs := directory.GetIndexSpec("PD")
	indSpecs1 := directory.GetIndexSpec("PN")
	num := 0
	keyObj := make(map[string]string)
	keyObj1 := make(map[string]string)
	/*
		Loop until Next marker is false
	 */
	switch (index) {
	case  "OM" :
		prefix = ""
		i := indSpecs["OTHER"]
		i1 := indSpecs1["OTHER"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No OTHER table in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	case "NP" :
		prefix ="XP"
		i := indSpecs["NP"]
		i1 := indSpecs1["NP"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No NP entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	default:
		/*  continue */
	}
	gLog.Info.Printf("Index: %s - Prefix: %s - Start with key %s ",index,prefix,marker)
	for Nextmarker {
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			for k, v := range resp.Fetched {
				/*   key  foramt CC/YYYY/MM/DD/NNNNNNNNNN/KC ( no KC for Cite NPL )    */
				keys := strings.Split(k,"/")
				k1 := keys[0]
				for i := 4; i < len(keys); i++ {
					k1 += "/"+keys[i]
				}
				if v1, err:= json.Marshal(v); err == nil {
					vs := string(v1)
					gLog.Trace.Println(k, vs)
					gLog.Trace.Println(k1,vs)
					keyObj[k] = vs
					keyObj1[k1]= vs
				}
			}

			/*
				add keys to tge PD sindexd tables
			    add keys to the PN sindexd tables
				Exit if any error or sindexd status != 200

			*/
			if !check {
				if r := directory.AddSerialPrefix1(sindexd.TargetHP, iIndex,prefix, indSpecs, keyObj); r.Err == nil {
					if r.Response.Status == 200 {
						if r1 := directory.AddSerialPrefix1(sindexd.TargetHP, iIndex,prefix, indSpecs1, keyObj1); r1.Err != nil {
							gLog.Error.Printf("Error: %v  adding key after marker %s to %s", r1.Err, marker, indSpecs1)
							os.Exit(100)
						} else {
							if r1.Response.Status != 200 {
								gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpecs1)
								os.Exit(100)
							}
						}
					} else {
						gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpecs)
						os.Exit(100)
					}
				} else {
					gLog.Error.Printf("Error: %v adding key after marker %s to %s", r.Err, marker, indSpecs)
					os.Exit(100)
				}
			}

			// Reuse the MAP storage rather then let the Garbage collector free the unused storage
			// this may introduce extra overhead without actual benefit
			for k := range keyObj{ delete(keyObj,k)}
			for k := range keyObj1{ delete(keyObj1,k)}

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				gLog.Info.Printf("Next marker => %s", marker)
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}

func bkupBnsId (index string,check bool)  {
	indSpecs := directory.GetIndexSpec("BN")
	num := 0
	keyObj := make(map[string]string)
	switch (index) {
		case "OB" :
			prefix = ""
			i := indSpecs["OTHER"]
			if i == nil {
				gLog.Error.Printf("No OTHER entry in BN Index spcification tables")
				os.Exit(2)
			}
			gLog.Info.Printf("Indexd specification BN: %v", *i)
		case "NP" :
			prefix ="XP"
			i := indSpecs["NP"]
			if i == nil  {
				gLog.Error.Printf("No NP entry in PD or PN Index spcification tables")
				os.Exit(2)
			}
			gLog.Info.Printf("Indexd specification PN: %v  - PD %v",  *i)
	default:
		/*  continue */
	}
	/*
		Loop until Next marker is false
	*/
	gLog.Info.Printf("Index: %s - Prefix: %s ",index,prefix)
	for Nextmarker {
		if response = directory.GetSerialPrefix(iIndex, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			for k, v := range resp.Fetched {
				if v1, err:= json.Marshal(v); err == nil {
					vs := string(v1)
					gLog.Trace.Println(k, vs)
					keyObj[k] = vs
				}
			}
			if !check {
				if r := directory.AddSerialPrefix1(sindexd.TargetHP, iIndex, prefix, indSpecs, keyObj); r.Err == nil {
					if r.Response.Status != 200 {
						gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpecs)
						os.Exit(100)
					}
				} else {
					gLog.Error.Printf("Error: %v adding key after marker %s to %s", r.Err, marker, indSpecs)
					os.Exit(100)
				}
			}

			// Reuse the MAP storage rather then let the Garbage free the unused storage
			// this may  create overhead without real benefit

			for k := range keyObj{ delete(keyObj,k)}

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				/*
					if num == 10 {
						Nextmarker = false
					}
				*/
				gLog.Info.Printf("Next marker => %s", marker)
			}

		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}


/*    incremental backup */

func incSindexd(index string ,index1 string, check bool) {

    var (
    	Key1 = []string{}
		indSpecs = directory.GetIndexSpec(index) //should be XX
		indSpecs1 = directory.GetIndexSpec(index1) // should be [PN|PD|BN
		keyObj = make(map[string][]byte)
		keyObj1 = make(map[string]string)
		num = 0
		loaded  = new(Loaded)
	)
	for Nextmarker {
		//  retrieve  keys from XX  index table
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			for k, v := range resp.Fetched {
				if v1, err:= json.Marshal(v); err == nil {
					// vs := string(v1)
					gLog.Trace.Println(k, string(v1))
					keyObj[k] = v1
				}
			}
			//  Build an array of document keys  Key1 to be retrieved

			for k, v := range keyObj {
				if err := json.Unmarshal(v, &loaded); err == nil {
					K := strings.Split(k, "/")
					pubDate := loaded.PubDate[0:4] + "/" + loaded.PubDate[4:6] + "/" + loaded.PubDate[6:8]
					if len(K)  ==  4 {
						if index1 == "PD" {
							Key1 = append(Key1, K[1]+"/"+pubDate+"/"+K[2] + "/"+K[3])
						} else {
							Key1 = append(Key1,K[1]+"/"+K[2]+ "/"+K[3]  )
						}
					} else if len(K) == 3 {
						if index1 == "PD" {
							Key1 = append(Key1, K[1]+"/"+pubDate+"/"+K[2] )
						} else {
							Key1 = append(Key1,K[1]+"/"+K[2] )
						}
					}  else {
						gLog.Warning.Printf("Invalid input key: %s is discarded", k)
					}
				}
			}
				//  sort the  key array Key1
			sort.Strings(Key1)

			// retrieve the document new
			// Build an index

			specs := make(map[string][]string)
			for _, v := range Key1 {
				// index := aKey[i][0:2]
				gLog.Trace.Println(v)
				index := v[0:2]
				if index == "XP" {
					pn := strings.Split(v, "/")
					switch (index1) {
					case "PD":
						if pn[4] >= "55000000"  && pn[4] < "56000000"{
							index = "NP"
						}
					case "PN","BN":
						if pn[1] >= "55000000"  && pn[1] < "56000000" {
							index = "NP"
						}

					default:
						gLog.Error.Println("Wrong value of index1 %s",index1)
					}
				}
				if indSpecs1[index] == nil {
					index = "OTHER"
				}
				specs[index] = append(specs[index], v)
			}
			gLog.Trace.Println(specs)
			//  retrieve  key=value from [PN|PD|BN] of source URL
			responses := directory.GetAsyncKeys(specs, indSpecs1)
			var indSpec *sindexd.Index_spec
			for _, r := range responses {
				//  Add fetched keys into  map[string]string
				for k, v := range r.Response.Fetched {
					indSpec = r.Index_Spec
					if v1, err := json.Marshal(v); err == nil {
						vs := string(v1)
						gLog.Trace.Println(k, vs)
						keyObj1[k] = vs
					}
				}
				/*  delete All Key not found from the target URL, one key at a time */
				for _, v := range r.Response.Not_found {
					gLog.Warning.Printf("Key %s is not found in indSpect %v in Host %v",v,*indSpec,sindexd.HP.Hosts())
					indSpec = r.Index_Spec
					// there is no legacy BNS XP  tables
					if v[0:2] != "XP" ||  index1 != "BN" {
						if !check {
							gLog.Warning.Printf("Deleting key %s from indSpec %v in Host %v",v,*indSpec,sindexd.TargetHP.Hosts())
							directory.DeleteSerialKey1(sindexd.TargetHP, indSpec, v)
						} else {
							gLog.Info.Printf("Deleting key %s from indSpec %v on Host %v ", v, *indSpec, sindexd.TargetHP.Hosts())
						}
					}

				}

				if !check {
					if len(keyObj1) > 0 {
						//  Add key=value to target URL
						if r := directory.AddSerialPrefix2(sindexd.TargetHP, indSpec, prefix, keyObj1); r.Err == nil {
							if r.Response.Status != 200 {
								gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpec)
								os.Exit(100)
							}
						} else {
							gLog.Error.Printf("Error: %v adding key after marker %s to %s", r.Err, marker, indSpec)
							os.Exit(100)
						}
					} else {
						gLog.Warning.Printf("Some keys can't be found")
						for _,v := range r.Response.Not_found{
							gLog.Warning.Println(v)
						}
					}
				}
			}
			for k := range keyObj1{ delete(keyObj1,k)}

			// Reuse the MAP storage rather then let the Garbage free the unused storage
			// this may  create overhead without real benefit

			for k := range keyObj{ delete(keyObj,k)}
			Key1 = Key1[:0]

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				gLog.Info.Printf("Next marker => %s", marker)
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}



