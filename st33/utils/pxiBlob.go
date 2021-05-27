package st33

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
)

type pxiBlob struct {
	Key     	string
	DatePub		string
	Size    	int
	Record      int
	Blob   		*bytes.Buffer
}

func  NewPxiBlob(key string, records int) (*pxiBlob) {

	var pxiblob = pxiBlob {
		Key : utils.Reverse(key)+".1",
		Record: records,
		Blob  :	 new(bytes.Buffer),
	}
	return &pxiblob
}

//
//  pxiblob method : buildpxiblob
//  Input : The pxiblob ( object)
//          St33 Reader
//          Current entry of the control file
//  Read St33 record by record of the BLOB
//  return   (number of record, error)
//
func ( blob *pxiBlob ) BuildPxiBlob(r *St33Reader,v Conval) (int, error) {

	var (
		err      			error
		recs        		int = 0
		nrec                int = 0
	)

	gLog.Trace.Printf("Build BLOB  - Buffer pointer => Key: %s - Previous: x'%X' - Current: x'%X'", blob.Key,r.GetPrevious(),r.GetCurrent())
	buf,err := r.Read()    // 	Read the first BLOB  record
	if  IsST33Blob(buf,0) {    //  Check if is it a ST33 BLOB ?
	     if recs,err = buildST33Blob(r,v,buf,blob); err != nil {
	     	return recs,err
		 }
	}  else {    // Just a regular BLOB
        recs, err= buildBlob(r, blob,buf)
	}
	nrec += recs
	return nrec,err
}

//   Build regular blob
//   the total number of recorsd are taken from the control file
//   append every record to form the final blob
//
func buildBlob(r *St33Reader, blob *pxiBlob, buf[]byte) (int, error) {

	gLog.Trace.Printf( "PXIID %s - Key: %s - BLOB records# : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  Previous: x'%X' Current: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),r.Previous, r.Current)
	blob.Blob.Write(buf)  // Append the  first record to the Blob
	recs :=1
	for rec := 2; rec <=  int(blob.Record); rec++ {     // append other records to the BLOB
		if buf,err  := r.Read(); err == nil  {
			blob.Blob.Write(buf)
			recs ++
		}
	}
	gLog.Trace.Printf("Regular Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())
	return recs,nil
}


// build ST33 blob
func buildST33Blob(r *St33Reader, v Conval, buf []byte, blob *pxiBlob )  (int, error) {

	var (
		Big      			binary.ByteOrder = binary.BigEndian
		blobl,blobRecs   	uint16
		blobLength          uint32
		recs                int = 0
		err     			error
	)

	_ = binary.Read(bytes.NewReader(buf[84 : 86]), Big, &blobRecs)       // Number of ST33 Blob records
	_ = binary.Read(bytes.NewReader(buf[214 : 218]), Big, &blobLength)   // Total length of this  ST33 BLOB
	_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &blobl)          // Get the length of image for this  BLOB

	//
	//   Append the first chunk to the Blob buffer
	//

	blobLen := int(blobl)
	blob.Blob.Write(buf[252 : 252+int64(blobl)])   // Append the first record to the blob
	recs = 1
	gLog.Trace.Printf("ST33 BLOB  Key: %s - Total Blob length: %d -  Blob's chunk length: %d  - rec of/Blob recs: %d/%d - Prev: x'%X' - Cur: x'%X'", blob.Key, blobLength, blobl, 1, blobRecs,r.GetPrevious(),r.GetCurrent())

	 //
	 //  Append all the St33 records to blob
	 //  Since it is an ST33, the number of records is taken from the ST33 header
	 //

	for rec:= 2; rec <= int(blobRecs); rec ++ {    // Read  all  the other records
		if buf,err = r.Read(); err == nil  {
			recs++
			if len(buf) > 252 {
				err = binary.Read(bytes.NewReader(buf[250:252]), Big, &blobl) // blobl =length of a  chunk
				gLog.Info.Printf("ST33 BLOB  Key: %s - ST33 Blob's chunk length: %d  - rec of/ Blob recs: %d/%d - Prev @: x'%X' - Cur @: x'%X' ", blob.Key, blobl, rec, blobRecs, r.Previous, r.Current)
				blobLen += int(blobl) //  add the lenght of this chunk to the total length of this BLOB
				if int(blobl) + 252 > len(buf) {
					blob.Blob.Write(buf[252:]) // Append this  chunk to the BLOB
				} else {
					blob.Blob.Write(buf[252 : 252+int64(blobl)]) // Append this  chunk to the BLOB
				}
			}
		} else {
			gLog.Error.Printf("Error %v after reading BLOB record number %d from input data file at Prev : X'%x'  Cur: X'%x' position",err,recs, r.Previous, r.Current)
			return recs,err
		}
	}

	//
	// Continue to build the blob with the other records apart from the ST33 header
	// the other number of records are taken from the control file
	//

	if int(blobLength) != blobLen { //
		err = errors.New(fmt.Sprintf("==> ST33 Blob bad Header. %s - Total length:%d != Blob length:%d",blob.Size,blobLen,blobLength))
		gLog.Error.Printf("%v",err)
	}

	gLog.Trace.Printf("PXIID:%s - ST33 BLOB Key: %s - remaining Blob records: %d ",v.PxiId, blob.Key,blob.Record)

	//
	//  Read  the other blob  records from the control file
	//  The other blob records are taken from the control file
	//
	//  v.Records == blob.Record ( set by newPXIBlob() )
	//
	//
	 /*
	for rec := 1; rec <= int(blob.Record); rec++ {
		if buf,err = r.Read(); err == nil  {
			gLog.Info.Printf("Other blob record - Buffer length %d - Prev @: X'%x' - Cur @: X'%x' ",len(buf),r.Previous,r.Current)
			recs ++
			blob.Blob.Write(buf)
		} else   {
			break
		}
	}
	  */

	gLog.Trace.Println("PXIID: %s - Key %s - Blob control records/Read records %d/%d",v.PxiId,utils.Reverse(v.PxiId),blobRecs,recs)

	if v.Records != recs {
		gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file ",v.PxiId,v.Records,recs)
		diff := v.Records - recs
		if diff < 0 {
			RewindST33(v,r,diff)
			recs -= diff
		} else {
			SkipST33(v,r,diff)
			recs += diff
		}
	}
	gLog.Info.Printf("ST33 Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())
	return recs,err
}

