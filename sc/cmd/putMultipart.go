package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"io"
	"sort"
	"strings"

	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/spf13/cobra"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var (
	putMulipartcmd = &cobra.Command{
		Use:   "putMultipart",
		Short: "Command to upload object in Multi parts to S3",
		Long:  ``,

		Run: func(cmd *cobra.Command, args []string) {
			putMultipart(cmd, args)
		},
	}
	maxPartSize, curr, remaining, partSize int64
	partNumber, maxCon                     int
	completedParts                         []*s3.CompletedPart
)

const (
	MinPartSize                = 5 * 1024 * 1024     // 5 MB
	MaxFileSize                = MinPartSize * 409.6 // 2 GB
	DefaultDownloadConcurrency = 5
	Dummy                      = "/dev/null"
)

type Resp struct {
	Cp  *s3.CompletedPart
	Cl  int64
	Err error
}

func initMPUFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&datafile, "datafile", "i", "", "the data file you 'd like to  upload")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "m", 5, "Maximum part size(MB)")
	cmd.Flags().IntVarP(&maxCon, "maxCon", "M", 0, "Maximum concurrent parts upload, 0 => all parts")
}

func init() {
	RootCmd.AddCommand(putMulipartcmd)
	RootCmd.MarkFlagRequired("bucket")
	initMPUFlags(putMulipartcmd)
}

func putMultipart(cmd *cobra.Command, args []string) {

	var (
		fileSize int64
		fd       *os.File
		err      error
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}

	if len(datafile) == 0 {
		gLog.Warning.Printf("%s", missingInputFile)
		return
	}
	maxPartSize= maxPartSize *1024*1024   // convert into byte
	if maxPartSize < MinPartSize {
		gLog.Error.Printf("Minimum maxPartize is %d", MinPartSize)
		return
	}
	/*
			check if input has separator
		    if not then join datafile with the current work directory
	*/

	if sep := strings.Split(datafile, string(os.PathSeparator)); len(sep) == 1 {
		cwd, _ := os.Getwd()
		datafile = filepath.Join(cwd, datafile)
	}

	if fd, err = os.Open(datafile); err != nil {
		gLog.Error.Printf("Error %v opening file %s", err, datafile)
		return
	} else {
		if fileInfo, err := fd.Stat(); err != nil {
			gLog.Error.Printf("Error %v opening file %s", err, datafile)
			return
		} else {
			fileSize = fileInfo.Size()
		}
	}

	svc := s3.New(api.CreateSession())
	if maxCon > 0 {
		UploadMultipart3(fd, fileSize, svc)
		return
	}
	if fileSize < MaxFileSize {
		UploadMultipart1(fd, fileSize, svc) //read all in memory
	} else {
		UploadMultipart2(fd, fileSize, svc)
	}

}

func UploadMultipart1(fd *os.File, fileSize int64, svc *s3.S3) {

	var (
		buffer = make([]byte, fileSize)
		n, t   = 0, 0
		bytes  int
		err    error
	)
	defer fd.Close()

	if bytes, err = fd.Read(buffer); err != nil {
		gLog.Error.Printf("ReadBuf error %v", err)
	} else {
		gLog.Info.Printf("Read %d bytes", bytes)
	}

	fType := http.DetectContentType(buffer)

	/* */
	key := filepath.Base(fd.Name())

	create := datatype.CreateMultipartUploadRequest{
		Service:     svc,
		Bucket:      bucket,
		Key:         key,
		ContentType: fType,
	}
	if resp, err := api.CreateMultipartUpload(create); err == nil {

		partNumber := 1
		upload := datatype.UploadPartRequest{
			Service: svc,
			Resp:    resp,
		}
		remaining = int64(len(buffer))
		ch := make(chan *Resp, 10)

		start := time.Now()
		for curr = 0; remaining != 0; curr += partSize {
			if remaining < maxPartSize {
				partSize = remaining
			} else {
				partSize = maxPartSize
			}
			content := buffer[curr : curr+partSize]
			n++
			go func(upload datatype.UploadPartRequest, partNumber int, content []byte) {
				// gLog.Trace.Printf("Uploading part :%d- Size %d", partNumber, len(content))
				upload.PartNumber = partNumber
				upload.Content = content
				if completedPart, err := uploadPart(upload); err == nil {
					ch <- &Resp{
						Cp:  completedPart,
						Cl:  int64(len(content)),
						Err: nil,
					}
				} else {
					ch <- &Resp{
						Cp:  nil,
						Err: err,
					}
				}
			}(upload, partNumber, content)
			remaining -= partSize
			partNumber++
		}
		done := false
		for ok := true; ok; ok = !done {
			select {
			case cp := <-ch:
				t++
				if cp.Err == nil {
					gLog.Trace.Printf("Appending completed part %d - Etag %s - size %d", *cp.Cp.PartNumber, *cp.Cp.ETag, cp.Cl)
					completedParts = append(completedParts, cp.Cp)
				} else {
					gLog.Error.Printf("Error %v uploading part %d size %d", cp.Err, cp.Cp.PartNumber, cp.Cl)
				}
				if t == n {
					gLog.Info.Printf("%d objects are uploaded to bucket %s", n, bucket)
					done = true
				}
			case <-time.After(50 * time.Millisecond):
				fmt.Printf("w")
			}
		}

		//   completed the multipart uploaded
		completedUpload(svc, resp, completedParts)

		elapsed := time.Since(start)
		MBsec := 1000.00 * (float64(fileSize) / float64(elapsed))
		gLog.Info.Printf("Elapsed time %v MB/sec %2.f", elapsed, MBsec)
	} else {
		gLog.Error.Printf("Created Multipart Error %v", err)
	}
}

func completedUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, part []*s3.CompletedPart) {
	sort.Sort(datatype.ByPart(part))
	comp := datatype.CompleteMultipartUploadRequest{
		Service:        svc,
		Resp:           resp,
		CompletedParts: part,
	}
	if cResp, err := api.CompleteMultipartUpload(comp); err == nil {
		gLog.Info.Printf("Key: %s - Etag: %s - Bucket: %s", *cResp.Key, *cResp.ETag, *cResp.Bucket)
	} else {
		gLog.Error.Printf("%v", err)
	}
}

func printParts(part []*s3.CompletedPart) {
	sort.Sort(datatype.ByPart(part))
	for _, v := range part {
		fmt.Printf("Part number:%d\tEtag %s\n", *v.PartNumber, *v.ETag)
	}
}

func UploadMultipart2(fd *os.File, fileSize int64, svc *s3.S3) {

	var (
		n, t  int   = 0, 0
		at    int64 = 0
		bytes int
	)
	defer fd.Close()

	numberPart := uint64(math.Ceil(float64(fileSize) / float64(maxPartSize)))

	key := filepath.Base(fd.Name())
	create := datatype.CreateMultipartUploadRequest{
		Service: svc,
		Bucket:  bucket,
		Key:     key,
	}

	ch := make(chan *Resp, 10)

	if resp, err := api.CreateMultipartUpload(create); err == nil {

		upload := datatype.UploadPartRequest{
			Service: svc,
			Resp:    resp,
		}

		// ch:= make(chan *Resp,10)
		start := time.Now()
		partSize = maxPartSize
		remaining = fileSize
		for p := 1; p <= int(numberPart); p++ {

			buffer := make([]byte, partSize)
			if bytes, err = fd.ReadAt(buffer, at); err != nil {
				gLog.Error.Printf("ReadBuf error %v", err)
				return
			} else {
				gLog.Trace.Printf("Part %d - Reading at: %d for %d bytes\n", p, at,bytes)
			}

			n++ //increment the number of read

			go func(upload datatype.UploadPartRequest, p int, buffer []byte) {

				upload.PartNumber = p
				upload.Content = buffer
				if completedPart, err := uploadPart(upload); err == nil {
					ch <- &Resp{
						Cp:  completedPart,
						Cl:  int64(len(buffer)),
						Err: nil,
					}
				} else {
					ch <- &Resp{
						Cp:  nil,
						Err: err,
					}
				}
			}(upload, p, buffer)

			at += partSize
			remaining -= partSize
			if remaining > maxPartSize {
				partSize = maxPartSize
			} else {
				partSize = remaining
			}

			buffer = nil

		}
		done := false
		for ok := true; ok; ok = !done {
			select {
			case cp := <-ch:
				t++
				if cp.Err == nil {
					gLog.Trace.Printf("Appending completed part %d - Etag %s - size %d", *cp.Cp.PartNumber, *cp.Cp.ETag, cp.Cl)
					completedParts = append(completedParts, cp.Cp)
				} else {
					gLog.Error.Printf("Error %v uploading part %d size %d", cp.Err, cp.Cp.PartNumber, cp.Cl)
				}
				if t == n {
					gLog.Info.Printf("%d objects are uploaded to bucket %s", n, bucket)
					done = true
				}
			case <-time.After(50 * time.Millisecond):
				fmt.Printf("w")
			}
		}

		//   completed the multipart uploaded

		completedUpload(svc, resp, completedParts)
		elapsed := time.Since(start)
		MBsec := 1000.00 * (float64(fileSize) / float64(elapsed))
		gLog.Info.Printf("Elapsed time %v MB/sec %2.f", elapsed, MBsec)

	} else {
		gLog.Error.Printf("Created Multipart Error %v", err)
	}
}

func UploadMultipart3(fd *os.File, fileSize int64, svc *s3.S3) {

	var (
		n, t  int   = 0, 0
		at    int64 = 0
		// bytes int
	)
	defer fd.Close()

	numberPart := int64(math.Ceil(float64(fileSize) / float64(maxPartSize)))

	key := filepath.Base(fd.Name())
	create := datatype.CreateMultipartUploadRequest{
		Service: svc,
		Bucket:  bucket,
		Key:     key,
	}

	ch := make(chan *Resp, 10)

	if resp, err := api.CreateMultipartUpload(create); err == nil {

		upload := datatype.UploadPartRequest{
			Service: svc,
			Resp:    resp,
		}

		// ch:= make(chan *Resp,10)
		start0 := time.Now()
		partSize = maxPartSize
		remaining = fileSize

		q1 := int(math.Ceil(float64(numberPart / int64(maxCon))))

		if numberPart%int64(maxCon) == 0 {
			q1--
		}

		for q := 0; q <= q1; q++ {

			start := time.Now()

			b := (q * maxCon) +1
			e := (q + 1) * maxCon
			if e > int(numberPart) {
				e = int(numberPart)
			}
			totalSize := 0;tot:=0;
			for p := b; p <= e; p++ {

				buffer := make([]byte, partSize)
				tot += len(buffer)
				gLog.Trace.Printf("Part %d - Reading at: %d - Buffer size: %d - total size: %d\n", p, at,len(buffer),tot)
				if _, err = fd.ReadAt(buffer, at); err != nil {
					gLog.Error.Printf("ReadBuf error %v", err)
					return
				}
				totalSize +=len(buffer)

				n++
				go func(upload datatype.UploadPartRequest, p int, buffer []byte) {
					upload.PartNumber = p
					upload.Content = buffer
					if completedPart, err := uploadPart(upload); err == nil {
						ch <- &Resp{
							Cp:  completedPart,
							Cl:  int64(len(buffer)),
							Err: nil,
						}
					} else {
						ch <- &Resp{
							Cp:  nil,
							Err: err,
						}
					}
				}(upload, p, buffer)

				at += partSize   // next read
				remaining -= partSize
				if remaining > maxPartSize {
					partSize = maxPartSize
				} else {
					partSize = remaining
				}

				buffer = nil
			}

			done := false
			for ok := true; ok; ok = !done {
				select {
				case cp := <-ch:
					t++
					if cp.Err == nil {
						gLog.Trace.Printf("Appending completed part %d - Etag %s - size %d", *cp.Cp.PartNumber, *cp.Cp.ETag, cp.Cl)
						completedParts = append(completedParts, cp.Cp)
					} else {
						gLog.Error.Printf("Error %v uploading part %d size %d", cp.Err, cp.Cp.PartNumber, cp.Cl)
					}
					if t == n {
						gLog.Info.Printf("%d objects are uploaded to bucket %s", n, bucket)
						done = true
					}
				case <-time.After(50 * time.Millisecond):
					fmt.Printf("w")
				}
			}
			gLog.Info.Printf("Number of objects uploaded:%d\tTotal uploaded size: %d\tfile Size: %d \tElapsed time %v", n, totalSize,fileSize,time.Since(start))
			n = 0
			t = 0
		}

		completedUpload(svc, resp, completedParts)
		elapsed := time.Since(start0)
		MBsec := 1000.00 * (float64(fileSize) / float64(elapsed))
		gLog.Info.Printf("Elapsed time:%v\tMB/sec:%2.f", elapsed, MBsec)

	} else {
		gLog.Error.Printf("Created Multipart Error:%v", err)
	}

}

func uploadPart(upload datatype.UploadPartRequest) (*s3.CompletedPart, error) {

	gLog.Trace.Printf("Uploading part :%d - Size %d", upload.PartNumber, len(upload.Content))
	upload.RetryNumber = retryNumber
	upload.WaitTime = waitTime

	if completedPart, err := api.UploadPart(upload); err != nil {
		gLog.Error.Printf("Error: %v uploading part:%d", err, partNumber)
		gLog.Warning.Printf("Aborting multipart upload")
		abort := datatype.AbortMultipartUploadRequest{
			Service: upload.Service,
			Resp:    upload.Resp,
		}
		if err = api.AbortMultipartUpload(abort); err != nil {
			gLog.Error.Printf("Error %v", err)
		}
		return nil, err
	} else {
		return completedPart, err
	}
}

func listMultipartObject(bucket string, key string) {

	var partnumber int64 = 1
	var w io.WriterAt
	svc := s3.New(api.CreateSession())
	gLog.Info.Printf("Downloading key %s", key)
	// Create a downloader with the s3 client and custom options
	downLoader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
		d.PartSize = 5 * 1024 * 1024
		d.Concurrency = 5

	})

	input := s3.GetObjectInput{
		Key:        aws.String(key),
		Bucket:     aws.String(bucket),
		PartNumber: aws.Int64(partnumber),
	}

	if n, err := downLoader.Download(w, &input); err == nil {
		gLog.Info.Printf("Download %d", n)
	} else {
		gLog.Error.Printf("%v", err)
	}

}
