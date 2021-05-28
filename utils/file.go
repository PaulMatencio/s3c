package utils

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Rf struct {
	buf []byte
	err error
}

func ReadFile(filename string) ([]byte , error) {
	var (
		data []byte
		err error
	)
	if r,err := os.Open(filename); err == nil {
		return ioutil.ReadAll(r)
	}
	return data,err
}

func ReadDirectory( dirname string, filter string) ([]string, error) {

	var  (
		files []string
		fname string
		err error
	)
	if filesinfo, err := ioutil.ReadDir(dirname); err == nil {
		for _,finfo:= range filesinfo {
			fname = finfo.Name()
			if len(filter) == 0 || strings.Contains(fname,filter) {
				files = append(files, fname)
			}
		}
	}
	return files,err
}

func AsyncReadFiles(entries []string) []*Rf{

	ch := make(chan *Rf)
	files := []*Rf{}
	treq := 0

	for _, entry := range entries {
		treq += 1
		go func(entry string) {
			buf, err := ReadFile(entry)
			// fmt.Println(entry, len(buf))
			ch <- &Rf{buf,err}
		}(entry)
	}
	for {
		select {
		case r := <-ch:
			files = append(files, r)
			if len(files) == treq {
				return files
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf("r")
		}
	}
	return files
}

func Exist(path string) (bool) {
	_, err := os.Stat(path)
	if err == nil { return true }
	return false

}

func ReadBuffer(filename string) (*bytes.Buffer, error) {
	fp, e := os.Open(filename)
	if e == nil {
		defer fp.Close()
		fi, e := fp.Stat()
		var n int64
		n = fi.Size() + bytes.MinRead
		buf := make([]byte, 0, n)
		buffer := bytes.NewBuffer(buf)
		_, e = buffer.ReadFrom(fp)
		return buffer, e
	} else {
		return nil, e
	}
}

func ReadLines(filename string) error {
	fp, err := os.Open(filename)
	if err == nil {
		defer fp.Close()
		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}
	return err
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func ScanLines(scanner *bufio.Scanner, num int) ([]string, error) {
	var (
		k   int = 0
		err error
	)
	linea := make([]string, num, num*2)
	stop := false

	for !stop {
		scanner.Scan()
		eof := false
		if text := scanner.Text(); len(text) > 0 {
			linea[k] = text
			k++
		} else {
			eof = true
		}
		err = scanner.Err()
		if k >= num || eof || err != nil {
			stop = true
		}
	}

	return linea[0:k], err
}

func ScanAllLines(scanner *bufio.Scanner) ([]string, error) {
	var (
		err error
		linea []string
	)
	stop := false

	for !stop {
		scanner.Scan()
		eof := false
		if text := scanner.Text(); len(text) > 0 {

			linea = append(linea,text)

		} else {
			eof = true
		}
		err = scanner.Err()
		if eof || err != nil {
			stop = true
		}
	}

	return linea, err
}


func Scanner(pathname string) (*bufio.Scanner, error) {
	var (
		err     error
		scanner *bufio.Scanner
		// fp      *os.File
	)
	if len(pathname) > 0 {
		fp, err := os.Open(pathname)
		if err == nil {
			// defer fp.Close()
			scanner = bufio.NewScanner(fp)
		} else {
			return nil, err
		}
	} else {
		err = errors.New(pathname + " is empty")
	}
	return scanner, err
}



func WriteFile(filename string, buf []byte, mode os.FileMode) error {
	var err error
	if err = ioutil.WriteFile(filename, buf, mode); err != nil {
		gLog.Warning.Printf("Err %v Writing %s",err, filename)
	}
	return err
}

func AsyncWriteFiles(entries []string, buf [][]byte, mode os.FileMode) []*Rf {
	ch := make(chan *Rf)
	resp := []*Rf{}
	treq := 0
	for k, entry := range entries {
		treq += 1
		go func(entry string) {
			err := WriteFile(entry, buf[k], mode)
			ch <- &Rf{nil,err}
		}(entry)
	}
	for {
		select {
		case r := <-ch:
			resp = append(resp, r)
			if len(resp) == treq {
				return resp
			}
		case <-time.After(50* time.Millisecond):
			fmt.Printf("w")
		}
	}
	return resp
}

func ListFile(root string) {
	if err := filepath.Walk(root,visit); err != nil {

	}
}

func visit(path string, fi os.FileInfo, err error) error {
	var (
		err1 error
		matched bool
	)
	if matched,err1 = filepath.Match("*.conf",fi.Name()); err == nil {
		gLog.Info.Println(path,matched)
	}
	return err1
}

