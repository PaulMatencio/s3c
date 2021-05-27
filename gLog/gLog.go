package gLog

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)


var (

	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
	Fatal   *log.Logger
	Debug   *log.Logger
)



func InitLog( cmd string, loglevel int, logOutput string) (*os.File,  *os.File,  *os.File,  *os.File, *os.File ) {


	//hostname, _ := os.Hostname()
	// pid := os.Getgid()

	var (
		 trf, waf, inf, erf,ftf  *os.File

	)

	if strings.ToLower(logOutput) == "terminal" {

		switch loglevel {

			case 1: Init(ioutil.Discard, ioutil.Discard, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
				break
			case 2: Init(ioutil.Discard, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
				break
			case 3: Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
				break
			case 4:  Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,os.Stdout,ioutil.Discard)
			case 5:  Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,os.Stdout,ioutil.Discard)
			default: Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
		}


	}   else {

		logOutput = filepath.Join(logOutput,"pid_"+strconv.Itoa(os.Getpid()))
		logPath := filepath.Join(logOutput,cmd)
		_, err := os.Stat(logOutput)

		if os.IsNotExist(err) {
			if err := os.MkdirAll(logOutput, 0755); err != nil {
				log.Printf("Error %v creating %s ", err, logOutput)
				log.Printf("Default logging")
				Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
			}
		}

		var (
			traceLog = logPath  + "_trace.log"
			// debugLog = logPath  + "_debug.log"
			infoLog = logPath + "_info.log"
			warnLog = logPath  + "_warning.log"
			errLog = logPath  + "_error.log"
			fatalLog = logPath  + "_fatal.log"

			trf, err1 = os.OpenFile(traceLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			inf, err2 = os.OpenFile(infoLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			waf, err3 = os.OpenFile(warnLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			erf, err4 = os.OpenFile(errLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			ftf, err5 = os.OpenFile(fatalLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			// def, err6  = os.OpenFile(debugLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
		)

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5  != nil  {
			log.Printf("Errors opening log files %v %v %v %v %v ",err1,err2,err3,err4,err5)
			log.Printf("Default logging")
			Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)

		} else {
			switch loglevel {
				case 1: Init(ioutil.Discard, ioutil.Discard, io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
						break
				case 2: Init(ioutil.Discard, io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
						break
				case 3: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
						break
				case 4: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),io.Writer(trf),ioutil.Discard)
				default: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
			}

		}
	}
	return  inf, waf, erf,ftf, trf
}

func InitLog1( cmd string, loglevel int, logOutput string) (*os.File,  *os.File,  *os.File,  *os.File, *os.File ) {


	//hostname, _ := os.Hostname()
	// pid := os.Getgid()

	var (
		trf, waf, inf, erf,ftf  *os.File

	)

	if strings.ToLower(logOutput) == "terminal" {

		switch loglevel {

		case 1: Init(ioutil.Discard, ioutil.Discard, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
			break
		case 2: Init(ioutil.Discard, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
			break
		case 3: Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
			break
		case 4:  Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,os.Stdout,ioutil.Discard)
		case 5:  Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,os.Stdout,ioutil.Discard)
		default: Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
		}


	}   else {

		logOutput = filepath.Join(logOutput,"pid_"+strconv.Itoa(os.Getpid()))
		logPath := filepath.Join(logOutput,cmd)
		_, err := os.Stat(logOutput)

		if os.IsNotExist(err) {
			if err := os.MkdirAll(logOutput, 0755); err != nil {
				log.Printf("Error %v creating %s ", err, logOutput)
				log.Printf("Default logging")
				Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)
			}
		}

		var (
			traceLog = logPath  + ".log"
			// debugLog = logPath  + "_debug.log"
			infoLog = logPath + ".log"
			warnLog = logPath  + ".log"
			errLog = logPath  + ".log"
			fatalLog = logPath  + ".log"

			trf, err1 = os.OpenFile(traceLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			inf, err2 = os.OpenFile(infoLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			waf, err3 = os.OpenFile(warnLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			erf, err4 = os.OpenFile(errLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			ftf, err5 = os.OpenFile(fatalLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
			// def, err6  = os.OpenFile(debugLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0744)
		)

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5  != nil  {
			log.Printf("Errors opening log files %v %v %v %v %v ",err1,err2,err3,err4,err5)
			log.Printf("Default logging")
			Init(os.Stdout, os.Stdout, os.Stderr,os.Stderr,ioutil.Discard,ioutil.Discard)

		} else {
			switch loglevel {
			case 1: Init(ioutil.Discard, ioutil.Discard, io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
				break
			case 2: Init(ioutil.Discard, io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
				break
			case 3: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
				break
			case 4: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),io.Writer(trf),ioutil.Discard)
			default: Init(io.Writer(inf), io.Writer(waf), io.Writer(erf),io.Writer(ftf),ioutil.Discard,ioutil.Discard)
			}

		}
	}
	return  inf, waf, erf,ftf, trf
}



func Init( infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer, fatalHandle io.Writer , traceHandle io.Writer, debugHandle io.Writer) {

	// Time := log.Lmicroseconds
	Time := log.Ltime

	Trace = log.New(traceHandle,
		"TRACE: ",
		log.Ldate|Time|log.Lshortfile)

	Debug = log.New(debugHandle,
		"DEBUG: ",
		log.Ldate|Time|log.Lshortfile)


	Info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|Time|log.Lshortfile)

	Warning = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|Time|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|Time|log.Lshortfile)

	Fatal = log.New(fatalHandle,
		"FATAL: ",
		log.Ldate|Time|log.Lshortfile)

}

