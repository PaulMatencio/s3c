package datatype

/*   save the init
	if myBdb, err = openBdb(mBDB); err == nil {
		defer myBdb.Close()
		if resume {
			err = restart([]byte(nSpace), []byte(keySuffix+"_"+strconv.Itoa(bInstance)))
			if err != nil {
				gLog.Error.Printf("%v",err)
			}
		}
	} else {
		gLog.Error.Printf("%v", err)
		if resume {
			gLog.Error.Printf("Can't run backup  with --resume=true because of previous error %v",err)
			return
		}
	}
	myContext = SetBackupContext()
	myContext.SetMarker(nextmarker)
	myKey := keySuffix + "_" + strconv.Itoa(bInstance)
	myContext.WriteBdb([]byte(nSpace), []byte(myKey), myBdb)


	incr = false //  full backup of moses backup

	if len(srcBucket) == 0 {
		gLog.Error.Printf(missingSrcBucket)
		return
	}

	if len(tgtBucket) == 0 {
		gLog.Warning.Printf("%s", missingTgtBucket)
		return
	}

	if len(inFile) > 0 || len(iBucket) > 0 {

		if len(inFile) > 0 && len(iBucket) > 0 {
			gLog.Error.Printf("--input-file  and --input-bucket are mutually exclusive", inFile, iBucket)
			return
		}

		if len(inFile) > 0 {
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
				return
			}

			if maxv := viper.GetInt("backup.maximumNumberOfVersions"); maxv > 0 {
				maxVersions = maxv
			}
		}
		incr = true

		if mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s must not have a suffix", srcBucket)
			return
		}
		if mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("target bucket %s must not have a suffix", tgtBucket)
			return
		}
	}

	if len(prefix) > 0 {

		if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}

		if err, suf := mosesbc.GetBucketSuffix(tgtBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				tgtBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the target Bucket %s", suf, tgtBucket)
			}
		}
	} else {

		if !incr && !mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s does not have a suffix. It should be  00..05 ", srcBucket)
			return
		}
		if !incr && !mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("Target bucket %s does not have a suffix. It should be  00..05 ", tgtBucket)
			return
		}
	}

	if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
		gLog.Warning.Printf("%v", err)
		return
	}

	// validate --from-date format
	if len(fromDate) > 0 {
		if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", fromDate)
			return
		}
	}
	// validate --to-date format
	if len(toDate) > 0 {
		if tDate, err = time.Parse(time.RFC3339, toDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", toDate)
			return
		}
	}

	if err = mosesbc.SetSourceSproxyd("backup", srcUrl, driver, env); err != nil {
		return
	}
	maxPartSize = maxPartSize * 1024 * 1024

	if srcS3 = mosesbc.CreateS3Session("backup", "source"); srcS3 == nil {
		gLog.Error.Printf("Failed to create a session with the source S3")
		return
	}

	if tgtS3 = mosesbc.CreateS3Session("backup", "target"); tgtS3 == nil {
		gLog.Error.Printf("Failed to create a session with the target S3")
		return
	}

	if logit {
		if logS3 = mosesbc.CreateS3Session("backup", "logger"); logS3 == nil {
			gLog.Error.Printf("Failed to create a S3 session with the logger s3")
			return
		} else {

			k := "backup.s3.logger.bucket"
			if logBucket = viper.GetString(k); len(logBucket) == 0 {
				gLog.Error.Printf("logger bucket is missing - add %s to the config.yaml file",k)
				return
			}
		}
	}
*/
