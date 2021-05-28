package st33


func GetPage( r *St33Reader,v Conval,cp int) (*PxiImg, int,error,error){
	var (
		err, err1 error
		nrec  int
		image = NewPxiImg()
	)
	nrec, err, err1 = image.BuildTiffImageV2(r, v,cp)
	return image,nrec,err,err1
}