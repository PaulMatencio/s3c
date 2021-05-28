package st33

type PxiMeta struct {
	PxiId   	string
	PageNum 	string
	RefNum  	string
	DataType 	string
	Bucket  	string
	NumPages    int
}


type UserMd struct {
	Pximeta PxiMeta
	Pages   int
}


func  New() PxiMeta{
	return PxiMeta {
	}
}

func (pxi *PxiMeta) GetPxiId() string {
	return pxi.PxiId
}

func (pxi *PxiMeta) GetRefNum() string {
	return pxi.RefNum
}

func(pxi *PxiMeta) GetBucket() string {
	return pxi.Bucket
}

func(pxi *PxiMeta) GetNumPages() int {
	return pxi.NumPages
}

func(pxi *PxiMeta)getBucket() string {
	return pxi.Bucket
}

func(pxi *PxiMeta)getDataType() string {
	return pxi.DataType
}






