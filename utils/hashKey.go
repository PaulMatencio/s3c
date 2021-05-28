package utils

func HashKey(key string, modulo int) (int) {
	v := 0;
	for k :=0; k < len(key); k++ {
		v += int(key[k])
	}
	return v % modulo
}

func KeyToAscii(key string) (int) {
	v := 0;
	for k :=0; k < len(key); k++ {
		v += int(key[k])
	}
	return v

}

func ParseSindexdLog() {


}

