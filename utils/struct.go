package utils

import (
	"reflect"
	"strings"
)

func StructToMap( i interface{}) map[string]interface{} {
	s := reflect.ValueOf(i).Elem()
	m := make(map[string] interface{})
	typ := s.Type()
	for i := 0; i < s.NumField();i++ {
		f := s.Field(i)
		key := strings.ToLower(typ.Field(i).Name)
		//m[key] = f.Interface().(Host)   /*  cast interface into Host structure */
		m[key] = f.Interface()
	}
	return m
}
