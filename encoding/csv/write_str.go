package csv

import (
    "encoding/csv"
    "io"
    "reflect"
    "fmt"
    //"log"
)

type Writer struct {
    *csv.Writer
}

func NewWriter(w io.Writer) *Writer {
    return &Writer{
	csv.NewWriter(w),
    }
}

func (w *Writer) WriteAllCsv(data interface{}) (err error) {

    refl := reflect.ValueOf(data)

    err = w.WriteCsvHeader(refl.Index(0).Interface())
    if err != nil {
	return err
    }


    for i:=0;i<refl.Len();i++ {
	val := refl.Index(i)
	
	err = w.WriteCsv(val.Interface())
	if err != nil {
	    return err
	}
    }
    w.Flush()
    return 
}
            
func (w *Writer) WriteCsv(data interface{}) error {
    rv := reflect.ValueOf(data)
    
    reflected := reflect.TypeOf(data)
    item := make([]string, reflected.NumField())
    
    for j:=0;j<reflected.NumField();j++{
	item[j] = fmt.Sprintf("%v", rv.Field(j))
    }
    
    return w.Write(item)
}

func (w *Writer) WriteCsvHeader(data interface{}) error {
    reflected := reflect.TypeOf(data)
    item := make([]string, reflected.NumField())
    for s:=0;s<reflected.NumField();s++{
	item[s] = fmt.Sprintf("%v", reflected.Field(s).Name)
    }
    return w.Write(item)
}