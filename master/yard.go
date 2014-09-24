package main

import (
    "fmt"
    "github.com/polluxx/yard/search"
)

func main() {
    Aggregate()
}

func Aggregate() {
    items := search.LogSearch(1, "2014-Sep-09", 2, 10000);
    
    //search.Aggregate();
    
    fmt.Print(items);
    
    //values := <- items
    /*
    for _, value := range values {
	fmt.Println(value);
    }*/
    
}