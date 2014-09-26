package main

import (
    "github.com/polluxx/yard/search"
    "fmt"
    "net/http"
    "time"
    "encoding/json"
    "log"
    
    "regexp"
)

var validPath = regexp.MustCompile("^/(links|aggregate)/([a-zA-Z0-9]+)$")

func main() {
    
    //&http.HandleFunc("/", Handler)
    
    /*http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	
	switch path := r.URL.Path[len("//")]; path {
	    case "links":
		fmt.Fprintf(w, "Params %q - ", r.URL.Path[1:])
	    case "aggregate":
	    
	    default:
		fmt.Fprintf(w, "Route %q - not found!", html.EscapeString(r.URL.Path[1:]))
	}
	
    })*/
    
    http.HandleFunc("/links/", mainHandler(linksHandler));
    
    s := &http.Server{
	Addr:           ":8090",
	//Handler:        Handle,
	ReadTimeout:    10 * time.Second,
	WriteTimeout:   10 * time.Second,
	MaxHeaderBytes: 1 << 20,
    }
    
    log.Fatal(s.ListenAndServe())

    //Aggregate()
}

func mainHandler(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
    
    return func(w http.ResponseWriter, r *http.Request) {
        mess := validPath.FindStringSubmatch(r.URL.Path)
        
        if mess == nil {
    	    http.NotFound(w,r);
            return
        }
        fn(w, r, mess[2])
    }
}

func linksHandler(w http.ResponseWriter, r *http.Request, param string) {
    switch param {
	case "auto":
	    GetProjectLinks(w, r, 1);
	case "dom":
	    GetProjectLinks(w, r, 3);
	case "ria":
	    GetProjectLinks(w, r, 2);
	case "market":
	    GetProjectLinks(w, r, 5);
	default:
	    http.NotFound(w, r)
    }
    
}

type Prof struct {
    Name	string
    Hobbies	[]string
}

func GetProjectLinks(w http.ResponseWriter, r *http.Request, project int) {

    
    data := search.Links(project)
    
    jsn, err := json.Marshal(data)
    
    if err != nil {
	http.Error(w, err.Error(), http.StatusInternalServerError)
	return
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.Write(jsn);
}

func Aggregate() {
    items := search.LogSearch(2, "2014-Sep-01", 31, 10000);
    
    //search.Aggregate();
    
    fmt.Print(items);
    
    //values := <- items
    /*
    for _, value := range values {
	fmt.Println(value);
    }*/
    
}