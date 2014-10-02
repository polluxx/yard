package main

import (
    "github.com/polluxx/yard/search"
    "fmt"
    "net/http"
    "time"
    "encoding/json"
    "github.com/polluxx/yard/encoding/csv"
    "log"
    "os"
    "regexp"
    //"log/syslog"
    "sort"
)

type Report struct {
    Title 	string
    Body	[]Record
}

type Record struct {
    time	string
    rank	string
    count	string
    rangeit	string
}

func main() {
    
    http.HandleFunc("/links/", mainHandler(linksHandler));
    http.HandleFunc("/aggregate", queryHandler(aggregateHandler));
    http.HandleFunc("/report/", mainHandler(reportHandler));
    http.HandleFunc("/list", queryHandler(listHandler));
    http.HandleFunc("/counter", queryHandler(counterHandler));
    
    s := &http.Server{
	Addr:           ":8090",
	//Handler:        Handle,
	ReadTimeout:    10 * time.Second,
	WriteTimeout:   120 * time.Second,
	MaxHeaderBytes: 1 << 20,
    }
    
    log.Fatal(s.ListenAndServe())

    //Aggregate()
}

func mainHandler(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
    
    return func(w http.ResponseWriter, r *http.Request) {
	
	// change valid path for main queries
	var validPath = regexp.MustCompile("^/(links|report)/([a-zA-Z0-9-]+)$")
        mess := validPath.FindStringSubmatch(r.URL.Path)
        
        if mess == nil {
    	    http.NotFound(w,r);
            return
        }
        fn(w, r, mess[2])
    }
}

func queryHandler (fn func(http.ResponseWriter, *http.Request, map[string]string)) http.HandlerFunc {
    var validPath = regexp.MustCompile("^/(list|aggregate|counter)")
    return func(w http.ResponseWriter, r *http.Request) {
	mess := validPath.FindStringSubmatch(r.URL.Path)
	if mess == nil {
	    http.NotFound(w,r);
	    return
	}
	
	r.ParseForm();
	queryParams := make(map[string]string)
	for index, value := range r.Form {
	    queryParams[index] = value[0];
	}
	
	fn(w, r, queryParams)
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

func aggregateHandler(w http.ResponseWriter, r *http.Request, params map[string]string) {
    //sl, _ := syslog.New(syslog.LOG_INFO, "info")
    expected := []string{"project"}
    
    for _, val := range expected {
	_, exist := params[val]
	if (!exist) {
	    http.Error(w, fmt.Sprintf("Param '%s' expected, but not provided", val), 403)
	    return 
	}
    }
    
    const longForm = "2006-Jan-02"
    //t, _ := time.Parse(longForm, dateto)
    
    
    if (params["to"] == "") {
	curr := time.Now()
	//params["to"] = time.Parse(longForm, curr.Date())
	params["to"] = curr.Format(longForm)
    }
    
    if (params["from"] == "") {
	
	curr, parseErr := time.Parse(longForm, params["to"])
	
	if parseErr != nil {
	    http.Error(w, fmt.Sprintf("%s", parseErr), 501)
	    return
	}
	
	timeFrom := curr.AddDate(0, -1, 0)
	params["from"] = timeFrom.Format(longForm)
    }
    
    if (params["group"] == "") {
	params["group"] = "all"
    }
    
    /*var validDate = regexp.MustCompile("^d{4}-([A-Z])w+-d{2}$")
    isValid := validDate.FindStringSubmatch(params["to"])
    if isValid == nil {
        http.Error(w, fmt.Sprintf("date '%s' isn't correct", params["to"]), 203);
        return
    }*/
    
    filename := fmt.Sprintf("%s-%s-%s-%s", params["from"], params["to"], params["project"], params["group"])
    
    // checking if needed raw data or only report link
    isRaw := true
    if(params["raw"] != "true") {
        isRaw = false
        link, errorRead := findReport(filename, r)
        if errorRead == nil {
    	    response := map[string]string{"resource":link, "error":"null"}
    	    makeResp(w, r, response)
    	    return
        }
    }
    // end
    
    
    fromT, _ := time.Parse(longForm, params["from"])
    toT, _ := time.Parse(longForm, params["to"])
    
    duration := toT.Sub(fromT)/24
    
    
    
    params["duration"] = fmt.Sprintf("%d", int(duration.Hours()))
    
    
    //project, _ := strconv.ParseInt(params["project"], 10, 64)
    itemsResp := search.LogSearch(params["project"], params["from"], int(duration.Hours()), 10000);
    
    if (!isRaw) {
    
	if (len(itemsResp) == 0) {
	    response := map[string]string{"resource":"null", "error":"no data for report"}
	    makeResp(w, r, response)
	    return 
	}
	
	
	formed := ReparseToCSV(itemsResp)
	
	
	filedata := &Report{Title: filename, Body: formed}
	errorWrite := filedata.saveReport(formed)
	
	if errorWrite != nil {
	    http.Error(w, errorWrite.Error(), http.StatusInternalServerError)
	}
	
	link := fmt.Sprintf("%s/report/%s", r.Host, filename)
	
    	response := map[string]string{"resource":link, "error":"null"}
    	makeResp(w, r, response)
    	return
	
    }
    
    
    ResponseRawData(w, itemsResp)
    
}

func ResponseRawData(w http.ResponseWriter, itemsResp map[string]map[string]string) {


    reports := make([]Record, len(itemsResp))
    
    in :=0
    var report = Record{}
    for index, value := range itemsResp {
	
	report.time 	=	index
	report.rank 	=	value["rank"]
	report.count 	=	value["count"]
	report.rangeit 	=	value["rangeitem"]
	reports[in] 	=	report
	in++
    }
    
    reports = MakeSort(reports)
    
    flat := make([]map[string]string, len(reports))
    i:=0;
    
    flatval := make(map[string]string)
    for _, tVal := range reports {
        
        flatval = map[string]string{"timedate":tVal.time,"rank":tVal.rank,"range":tVal.rangeit,"count":tVal.count}
        
        flat[i] = flatval
        i++
    }
    
    
    jsn, err := json.Marshal(flat)
    
    //sl.Info(fmt.Sprintf("%s", jsn))
    
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "http://avp.ria.local")
    w.Header().Set("Access-Control-Allow-Credentials", "true")
    w.Header().Set("Access-Control-Allow-Headers", "authorization")
    
    w.Write(jsn);
}

func ReparseToCSV(data map[string]map[string]string) []Record{
    
    formed := make([]Record, len(data))
    ind := 0
    
    var rec = Record{}
    for time, value := range data {
	
	rec.time = 	time
	rec.rank = 	value["rank"]
	rec.count =	value["count"]
	rec.rangeit =	value["rangeitem"]
	formed[ind] = rec
	ind++
    }
    
    log.Print(fmt.Sprintf("%v", formed))
    
    return formed
}

func GetProjectLinks(w http.ResponseWriter, r *http.Request, project int) {

    
    data := search.Links(project)
    
    makeResp(w, r, data)
}

func makeResp(w http.ResponseWriter, r *http.Request, data map[string]string) {
    jsn, err := json.Marshal(data)
    
    if err != nil {
	http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "http://avp.ria.local")
    w.Header().Set("Access-Control-Allow-Credentials", "true")
    w.Header().Set("Access-Control-Allow-Headers", "authorization")
    w.Write(jsn);
}

func findReport(link string, r *http.Request) (string, error) {
    errorChd := os.Chdir("/var/spool/reports")
    if errorChd != nil {
	errCrt := os.Mkdir("/var/spool/reports", os.ModePerm)
	if (errCrt != nil) {
	    log.Fatal(errCrt)
	    return "",errCrt
	}
	os.Chdir("/var/spool/reports")
	log.Fatal(errorChd)
    }
    
    _, err := os.Open(fmt.Sprintf("%s.csv", link))
    
    return fmt.Sprintf("%s/report/%s", r.Host, link), err
}

func loadReport(link string) (file *os.File, err error) {
    err = os.Chdir("/var/spool/reports")
    if err != nil {
	return
    }
    
    file, err = os.Open(fmt.Sprintf("%s.csv", link))
    
    return 
}

func (rep *Report) saveReport(datafile []Record) error {
    os.Chdir("/var/spool/reports")
    
    filename := rep.Title + ".csv"
    
    newFile, err := os.Create(filename)
    if err != nil {
	log.Fatal(err)
	return err
    }
    
    writer := csv.NewWriter(newFile)
    return writer.WriteAllCsv(datafile)
}

func reportHandler(w http.ResponseWriter, r *http.Request, param string) {
    
    _, err := loadReport(param)
    
    if err != nil {
	http.Error(w, err.Error(), 501)
	return
    }
    
    http.ServeFile(w, r, fmt.Sprintf("/var/spool/reports/%s.csv", param))
    
}

func listHandler(w http.ResponseWriter, r *http.Request, param map[string]string) {

}

func counterHandler(w http.ResponseWriter, r *http.Request, params map[string]string) {
    
}


type By func(a1, a2 *Record) bool

    func (by By) Sort(items []Record) {
	isor := &itemsSorter {
	    items: items,
	    by	: by,
	}
        sort.Sort(isor)
    }
    
    type itemsSorter struct {
        items	[]Record
        by	func(a1, a2 *Record) bool
    }
    
    func (s *itemsSorter) Len() int {
        return len(s.items)
    }
    
    func (s *itemsSorter) Swap(i,j int) {
        s.items[i], s.items[j] = s.items[j], s.items[i]
    }
    
    func (s *itemsSorter) Less(i,j int) bool {
        return s.by(&s.items[i], &s.items[j])
    }
    
    func MakeSort(items []Record) []Record {
        // sort closure
        date := func(r1, r2 *Record) bool {
            return r1.time < r2.time
        }
        
        By(date).Sort(items)
	
        return items
    }