package search

import (
	"github.com/gocql/gocql"
	"fmt"
	"log"
	"time"
	"strings"
	"math/rand"
	//"encoding/json"
)

type Log struct {
    rank	int
    keyword	string
    cat		string
    subcat	string
    city	string
    timedata	string
    day 	string
    rangeItem	int
    counter 	int
}

type Checked struct {
    checked	int
    project	int
    catid	string
    subid	string
    city	string
    operation	string
    counter	int
}

type Input struct {
    category	string	`bson:"category"`
    subcategory	string	`bson:"subcategory"`
    city	string	`bson:"city"`
    typeval	string	`bson:"typeval"`
}

var logged map[string] Log 

var filtered map[string] Log

func Aggregate(results map[string]Log, output chan map[string]Log) map[string]Log {
    
    //params := Input{"empty","empty","empty","group"};
    
    tmpData := make(map[string]Log);
    tmpData = <- output;
    
    
    for key, value := range tmpData {
	//key := item.keyword;
	_, exist := results[key];
	if exist {
	    value.counter = results[key].counter+1;
	    value.rank = results[key].rank + value.rank
	    results[key] = value;
	} else {
	    results[key] = value;
	}
    }
    
    return results
}

func LogSearch(project string, from string, duration int, limit int) map[string]map[string]string {

    
    //output := make(chan map[string]Log)
    
    //loc, _ := time.LoadLocation("Europe/Kiev");
    const longForm = "2006-Jan-02"
    t, _ := time.Parse(longForm, from)
    
    //t = t.In(loc);
    
    join := "+0300"
    
    dates := strings.Fields(fmt.Sprintf("%s", t));
    from = fmt.Sprintf("%v %v%s", dates[0], dates[1], join);
    
    //filteredItems := make(map[string]Log)
    
    //output <- filteredItems
    
    results := make(map[string]Log)
    
    var valD int = 0;
    
    duration *= 12;
    
    
    
    //go func() {
    
	allCount := 0;
	for valD < duration {
	    valD++;
	    //nextDate := t.AddDate(0, 0, valD);
	    nextDate := t.Add(time.Duration(120*valD)*time.Minute);
	    
	    datesTo := strings.Fields(fmt.Sprintf("%s", nextDate));
	    to := fmt.Sprintf("%v %v%s", datesTo[0], datesTo[1], join);
	    
	    response := GetLog(project, from, to, limit);
	    
	    //fmt.Println(from, to)
	    
	    
	    from = to;
	    
	    tmpData := make(map[string]Log);
	    tmpData = <- response;
	    
	    //dataRes = Aggregate(dataRes, response);
		
	        for key, value := range tmpData {
	    	    
	    	    //key := item.keyword;
	    	    _, exist := results[key];
	    	    if exist {
	    		
	    		value.counter = results[key].counter+1
	    		//value.range = value.rank - results[key].rank
	    		value.rank += results[key].rank
	    		results[key] = value
	    	    } else {
	    		allCount++;
			results[key] = value
	    	    }
		}
		
	    
	}
	
	
	filterItems := make(map[string]Log)
	for _,value := range results {
	    
	    if(value.counter > 1) {
		value.rank /= value.counter;
		value.counter = 1;
		key := value.day
		_, exist := filterItems[key]
		if(exist) {
		    
		    value.rank += filterItems[key].rank;
		    value.counter += filterItems[key].counter;
		    filterItems[key] = value
		} else {
		    filterItems[key] = value
		}
		
	    }
	}
	
	
	marshaled := make(map[string]map[string]string)
	
	//marsh := make(map[string]string)
	for key, value := range filterItems {
	    value.rank /= value.counter;
	    value.rangeItem = allCount
	    
	    marsh := make(map[string]string)
	    
	    marsh["rank"] = fmt.Sprintf("%v", value.rank)
	    marsh["rangeitem"] = fmt.Sprintf("%v", value.rangeItem)
	    marsh["count"] = fmt.Sprintf("%v", value.counter)
	    
	    marshaled[key] = marsh;
	}
	
	
    //}();
    
    
    return marshaled
    
    //return output
}


func GetLog(project string, from,to string, limit int) <- chan map[string]Log {
    output := make(chan map[string]Log)
    
    location, _ := time.LoadLocation("Europe/Kiev")

    cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
    cluster.Keyspace = "avp"
    session, _ := cluster.CreateSession()
    var keyword, cat, subcat, city, item string;
    var timedata time.Time;
    
    var rank int;
    
    //var paramsToGrab
    
    iter := session.Query(fmt.Sprintf("select rank, keyword, cat, subcat, city, time, item from rank_log where project = %v and time > '%v' and time < '%v' limit %v", project, from, to, limit)).Iter() 
    
    const timeform = "02 Jan 06 15:04 (MST)"
    
    const dayform = "2006-01-02Z01:00 (RFC3339)"
    
    logged = make(map[string]Log)
    
    response := make(map[string]Log)
    
    go func() {

		for iter.Scan(&rank, &keyword, &cat, &subcat, &city, &timedata, &item) {

			time := timedata;

			t := timedata.In(location).Format(timeform)
			day := time.In(location).Format(dayform)

			dayTime := strings.Split(day, "Z")

			response[item] = Log{rank,keyword,cat,subcat,city, t, dayTime[0], 0, 1}

		//logged = Aggregate(logged, valLog, item);

		}

		output <- response
    
    }();
    
    
    if err := iter.Close(); err != nil {
		log.Fatal(err)
    }

    defer session.Close()
    
    return output;
}

func Links(project int) map[string]string{
    
    cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
    cluster.Keyspace = "avp"
    session, _ := cluster.CreateSession()
    
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    
    //item1 := ReturnRand(2, r)
    item2 := ReturnRand(19, r)
    //item1 := ReturnRand(8, r)
    
    //fmt.Print(item1, item2, item3);
    
    response := make(map[string]string)
    tmpResp := make(map[string]string)
    var keyword, link string
    
    query := fmt.Sprintf("select keyword, link from pure_data where project = %v and checked = 1 and priority > %v limit 30", project, item2)
    
    //fmt.Print(query);
    iter := session.Query(query).Iter()
    
    for iter.Scan(&keyword, &link) {
	tmpResp[keyword] = keyword
	//fmt.Print(keyword);
    }
    
    if(len(tmpResp) == 0) {
	return response
    }
    
    items := make([]string, len(tmpResp))
    
    i := 0;
    for _, value := range tmpResp {
	items[i] = value;
	i++;
    }
    
    for j := 0; j < 3; j++ {
	rander := r.Intn(len(tmpResp))
	key := items[rander]
	response[key] = key
    }
    
    
    if err := iter.Close(); err != nil{
	return response
	log.Fatal(err);
    }
    
    return response
}

func Count() [][]string {
    cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
    cluster.Keyspace = "counterks"
    session, _ := cluster.CreateSession()
    
    output := make(chan map[string]Checked)
    
    checkers := map[int]string{0:"не проверено",1:"проверено"}
    
    projects := map[int]string{1:"Auto",2:"Ria",3:"Dom",5:"Market"}
    
    var catid,subid,city,operation string
    var checked,project,counter int
    
    
    grabbed := make(map[string]Checked)
    for chk, _ := range checkers {
    
	for pInd, _ := range projects {
	
	    query := fmt.Sprintf("select checked,project,catid,subid,city,operation,counterval from checkers where checked = %d and project = %d limit 100000", chk, pInd)
	    
	    iter := session.Query(query).Iter()
	    
	    
	    
	    
    	    go func() {
		
		for iter.Scan(&checked, &project, &catid, &subid, &city, &operation, &counter) {
		    out := Checked{checked, project, catid, subid, city, operation, counter}
		    
		    substKey := fmt.Sprintf("%s-%s-%s-%s-%s", checkers[out.checked], projects[out.project], out.catid, out.operation, out.city)
		    _, exist := grabbed[substKey]
		    if (exist) {
			
			out.counter += grabbed[substKey].counter
		    }
		    grabbed[substKey] = out
		}
		output <- grabbed
		
	    }()
	    
	    if err := iter.Close(); err != nil {
		log.Fatal(err)
	    }
	}
    
    }
    
    tmpData := make(map[string]Checked);
    tmpData = <- output;
    
    
    
    response := make([][]string, len(tmpData))
    
    //item := make(map[string]string)
    i:=0
    var item []string
    for ind, valR := range tmpData {
	item = make([]string, 2)
	/*item["checked"] = fmt.Sprintf("%d", valR.checked)
	item["project"] = fmt.Sprintf("%d", valR.project)
	item["category"] = valR.catid
	item["subcategory"] = valR.subid
	item["city"] = valR.city
	item["operation"] = valR.operation
	item["count"] = fmt.Sprintf("%d", valR.counter)
	*/
	
	//marsh, _ := json.Marshal(item)
	item[0] = ind
	
	if(valR.counter < 0) {
	    valR.counter = 0
	}
	item[1] = fmt.Sprintf("%d", valR.counter)
	response[i] = item
	i++
    }
    
    defer session.Close()
    
    return response
}

func ReturnRand(volume int64, r *rand.Rand) string {
    return fmt.Sprintf("%v%v", r.Int63n(volume), strings.Replace(fmt.Sprintf("%v", r.Float32()), "0", "", 1));
}
