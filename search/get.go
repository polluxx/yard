package search

import (
	"github.com/gocql/gocql"
	"fmt"
	"log"
	"time"
	"strings"
)

type Log struct {
    rank	int
    keyword	string
    cat		string
    subcat	string
    city	string
    timedata	string
    day 	string
    counter int
}

type Input struct {
    category	string	`bson:"category"`
    subcategory	string	`bson:"subcategory"`
    city	string	`bson:"city"`
    typeval	string	`bson:"typeval"`
}

var logged map[string] Log 

var filtered map[string] Log

func Aggregate(results map[string]Log, item Log, key string) map[string]Log {
    
    //params := Input{"empty","empty","empty","group"};
    
    
    //for key, value := range item {
	//key := item.keyword;
	_, exist := results[key];
	if exist {
	    item.counter = results[key].counter+1;
	    item.rank = results[key].rank + item.rank
	    results[key] = item;
	} else {
	    results[key] = item;
	}
    //}
    
    return results
}

func LogSearch(project int, from string, duration int, limit int) interface{} {

    
    output := make(chan interface{})
    
    loc, _ := time.LoadLocation("Europe/Kiev");
    const longForm = "2006-Jan-02"
    t, _ := time.Parse(longForm, from)
    
    t = t.In(loc);
    
    join := "00:00:00+0300"
    
    dates := strings.Fields(fmt.Sprintf("%s", t));
    from = fmt.Sprintf("%v %s", dates[0], join);
    
    var valD int = 0;
    go func() {
	for valD < duration {
	    valD++;
	    nextDate := t.AddDate(0, 0, valD);
	    datesTo := strings.Fields(fmt.Sprintf("%s", nextDate));
	    to := fmt.Sprintf("%v %s", datesTo[0], join);
	    GetLog(project, from, to, limit, output);
	    from = to;
	}
	close(output);
    }();
    
    
    return <- output;
}


func GetLog(project int, from,to string, limit int, output chan interface{}) {

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
    
    for iter.Scan(&rank, &keyword, &cat, &subcat, &city, &timedata, &item) {
	
	time := timedata;
	
	t := timedata.In(location).Format(timeform)
	day := time.In(location).Format(dayform)
	
	dayTime := strings.Split(day, "Z")
	
	valLog := Log{rank,keyword,cat,subcat,city, t, dayTime[0], 1}
	
	logged = Aggregate(logged, valLog, item);
	
    }
    
    
    filtered = make(map[string]Log)
    
    for _,value := range logged {
    
	if (value.counter > 1) {
	    filtered = Aggregate(filtered, value, value.day)
	}
    }
    
    for keyIter,_ := range filtered {
	insert := filtered[keyIter];
	insert.rank = insert.rank / insert.counter;
	
	filtered[keyIter] = insert;
    }
    
    if err := iter.Close(); err != nil {
	log.Fatal(err)
    }

    defer session.Close()
    
    output <- filtered;
}