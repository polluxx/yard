package main

import (
	"github.com/gocql/gocql"
	"fmt"
	"log"
)

func main() {
    cluster := gocql.NewCluster("10.1.51.65")
    cluster.Keyspace = "avp"
    session, _ := cluster.CreateSession()
    var keyword string;
    var rank int;

    if err := session.Query("SELECT rank,keyword from avp.rank_log where project = 1 and time > '2014-09-09 13:00:00+0300' and time < '2014-09-
	fmt.Println("error");
	log.Fatal(err);
    }

    fmt.Println(rank, keyword);

    defer session.Close()
}