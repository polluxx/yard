package sorter

import (
    "sort"
)

type Record struct {
    time	string
    rank	string
    count	string
    rangeit	string
}

type By func(a1, a2 *Record) bool

func (by By) Sort(items []Record) {
    isor := &itemsSorter {
	items	: items,
	by	: by,
    }
    sort.Sort(isor)
}

type itemsSorter struct {
    items	[]Record
    by		func(a1, a2 *Record) bool
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


