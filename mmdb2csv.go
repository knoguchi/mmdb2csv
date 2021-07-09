package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/oschwald/geoip2-golang"
	"github.com/oschwald/maxminddb-golang"
	"log"
	"os"
	"path"
	"reflect"
)

func getTags(obj interface{}) []string {
	var tags []string
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("maxminddb")
		tags = append(tags, tag)
	}
	return tags
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		log.Fatal("Please provide mmdb path")
	}

	fullPath := flag.Args()[0]
	fname := path.Base(fullPath)

	// open mmdb
	db, err := maxminddb.Open(fullPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *maxminddb.Reader) {
		err := db.Close()
		if err != nil {
			// ignore
		}
	}(db)

	// open CSV writer, and write header
	w := csv.NewWriter(os.Stdout)

	// skip aliased networks
	networks := db.Networks(maxminddb.SkipAliasedNetworks)
	if networks.Err() != nil {
		log.Fatalln(networks.Err())
	}

	headerWritten := false
	for networks.Next() {
		var values []string

		switch fname {
		case "GeoIP2City.mmdb":
			record := geoip2.City{}
			if !headerWritten {
				err = w.Write(getTags(record))
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{subnet.String()}
			r := reflect.ValueOf(record)
			for i := 0; i < r.NumField(); i++ {
				values = append(values, fmt.Sprintf("%v", r.Field(i)))
			}
		case "GeoIP2Connections.mmdb":
			record := geoip2.ConnectionType{}
			if !headerWritten {
				err = w.Write(getTags(record))
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{subnet.String()}
			r := reflect.ValueOf(record)
			for i := 0; i < r.NumField(); i++ {
				values = append(values, fmt.Sprintf("%v", r.Field(i)))
			}
		case "GeoIP2Country.mmdb":
			record := geoip2.Country{}
			if !headerWritten {
				err = w.Write(getTags(record))
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{subnet.String()}
			r := reflect.ValueOf(record)
			for i := 0; i < r.NumField(); i++ {
				values = append(values, fmt.Sprintf("%v", r.Field(i)))
			}
		case "GeoIP2ISP.mmdb":
			record := geoip2.ISP{}
			if !headerWritten {
				err = w.Write(getTags(record))
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{subnet.String()}
			r := reflect.ValueOf(record)
			for i := 0; i < r.NumField(); i++ {
				values = append(values, fmt.Sprintf("%v", r.Field(i)))
			}
		}
		if err := w.Write(values); err != nil {
			log.Fatalln("error writing record to csv:", err)
		}
	}
}
