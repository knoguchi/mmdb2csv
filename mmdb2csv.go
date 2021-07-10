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
			// City is nested
			record := geoip2.City{}
			if !headerWritten {
				headers := []string{
					"prefix",
					"city_geoname_id",
					"city_name",

					"continent_code",
					"continent_geoname_id",
					"continent_name", // pick en only

					"country_geoname_id",
					"country_is_in_european_union",
					"country_iso_code",
					"country_name", // pick en only

					"location_accuracy_radius",
					"location_latitude",
					"location_longitude",
					"location_metro_code",
					"location_time_zone",

					"postal_code",

					"registered_country_geoname_id",
					"registered_country_is_in_european_union",
					"registered_country_iso_code",
					"registered_country_name", // pick en only

					"represented_country_geoname_id",
					"represented_country_is_in_european_union",
					"represented_country_iso_code",
					"represented_country_name", // pick en only
					"represented_country_type",

					"subdivisions_geoname_id",
					"subdivisions_iso_code",
					"subdivisions_name",

					"traits_is_anonymous_proxy",
					"traits_is_satellite_provider",
				}
				err = w.Write(headers)
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{
				subnet.String(),

				fmt.Sprintf("%v", record.City.GeoNameID),
				record.City.Names["en"],

				record.Continent.Code,
				fmt.Sprintf("%v", record.Continent.GeoNameID),
				record.Continent.Names["en"],

				fmt.Sprintf("%v", record.Country.GeoNameID),
				fmt.Sprintf("%v", record.Country.IsInEuropeanUnion),
				record.Country.IsoCode,
				record.Country.Names["en"],

				fmt.Sprintf("%v", record.Location.AccuracyRadius),
				fmt.Sprintf("%v", record.Location.Latitude),
				fmt.Sprintf("%v", record.Location.Longitude),
				fmt.Sprintf("%v", record.Location.MetroCode),
				record.Location.TimeZone,

				record.Postal.Code,

				fmt.Sprintf("%v", record.RegisteredCountry.GeoNameID),
				fmt.Sprintf("%v", record.RegisteredCountry.IsInEuropeanUnion),
				record.RegisteredCountry.IsoCode,
				record.RegisteredCountry.Names["en"],

				fmt.Sprintf("%v", record.RepresentedCountry.GeoNameID),
				fmt.Sprintf("%v", record.RepresentedCountry.IsInEuropeanUnion),
				record.RepresentedCountry.IsoCode,
				record.RepresentedCountry.Names["en"],
				record.RepresentedCountry.Type,
			}

			if len(record.Subdivisions) > 0 {
				subd := record.Subdivisions[0]
				values = append(values,
					fmt.Sprintf("%v", subd.GeoNameID),
					subd.IsoCode,
					subd.Names["en"])
			} else {
				values = append(values, "", "", "", "")
			}

			values = append(values,
				fmt.Sprintf("%v", record.Traits.IsAnonymousProxy),
				fmt.Sprintf("%v", record.Traits.IsSatelliteProvider),
			)
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
				headers := []string{
					"prefix",
					"continent_code",
					"continent_geoname_id",
					"continent_name", // pick en only

					"country_geoname_id",
					"country_is_in_european_union",
					"country_name", // pick en only

					"registered_country_geoname_id",
					"registered_country_is_in_european_union",
					"registered_country_iso_code",
					"registered_country_name", // pick en only

					"represented_country_geoname_id",
					"represented_country_is_in_european_union",
					"represented_country_iso_code",
					"represented_country_type",

					"traits_is_anonymous_proxy",
					"traits_is_satellite_provider",
				}
				err = w.Write(headers)
				if err != nil {
					log.Fatal("can't write header")
				}
				headerWritten = true
			}
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Fatalln(err)
			}
			values = []string{
				subnet.String(),
				record.Continent.Code,
				fmt.Sprintf("%v", record.Continent.GeoNameID),
				record.Continent.Names["en"],

				fmt.Sprintf("%v", record.Country.GeoNameID),
				fmt.Sprintf("%v", record.Country.IsInEuropeanUnion),
				record.Country.Names["en"],

				fmt.Sprintf("%v", record.RegisteredCountry.GeoNameID),
				fmt.Sprintf("%v", record.RegisteredCountry.IsInEuropeanUnion),
				record.RegisteredCountry.IsoCode,
				record.RegisteredCountry.Names["en"],

				fmt.Sprintf("%v", record.RepresentedCountry.GeoNameID),
				fmt.Sprintf("%v", record.RepresentedCountry.IsInEuropeanUnion),
				record.RepresentedCountry.IsoCode,
				record.RepresentedCountry.Type,

				fmt.Sprintf("%v", record.Traits.IsAnonymousProxy),
				fmt.Sprintf("%v", record.Traits.IsSatelliteProvider),
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
