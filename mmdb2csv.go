package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/oschwald/geoip2-golang"
	"github.com/oschwald/maxminddb-golang"
)

// ClickHouse CSV mode
var clickhouseFlag bool

func removeUnsafeChars(strarr []string) []string {
	var output = []string{}
	replacer := strings.NewReplacer("\"", "", "'", "")

	for _, str := range strarr {
		output = append(output, strings.TrimSpace(replacer.Replace(str)))
	}
	return output
}

func dumpCity(networks *maxminddb.Networks, writer *csv.Writer) (err error) {
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
	err = writer.Write(headers)
	if err != nil {
		return err
	}

	// City is nested
	record := geoip2.City{}
	for networks.Next() {
		subnet, err := networks.Network(&record)
		if err != nil {
			log.Fatalln(err)
		}
		values := []string{
			subnet.String(),

			fmt.Sprintf("%d", record.City.GeoNameID),
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

			fmt.Sprintf("%d", record.RegisteredCountry.GeoNameID),
			fmt.Sprintf("%v", record.RegisteredCountry.IsInEuropeanUnion),
			record.RegisteredCountry.IsoCode,
			record.RegisteredCountry.Names["en"],

			fmt.Sprintf("%d", record.RepresentedCountry.GeoNameID),
			fmt.Sprintf("%v", record.RepresentedCountry.IsInEuropeanUnion),
			record.RepresentedCountry.IsoCode,
			record.RepresentedCountry.Names["en"],
			record.RepresentedCountry.Type,
		}

		if len(record.Subdivisions) > 0 {
			subd := record.Subdivisions[0]
			values = append(values,
				fmt.Sprintf("%d", subd.GeoNameID),
				subd.IsoCode,
				subd.Names["en"])
		} else {
			values = append(values, "", "", "", "")
		}

		values = append(values,
			fmt.Sprintf("%v", record.Traits.IsAnonymousProxy),
			fmt.Sprintf("%v", record.Traits.IsSatelliteProvider),
		)
		if clickhouseFlag {
			err = writer.Write(removeUnsafeChars(values))
		} else {
			err = writer.Write(values)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpConnections(networks *maxminddb.Networks, writer *csv.Writer) (err error) {
	headers := []string{
		"prefix",
		"connection_type",
	}
	err = writer.Write(headers)
	if err != nil {
		return err
	}

	record := geoip2.ConnectionType{}
	for networks.Next() {
		subnet, err := networks.Network(&record)
		if err != nil {
			log.Fatalln(err)
		}
		values := []string{
			subnet.String(),
			record.ConnectionType,
		}
		if clickhouseFlag {
			err = writer.Write(removeUnsafeChars(values))
		} else {
			err = writer.Write(values)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpCountry(networks *maxminddb.Networks, writer *csv.Writer) (err error) {
	headers := []string{
		"prefix",
		"continent_code",
		"continent_geoname_id",
		"continent_name", // pick en only

		"country_geoname_id",
		"country_is_in_european_union",
		"country_iso_code",
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
	err = writer.Write(headers)
	if err != nil {
		return err
	}
	record := geoip2.Country{}
	for networks.Next() {
		subnet, err := networks.Network(&record)
		if err != nil {
			log.Fatalln(err)
		}
		values := []string{
			subnet.String(),
			record.Continent.Code,
			fmt.Sprintf("%d", record.Continent.GeoNameID),
			record.Continent.Names["en"],

			fmt.Sprintf("%d", record.Country.GeoNameID),
			fmt.Sprintf("%v", record.Country.IsInEuropeanUnion),
			record.Country.IsoCode,
			record.Country.Names["en"],

			fmt.Sprintf("%d", record.RegisteredCountry.GeoNameID),
			fmt.Sprintf("%v", record.RegisteredCountry.IsInEuropeanUnion),
			record.RegisteredCountry.IsoCode,
			record.RegisteredCountry.Names["en"],

			fmt.Sprintf("%d", record.RepresentedCountry.GeoNameID),
			fmt.Sprintf("%v", record.RepresentedCountry.IsInEuropeanUnion),
			record.RepresentedCountry.IsoCode,
			record.RepresentedCountry.Type,

			fmt.Sprintf("%v", record.Traits.IsAnonymousProxy),
			fmt.Sprintf("%v", record.Traits.IsSatelliteProvider),
		}
		if clickhouseFlag {
			err = writer.Write(removeUnsafeChars(values))
		} else {
			err = writer.Write(values)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpISP(networks *maxminddb.Networks, writer *csv.Writer) (err error) {
	headers := []string{
		"prefix",
		"autonomous_system_number",
		"autonomous_system_organization",
		"isp",
		"organization",
	}
	err = writer.Write(headers)
	if err != nil {
		return err
	}

	record := geoip2.ISP{}
	for networks.Next() {
		subnet, err := networks.Network(&record)
		if err != nil {
			log.Fatalln(err)
		}
		values := []string{
			subnet.String(),
			fmt.Sprintf("%d", record.AutonomousSystemNumber),
			record.AutonomousSystemOrganization,
			record.ISP,
			record.Organization,
		}
		if clickhouseFlag {
			err = writer.Write(removeUnsafeChars(values))
		} else {
			err = writer.Write(values)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.BoolVar(&clickhouseFlag, "c", false, "for ClickHouse dictionary")

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
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// skip aliased networks
	networks := db.Networks(maxminddb.SkipAliasedNetworks)
	if networks.Err() != nil {
		log.Fatalln(networks.Err())
	}
	var err2 error
	switch fname {
	case "GeoIP2City.mmdb":
		err2 = dumpCity(networks, writer)
	case "GeoIP2Connections.mmdb":
		err2 = dumpConnections(networks, writer)
	case "GeoIP2Country.mmdb":
		err2 = dumpCountry(networks, writer)
	case "GeoIP2ISP.mmdb":
		err2 = dumpISP(networks, writer)
	}
	if err2 != nil {
		log.Fatal(err2.Error())
	}
	if networks.Err() != nil {
		log.Panic(networks.Err())
	}
}
