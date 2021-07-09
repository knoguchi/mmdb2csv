# mmdb2csv

# What does it do?

Convert Maxmind mmdb database to CSV.

# Why?

Many applications support CSV but not mmdb.  For example it's easy to import CSV to SQL databases.

# How?

./mmdb2csv GeoIP2ISP.mmdb > isp.csv

# Build
go build mmdb2csv.go

